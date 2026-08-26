package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
	"golang.org/x/sync/errgroup"
)

type cliOptions struct {
	nzbFile        string
	configPath     string
	par2DBPath     string
	maxConnections int
	repairMissing  bool
	keepPAR2State  bool
}

type recoveryHarness struct {
	coordinator  *recovery.Coordinator
	store        *recovery.Store
	dbPath       string
	temporaryDir string
}

type repairTarget struct {
	fileName string
	segment  storage.NZBSegment
}

type missingScanSummary struct {
	Articles       int
	Available      int
	Missing        int
	StatErrors     int
	RepairRanges   int
	RepairFailures int
}

type statResult struct {
	available bool
	err       error
}

func main() {
	opts, err := parseCLI(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "test-parser: %v\n", err)
		fmt.Fprintln(os.Stderr, "Run test-parser -help for usage.")
		os.Exit(2)
	}

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	logger := zerolog.New(output).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, opts, os.Stdout, logger); err != nil {
		logger.Error().Err(err).Msg("Test failed")
		os.Exit(1)
	}
}

func parseCLI(args []string, output io.Writer) (cliOptions, error) {
	opts := cliOptions{}
	flags := flag.NewFlagSet("test-parser", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&opts.configPath, "config", "data", "directory containing config.json")
	flags.StringVar(&opts.par2DBPath, "par2-db", "", "PAR2 database path (default: isolated temporary database)")
	flags.IntVar(&opts.maxConnections, "max-connections", 10, "maximum concurrent parser and STAT requests")
	flags.BoolVar(&opts.repairMissing, "repair-missing", false, "STAT every logical article and PAR2-recover only missing ranges")
	flags.BoolVar(&opts.keepPAR2State, "keep-par2-state", false, "retain this run's PAR2 records after the test")
	flags.Usage = func() {
		fmt.Fprintln(output, "Usage: test-parser [options] <nzb-file>")
		fmt.Fprintln(output, "Example: test-parser -repair-missing test.nzb")
		fmt.Fprintln(output)
		fmt.Fprintln(output, "Parses and processes an NZB using the configured NNTP providers. When")
		fmt.Fprintln(output, "PAR2 is enabled, archive parsing uses the same bounded recovery engine")
		fmt.Fprintln(output, "as a normal import. -repair-missing performs a complete STAT sweep and")
		fmt.Fprintln(output, "requests BODY data only for the bounded PAR2 plans of missing ranges.")
		fmt.Fprintln(output, "It deliberately does not BODY-scan healthy articles for corruption;")
		fmt.Fprintln(output, "ordinary parser header reads may still consume their complete articles.")
		fmt.Fprintln(output)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if flags.NArg() != 1 {
		return cliOptions{}, fmt.Errorf("expected exactly one NZB file, got %d", flags.NArg())
	}
	if opts.maxConnections <= 0 {
		return cliOptions{}, fmt.Errorf("max-connections must be positive")
	}

	opts.nzbFile = flags.Arg(0)
	return opts, nil
}

func run(ctx context.Context, opts cliOptions, output io.Writer, logger zerolog.Logger) (runErr error) {
	logger.Info().Str("file", opts.nzbFile).Msg("Reading NZB file")
	nzbContent, err := os.ReadFile(opts.nzbFile)
	if err != nil {
		return fmt.Errorf("read NZB file %q: %w", opts.nzbFile, err)
	}
	logger.Info().Int("size", len(nzbContent)).Msg("NZB file read successfully")

	config.SetConfigPath(opts.configPath)
	cfg := config.Get()
	nntpClient, err := nntp.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("create NNTP client: %w", err)
	}
	defer func() {
		if closeErr := nntpClient.Close(); closeErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close NNTP client: %w", closeErr))
		}
	}()
	logger.Info().Msg("NNTP client created successfully")

	p := parser.NewParser(nntpClient, opts.maxConnections, logger)
	logger.Info().Msg("Parsing NZB file...")
	nzb, groups, manifest, err := p.ParseWithManifest(ctx, opts.nzbFile, nzbContent)
	if err != nil {
		return fmt.Errorf("parse NZB: %w", err)
	}

	var harness *recoveryHarness
	if manifest != nil && manifest.HasPAR2() && cfg.Usenet.PAR2.IsEnabled() {
		harness, err = openRecoveryHarness(opts.par2DBPath, cfg, nntpClient)
		if err != nil {
			return err
		}
		defer func() {
			printRecoverySummary(output, harness.coordinator.Stats(), harness.dbPath, opts.keepPAR2State)
			if closeErr := harness.close(nzb.ID, !opts.keepPAR2State); closeErr != nil {
				runErr = errors.Join(runErr, closeErr)
			}
		}()

		if err := harness.coordinator.RegisterManifest(manifest); err != nil {
			return fmt.Errorf("register PAR2 manifest: %w", err)
		}
		p.SetArticleRecovery(nzb.ID, harness.coordinator)
		logger.Info().
			Str("nzb_id", nzb.ID).
			Str("par2_db", harness.dbPath).
			Int("max_download_percent", cfg.Usenet.PAR2.DownloadPercent()).
			Int64("max_download_bytes", cfg.Usenet.PAR2.DownloadBytes()).
			Int64("max_storage_bytes", cfg.Usenet.PAR2.StorageBytes()).
			Msg("Bounded PAR2 recovery enabled for parser test")
	} else {
		reason := "NZB has no detected PAR2 files"
		if manifest != nil && manifest.HasPAR2() && !cfg.Usenet.PAR2.IsEnabled() {
			reason = "PAR2 recovery is disabled in config"
		}
		logger.Warn().Str("reason", reason).Msg("Parser test is running without PAR2 recovery")
	}
	if opts.repairMissing && harness == nil {
		return fmt.Errorf("repair-missing requires an NZB with PAR2 and enabled usenet.par2 configuration")
	}

	updatedNZB, err := p.Process(ctx, nzb, groups)
	if err != nil {
		return fmt.Errorf("process NZB: %w", err)
	}
	nzb = updatedNZB

	// Process learns exact yEnc offsets and filenames. Persist the same shared,
	// enriched manifest that the production import path re-registers.
	if harness != nil {
		enriched := parser.ManifestFromGroups(groups)
		if enriched == nil {
			return fmt.Errorf("processed NZB lost its PAR2 recovery manifest")
		}
		if err := harness.coordinator.RegisterManifest(enriched); err != nil {
			return fmt.Errorf("register enriched PAR2 manifest: %w", err)
		}
	}

	if opts.repairMissing {
		logger.Info().Msg("STAT-scanning every distinct logical article for missing content")
		summary, scanErr := repairMissingArticles(ctx, nntpClient, harness.coordinator, nzb, opts.maxConnections, logger)
		printMissingScanSummary(output, summary)
		if scanErr != nil {
			return fmt.Errorf("repair missing articles: %w", scanErr)
		}
	}

	printFileSummary(output, nzb)
	logger.Info().Msg("Test completed successfully")
	return nil
}

func openRecoveryHarness(dbPath string, cfg *config.Config, client *nntp.Client) (*recoveryHarness, error) {
	harness := &recoveryHarness{}
	if dbPath == "" {
		dir, err := os.MkdirTemp("", "goblack-test-parser-par2-")
		if err != nil {
			return nil, fmt.Errorf("create temporary PAR2 directory: %w", err)
		}
		harness.temporaryDir = dir
		dbPath = filepath.Join(dir, "par2.db")
	} else {
		absolute, err := filepath.Abs(dbPath)
		if err != nil {
			return nil, fmt.Errorf("resolve PAR2 database path: %w", err)
		}
		dbPath = absolute
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return nil, fmt.Errorf("create PAR2 database directory: %w", err)
		}
	}
	harness.dbPath = dbPath

	store, err := recovery.Open(dbPath)
	if err != nil {
		harness.removeTemporaryDir()
		return nil, fmt.Errorf("open PAR2 recovery store: %w", err)
	}
	harness.store = store

	coordinator, err := recovery.NewCoordinator(store, client, recovery.Policy{
		Enabled:            cfg.Usenet.PAR2.IsEnabled(),
		MaxDownloadPercent: cfg.Usenet.PAR2.DownloadPercent(),
		MaxDownloadBytes:   cfg.Usenet.PAR2.DownloadBytes(),
		MaxStorageBytes:    cfg.Usenet.PAR2.StorageBytes(),
	})
	if err != nil {
		_ = store.Close()
		harness.removeTemporaryDir()
		return nil, fmt.Errorf("create PAR2 recovery coordinator: %w", err)
	}
	harness.coordinator = coordinator
	return harness, nil
}

func (h *recoveryHarness) close(nzbID string, deleteState bool) error {
	if h == nil {
		return nil
	}
	var result error
	if deleteState && h.coordinator != nil && nzbID != "" {
		result = errors.Join(result, h.coordinator.DeleteNZB(nzbID))
	}
	if h.coordinator != nil {
		result = errors.Join(result, h.coordinator.Close())
	}
	if h.store != nil {
		result = errors.Join(result, h.store.Close())
	}
	if deleteState {
		result = errors.Join(result, h.removeTemporaryDir())
	}
	if result != nil {
		return fmt.Errorf("close PAR2 test state: %w", result)
	}
	return nil
}

func (h *recoveryHarness) removeTemporaryDir() error {
	if h == nil || h.temporaryDir == "" {
		return nil
	}
	err := os.RemoveAll(h.temporaryDir)
	h.temporaryDir = ""
	return err
}

func repairMissingArticles(
	ctx context.Context,
	client *nntp.Client,
	coordinator *recovery.Coordinator,
	nzb *storage.NZB,
	maxConnections int,
	logger zerolog.Logger,
) (missingScanSummary, error) {
	targets := collectRepairTargets(nzb)
	messageIDs := make([]string, 0, len(targets))
	for messageID := range targets {
		messageIDs = append(messageIDs, messageID)
	}
	sort.Strings(messageIDs)
	summary := missingScanSummary{Articles: len(messageIDs)}
	if len(messageIDs) == 0 {
		return summary, fmt.Errorf("NZB contains no logical article ranges to scan")
	}

	results := make([]statResult, len(messageIDs))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(min(maxConnections, len(messageIDs)))
	for i, messageID := range messageIDs {
		i, messageID := i, messageID
		group.Go(func() error {
			err := client.ExecuteWithFailover(groupCtx, func(conn *nntp.Connection) error {
				_, _, statErr := conn.Stat(messageID)
				return statErr
			})
			results[i] = statResult{available: err == nil, err: err}
			return nil
		})
	}
	_ = group.Wait() // Workers retain per-article errors so the scan is exhaustive.
	if err := ctx.Err(); err != nil {
		return summary, err
	}

	var scanErrors []error
	for i, result := range results {
		messageID := messageIDs[i]
		if result.available {
			summary.Available++
			continue
		}
		if !nntp.IsArticleNotFoundError(result.err) {
			summary.StatErrors++
			scanErrors = append(scanErrors, fmt.Errorf("STAT %s: %w", messageID, result.err))
			continue
		}

		summary.Missing++
		logger.Warn().Str("message_id", messageID).Int("logical_ranges", len(targets[messageID])).Msg("Article missing on every provider; testing PAR2 recovery")
		for _, target := range targets[messageID] {
			body, err := coordinator.RecoverArticle(ctx, nzb.ID, reader.NewSegmentMeta(target.segment))
			if err == nil {
				err = validateRecoveredRange(target.segment, body)
			}
			if err != nil {
				summary.RepairFailures++
				scanErrors = append(scanErrors, fmt.Errorf("repair %q article %s raw range %d+%d: %w",
					target.fileName, messageID, target.segment.RawOffset, target.segment.RawLength, err))
				continue
			}
			summary.RepairRanges++
			logger.Info().
				Str("file", target.fileName).
				Str("message_id", messageID).
				Int64("raw_offset", target.segment.RawOffset).
				Int64("raw_length", target.segment.RawLength).
				Msg("PAR2 recovery produced the missing logical range")
		}
	}
	return summary, errors.Join(scanErrors...)
}

func collectRepairTargets(nzb *storage.NZB) map[string][]repairTarget {
	targets := make(map[string][]repairTarget)
	if nzb == nil {
		return targets
	}
	type rangeKey struct {
		messageID        string
		rawFileKey       uint32
		rawOffset        int64
		rawLength        int64
		segmentDataStart int64
	}
	seen := make(map[rangeKey]struct{})
	for i := range nzb.Files {
		file := &nzb.Files[i]
		if file.IsDeleted || file.FileType == storage.NZBFileTypePar2 || file.FileType == storage.NZBFileTypeIgnore {
			continue
		}
		for _, segment := range file.Segments {
			key := rangeKey{
				messageID:        segment.MessageID,
				rawFileKey:       segment.RawFileKey,
				rawOffset:        segment.RawOffset,
				rawLength:        segment.RawLength,
				segmentDataStart: segment.SegmentDataStart,
			}
			if segment.MessageID == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			targets[segment.MessageID] = append(targets[segment.MessageID], repairTarget{fileName: file.Name, segment: segment})
		}
	}
	return targets
}

func validateRecoveredRange(segment storage.NZBSegment, body []byte) error {
	if segment.RawFileKey == 0 {
		return fmt.Errorf("segment has no raw-file provenance")
	}
	if segment.RawLength <= 0 {
		return fmt.Errorf("segment has invalid raw length %d", segment.RawLength)
	}
	if segment.SegmentDataStart < 0 || segment.SegmentDataStart > int64(len(body)) {
		return fmt.Errorf("recovered body has %d bytes but data starts at %d", len(body), segment.SegmentDataStart)
	}
	if segment.RawLength > int64(len(body))-segment.SegmentDataStart {
		return fmt.Errorf("recovered body has %d usable bytes, need %d", int64(len(body))-segment.SegmentDataStart, segment.RawLength)
	}
	return nil
}

func printMissingScanSummary(output io.Writer, summary missingScanSummary) {
	fmt.Fprintln(output, "\n"+strings.Repeat("=", 80))
	fmt.Fprintln(output, "MISSING ARTICLE SWEEP")
	fmt.Fprintln(output, strings.Repeat("=", 80))
	fmt.Fprintf(output, "Distinct logical articles: %d\n", summary.Articles)
	fmt.Fprintf(output, "Available:                 %d\n", summary.Available)
	fmt.Fprintf(output, "Definitively missing:      %d\n", summary.Missing)
	fmt.Fprintf(output, "STAT errors:               %d\n", summary.StatErrors)
	fmt.Fprintf(output, "Ranges repaired:           %d\n", summary.RepairRanges)
	fmt.Fprintf(output, "Repair failures:           %d\n", summary.RepairFailures)
}

func printRecoverySummary(output io.Writer, stats recovery.CoordinatorStats, dbPath string, kept bool) {
	fmt.Fprintln(output, "\n"+strings.Repeat("=", 80))
	fmt.Fprintln(output, "PAR2 RECOVERY SUMMARY")
	fmt.Fprintln(output, strings.Repeat("=", 80))
	fmt.Fprintf(output, "Recovery attempts:      %d\n", stats.RepairAttempts)
	fmt.Fprintf(output, "Recovery successes:     %d\n", stats.RepairSuccesses)
	fmt.Fprintf(output, "Recovery failures:      %d\n", stats.RepairFailures)
	fmt.Fprintf(output, "Stored patch hits:      %d\n", stats.PatchHits)
	fmt.Fprintf(output, "Observed source parts:  %d\n", stats.Observations)
	fmt.Fprintf(output, "PAR2 BODY calls:        %d\n", stats.BODYCalls)
	fmt.Fprintf(output, "PAR2 modeled bytes:     %d\n", stats.ModeledDownloadBytes)
	fmt.Fprintf(output, "Recovery payload bytes: %d\n", stats.RecoveryPayloadBytes)
	fmt.Fprintf(output, "Stored patch bytes:     %d\n", stats.PatchBytes)
	fmt.Fprintf(output, "Store entries:          %d\n", stats.Store.Entries)
	fmt.Fprintf(output, "Store disk bytes:       %d\n", stats.Store.DiskBytes)
	if kept {
		fmt.Fprintf(output, "PAR2 state retained at: %s\n", dbPath)
	} else {
		fmt.Fprintln(output, "PAR2 state:             cleaned after summary")
	}
}

func printFileSummary(output io.Writer, nzb *storage.NZB) {
	fmt.Fprintln(output, "\n"+strings.Repeat("=", 80))
	fmt.Fprintln(output, "FILE SUMMARY")
	fmt.Fprintln(output, strings.Repeat("=", 80))
	fmt.Fprintf(output, "NZB ID:        %s\n", nzb.ID)
	fmt.Fprintf(output, "Name:          %s\n", nzb.Name)
	fmt.Fprintf(output, "Total Size:    %.2f GB\n", float64(nzb.TotalSize)/(1024*1024*1024))
	fmt.Fprintf(output, "Logical Files: %d\n", len(nzb.Files))
	fmt.Fprintln(output, strings.Repeat("=", 80))

	for i, file := range nzb.Files {
		fmt.Fprintf(output, "\n[%d] %s\n", i+1, file.Name)
		fmt.Fprintf(output, "    Size:         %.2f MB (%d bytes)\n", float64(file.Size)/(1024*1024), file.Size)
		fmt.Fprintf(output, "    Segments:     %d\n", len(file.Segments))
		if file.SegmentSize > 0 {
			fmt.Fprintf(output, "    Segment Size: %.2f KB\n", float64(file.SegmentSize)/1024)
		}
		fmt.Fprintf(output, "    Password:     %s\n", getPasswordStatus(file.Password))
		fmt.Fprintf(output, "    Entry:        %s (%d bytes)\n", file.Name, file.Size)
		if file.InternalPath != "" {
			fmt.Fprintf(output, "    Internal:     %s\n", file.InternalPath)
		}
		if file.IsStored {
			fmt.Fprintln(output, "    Compression:  ✅ Stored (seekable)")
		} else {
			fmt.Fprintln(output, "    Compression:  ⚠️  Compressed")
		}
		if len(file.Groups) > 0 {
			fmt.Fprintf(output, "    Groups:       %v\n", file.Groups[:min(3, len(file.Groups))])
		}

		zeroByteCount := 0
		for segmentIndex, segment := range file.Segments {
			if segment.Bytes > 0 {
				continue
			}
			zeroByteCount++
			if zeroByteCount <= 5 {
				fmt.Fprintf(output, "    ⚠️  ZERO BYTE SEG[%d]: Bytes=%d, StartOffset=%d, EndOffset=%d, DataStart=%d\n",
					segmentIndex, segment.Bytes, segment.StartOffset, segment.EndOffset, segment.SegmentDataStart)
			}
		}
		if zeroByteCount > 5 {
			fmt.Fprintf(output, "    ⚠️  ... and %d more zero-byte segments\n", zeroByteCount-5)
		}
		if zeroByteCount > 0 {
			fmt.Fprintf(output, "    ⚠️  TOTAL ZERO-BYTE SEGMENTS: %d (this breaks seeking!)\n", zeroByteCount)
		}
		if len(file.Segments) > 0 {
			segment := file.Segments[0]
			fmt.Fprintf(output, "    First segment: Bytes=%d, StartOff=%d, EndOff=%d, DataStart=%d, Raw=%d:%d+%d\n",
				segment.Bytes, segment.StartOffset, segment.EndOffset, segment.SegmentDataStart,
				segment.RawFileKey, segment.RawOffset, segment.RawLength)
		}
		if len(file.Segments) > 3 {
			segment := file.Segments[3]
			fmt.Fprintf(output, "    Seg[3]: Bytes=%d, StartOff=%d, EndOff=%d, DataStart=%d, Raw=%d:%d+%d\n",
				segment.Bytes, segment.StartOffset, segment.EndOffset, segment.SegmentDataStart,
				segment.RawFileKey, segment.RawOffset, segment.RawLength)
		}
		fmt.Fprintln(output, "    "+strings.Repeat("-", 76))
	}
}

func getPasswordStatus(password string) string {
	if password == "" {
		return "None"
	}
	return "Protected (***)"
}
