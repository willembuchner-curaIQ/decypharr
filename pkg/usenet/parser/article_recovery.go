package parser

import (
	"context"
	"fmt"

	"github.com/Tensai75/nzbparser"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs"
	"github.com/sirrobot01/decypharr/pkg/usenet/fs/reader"
)

const maxCorruptHeaderProbes = 4

// headerProbeOrder checks likely boundary/header parts first, then every
// remaining article. A not-found BODY response carries no article payload, so
// walking past missing parts costs round trips rather than release bandwidth.
func headerProbeOrder(count int) []int {
	if count <= 0 {
		return nil
	}
	priority := []int{0, count / 2, count - 1, 1}
	result := make([]int, 0, count)
	seen := make(map[int]struct{}, count)
	add := func(index int) {
		if index < 0 || index >= count {
			return
		}
		if _, ok := seen[index]; ok {
			return
		}
		seen[index] = struct{}{}
		result = append(result, index)
	}
	for _, index := range priority {
		add(index)
	}
	for index := range count {
		add(index)
	}
	return result
}

// fetchFileHeaderPrefix finds one usable article within a raw file. Definitive
// missing responses do not consume article bodies and therefore do not count
// against the corruption-probe ceiling. A decoded-but-corrupt response does
// consume bandwidth, so at most maxCorruptHeaderProbes are attempted.
func fetchFileHeaderPrefix(ctx context.Context, manager *nntp.Client, file nzbparser.NzbFile, maxSnippet int) (*nntp.YencMetadata, error) {
	if len(file.Segments) == 0 {
		return nil, fmt.Errorf("file has no segments")
	}
	if manager == nil {
		return nil, fmt.Errorf("NNTP client is nil")
	}

	var lastErr error
	corruptProbes := 0
	for _, index := range headerProbeOrder(len(file.Segments)) {
		segment := file.Segments[index]
		if segment.Id == "" {
			lastErr = fmt.Errorf("segment %d has no message ID", segment.Number)
			continue
		}

		var metadata *nntp.YencMetadata
		err := manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
			value, fetchErr := conn.GetHeaderPrefix(segment.Id, maxSnippet)
			metadata = value
			return fetchErr
		})
		if err == nil {
			if metadata == nil {
				return nil, fmt.Errorf("segment header contained no yEnc metadata")
			}
			return metadata, nil
		}

		lastErr = err
		if nntp.IsArticleNotFoundError(err) {
			continue
		}
		if !nntp.IsYencDecodeError(err) {
			return nil, err
		}
		corruptProbes++
		if corruptProbes >= maxCorruptHeaderProbes {
			break
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("file has no fetchable segment")
	}
	return nil, lastErr
}

// fetchFileHeader obtains yEnc topology while allowing any healthy part to
// stand in for a missing first article.
func fetchFileHeader(ctx context.Context, manager *nntp.Client, file nzbparser.NzbFile) (*nntp.YencMetadata, error) {
	return fetchFileHeaderPrefix(ctx, manager, file, metadataOnly)
}

// setArticleRecovery attaches the same raw-article repair path used by normal
// streaming. Archive header parsing must use it too: otherwise an NZB can be
// repairable at playback time but fail before its logical files are created.
func (p *NZBParser) SetArticleRecovery(nzbID string, recovery reader.ArticleRecovery) {
	if p == nil {
		return
	}
	p.recoveryNZBID = nzbID
	p.recovery = recovery
}

func (p *NZBParser) fsOptions() []fs.Option {
	if p == nil || p.recovery == nil || p.recoveryNZBID == "" {
		return nil
	}
	return []fs.Option{fs.WithArticleRecovery(p.recoveryNZBID, p.recovery)}
}

// fetchSegmentData returns exactly the raw-file bytes described by segment.
// Normal NNTP/backbone failover always runs first. Only a definitive failure
// reaches PAR2, and both paths apply the same SegmentDataStart/Bytes window.
func fetchSegmentData(
	ctx context.Context,
	manager *nntp.Client,
	nzbID string,
	recovery reader.ArticleRecovery,
	segment storage.NZBSegment,
) ([]byte, error) {
	meta := reader.NewSegmentMeta(segment)
	var (
		body     []byte
		yencMeta *nntp.YencMetadata
	)
	err := manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		decoded, metadata, fetchErr := conn.GetDecodedBodyWithMetadata(segment.MessageID)
		if fetchErr == nil {
			body = decoded
			yencMeta = metadata
		}
		return fetchErr
	})
	if err == nil {
		if recovery != nil && nzbID != "" {
			recovery.ObserveArticle(ctx, nzbID, meta, body, yencMeta)
		}
	} else {
		definitiveArticleFailure := nntp.IsArticleNotFoundError(err) || nntp.IsYencDecodeError(err)
		if !definitiveArticleFailure || recovery == nil || nzbID == "" || segment.RawFileKey == 0 {
			return nil, err
		}
		repaired, repairErr := recovery.RecoverArticle(ctx, nzbID, meta)
		if repairErr != nil {
			return nil, fmt.Errorf("%w (PAR2 recovery: %v)", err, repairErr)
		}
		body = repaired
	}

	start := segment.SegmentDataStart
	if start < 0 || start > int64(len(body)) {
		return nil, fmt.Errorf("decoded segment starts at %d with only %d bytes", start, len(body))
	}
	end := int64(len(body))
	if segment.Bytes > 0 && start+segment.Bytes < end {
		end = start + segment.Bytes
	}
	if end <= start {
		return nil, fmt.Errorf("decoded segment has no usable data")
	}
	return body[start:end:end], nil
}
