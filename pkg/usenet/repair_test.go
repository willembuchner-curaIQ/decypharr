package usenet

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/testutil/nntpd"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

func TestCollectArticleRepairGroups(t *testing.T) {
	shared := storage.NZBSegment{
		MessageID:        "shared@example",
		RawFileKey:       7,
		RawOffset:        100,
		RawLength:        50,
		SegmentDataStart: 3,
	}
	nzb := &storage.NZB{Files: []storage.NZBFile{
		{Name: "movie.mkv", FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{
			shared,
			{MessageID: "shared@example", RawFileKey: 7, RawOffset: 150, RawLength: 25, SegmentDataStart: 53},
			{MessageID: "unique@example", RawFileKey: 8, RawLength: 100},
			{RawFileKey: 9, RawLength: 100},
		}},
		{Name: "duplicate.mkv", FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{shared}},
		{Name: "ignored.par2", FileType: storage.NZBFileTypePar2, Segments: []storage.NZBSegment{
			{MessageID: "parity@example", RawFileKey: 10, RawLength: 100},
		}},
		{Name: "deleted.mkv", FileType: storage.NZBFileTypeMedia, IsDeleted: true, Segments: []storage.NZBSegment{
			{MessageID: "deleted@example", RawFileKey: 11, RawLength: 100},
		}},
	}}

	groups := collectArticleRepairGroups(nzb)
	if len(groups) != 2 {
		t.Fatalf("groups=%d, want 2: %#v", len(groups), groups)
	}
	if groups[0].messageID != "shared@example" || len(groups[0].targets) != 2 {
		t.Fatalf("shared group=%#v", groups[0])
	}
	if groups[1].messageID != "unique@example" || len(groups[1].targets) != 1 {
		t.Fatalf("unique group=%#v", groups[1])
	}
}

func TestRepairNZBAuditsAllArticlesAndReportsUnrecoverableRanges(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath("")
	})

	server, err := nntpd.New(nntpd.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	server.AddArticle("<available@nntpd>", []byte("article"))
	host, port := server.Addr()
	client, err := nntp.NewClient(&config.Config{
		Usenet: config.Usenet{Providers: []config.UsenetProvider{{
			Host: host, Port: port, MaxConnections: 1,
		}}},
		Repair: config.RepairConfig{NNTPConnectionPercent: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	metaDir := t.TempDir()
	nzbStorage := &NZBStorage{metaDir: metaDir, logger: zerolog.Nop()}
	nzb := &storage.NZB{ID: "scheduled", Files: []storage.NZBFile{{
		Name: "movie.mkv", FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{
			{MessageID: "<available@nntpd>", RawFileKey: 1, RawLength: 8},
			{MessageID: "<missing@nntpd>", RawFileKey: 2, RawLength: 8},
		},
	}}}
	if err := nzbStorage.AddNZB(nzb); err != nil {
		t.Fatal(err)
	}

	store, err := recovery.Open(filepath.Join(t.TempDir(), "par2.db"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := &recovery.Manifest{Version: recovery.ManifestVersion, NZBID: nzb.ID, Files: []recovery.RawFile{
		{Key: 1, BaseFilename: "available.bin", Articles: []recovery.Article{{MessageID: "<available@nntpd>", PostedBytes: 10, DecodedSize: 8, Layout: recovery.LayoutExact}}},
		{Key: 2, BaseFilename: "missing.bin", Articles: []recovery.Article{{MessageID: "<missing@nntpd>", PostedBytes: 10, DecodedSize: 8, Layout: recovery.LayoutExact}}},
		{Key: 3, BaseFilename: "release.par2", IsPAR2: true},
	}}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	coordinator, err := recovery.NewCoordinator(store, client, recovery.Policy{Enabled: true, MaxDownloadBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = coordinator.Close()
		_ = store.Close()
	})

	u := &Usenet{nntp: client, nzbStorage: nzbStorage, par2Store: store, par2Recovery: coordinator}
	report, err := u.RepairNZB(context.Background(), nzb.ID)
	if !errors.Is(err, recovery.ErrNoRecoverySet) {
		t.Fatalf("RepairNZB error=%v, want ErrNoRecoverySet", err)
	}
	if report.Articles != 2 || report.AvailableArticles != 1 || report.MissingArticles != 1 || report.UnknownArticles != 0 {
		t.Fatalf("unexpected audit report: %+v", report)
	}
	if report.RepairRanges != 1 || report.RepairedRanges != 0 || report.FailedRanges != 1 {
		t.Fatalf("unexpected repair report: %+v", report)
	}
	if server.Bodies.Load() != 0 {
		t.Fatalf("BODY calls=%d, want 0", server.Bodies.Load())
	}
}
