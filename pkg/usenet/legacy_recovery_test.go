package usenet

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/testutil/nntpd"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/recovery"
)

func TestNeedsPAR2HydrationUsesStoredRecoveryOrigins(t *testing.T) {
	tests := []struct {
		name  string
		nzb   *storage.NZB
		needs bool
	}{
		{
			name: "legacy",
			nzb: &storage.NZB{ID: "legacy", Files: []storage.NZBFile{{
				Name: "movie.mkv", FileType: storage.NZBFileTypeMedia,
				Segments: []storage.NZBSegment{{MessageID: "legacy@example", Bytes: 8}},
			}}},
			needs: true,
		},
		{
			name: "modern",
			nzb: &storage.NZB{ID: "modern", Files: []storage.NZBFile{{
				Name: "movie.mkv", FileType: storage.NZBFileTypeMedia,
				Segments: []storage.NZBSegment{{MessageID: "modern@example", Bytes: 8, RawFileKey: 1, RawLength: 8}},
			}}},
		},
		{
			name: "partially hydrated",
			nzb: &storage.NZB{ID: "partial", Files: []storage.NZBFile{{
				Name: "movie.mkv", FileType: storage.NZBFileTypeMedia,
				Segments: []storage.NZBSegment{
					{MessageID: "modern@example", Bytes: 8, RawFileKey: 1, RawLength: 8},
					{MessageID: "legacy@example", Bytes: 8},
				},
			}}},
			needs: true,
		},
		{
			name: "parity only",
			nzb: &storage.NZB{ID: "parity", Files: []storage.NZBFile{{
				Name: "repair.par2", FileType: storage.NZBFileTypePar2,
				Segments: []storage.NZBSegment{{MessageID: "parity@example"}},
			}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &NZBStorage{metaDir: t.TempDir(), logger: zerolog.Nop()}
			if err := store.AddNZB(test.nzb); err != nil {
				t.Fatal(err)
			}
			u := &Usenet{nzbStorage: store}
			needs, err := u.NeedsPAR2Hydration(test.nzb.ID)
			if err != nil || needs != test.needs {
				t.Fatalf("needs = %t, err = %v; want %t", needs, err, test.needs)
			}
		})
	}
}

func TestValidateLegacyArticleIdentity(t *testing.T) {
	legacy := &storage.NZB{Files: []storage.NZBFile{{
		Name: "renamed/movie.mkv",
		Segments: []storage.NZBSegment{
			{MessageID: "first@example"},
			{MessageID: "second@example"},
		},
	}}}
	manifest := &recovery.Manifest{Files: []recovery.RawFile{
		{Key: 1, Articles: []recovery.Article{{MessageID: "first@example"}}},
		{Key: 2, Articles: []recovery.Article{{MessageID: "second@example"}}},
		{Key: 3, IsPAR2: true, Articles: []recovery.Article{{MessageID: "parity@example"}}},
	}}
	if err := validateLegacyArticleIdentity(legacy, manifest); err != nil {
		t.Fatalf("valid identity: %v", err)
	}

	manifest.Files[1].Articles[0].MessageID = "different@example"
	if err := validateLegacyArticleIdentity(legacy, manifest); !errors.Is(err, ErrLegacyNZBIdentityMismatch) {
		t.Fatalf("missing article error = %v", err)
	}

	manifest.Files[1].Articles[0].MessageID = "second@example"
	manifest.Files = append(manifest.Files, recovery.RawFile{Key: 4, Articles: []recovery.Article{{MessageID: "second@example"}}})
	if err := validateLegacyArticleIdentity(legacy, manifest); !errors.Is(err, ErrLegacyNZBIdentityMismatch) {
		t.Fatalf("ambiguous article error = %v", err)
	}
}

func TestApplyLegacySegmentOriginsPreservesLogicalLayout(t *testing.T) {
	legacy := &storage.NZB{ID: "legacy", Files: []storage.NZBFile{{
		Name:         "Arr Renamed Movie.mkv",
		InternalPath: "original/movie.mkv",
		Segments: []storage.NZBSegment{
			{Number: 4, MessageID: "archive@example", Bytes: 400, StartOffset: 0, EndOffset: 399, SegmentDataStart: 90},
			{Number: 5, MessageID: "archive2@example", Bytes: 600, StartOffset: 400, EndOffset: 999, SegmentDataStart: 0},
		},
	}}}
	candidate := &storage.NZB{ID: "candidate", Files: []storage.NZBFile{{
		Name: "different-parser-name.mkv",
		Segments: []storage.NZBSegment{
			{Number: 4, MessageID: "archive@example", Bytes: 400, SegmentDataStart: 90, RawFileKey: 7, RawOffset: 1234, RawLength: 400},
			{Number: 5, MessageID: "archive2@example", Bytes: 600, SegmentDataStart: 0, RawFileKey: 8, RawOffset: 0, RawLength: 600},
		},
	}}}

	upgraded := cloneNZBMetadata(legacy)
	mapped, err := applyLegacySegmentOrigins(upgraded, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if mapped != 2 {
		t.Fatalf("mapped = %d, want 2", mapped)
	}
	if upgraded.Files[0].Name != legacy.Files[0].Name || upgraded.Files[0].InternalPath != legacy.Files[0].InternalPath {
		t.Fatalf("logical identity changed: %#v", upgraded.Files[0])
	}
	first := upgraded.Files[0].Segments[0]
	if first.RawFileKey != 7 || first.RawOffset != 1234 || first.RawLength != 400 {
		t.Fatalf("first origin = %#v", first)
	}
	if legacy.Files[0].Segments[0].RawFileKey != 0 {
		t.Fatal("clone mutated original metadata")
	}
}

func TestApplyLegacySegmentOriginsRejectsAmbiguousRange(t *testing.T) {
	legacy := &storage.NZB{Files: []storage.NZBFile{{Segments: []storage.NZBSegment{
		{Number: 1, MessageID: "same@example", Bytes: 10},
	}}}}
	candidate := &storage.NZB{Files: []storage.NZBFile{
		{Segments: []storage.NZBSegment{{Number: 1, MessageID: "same@example", Bytes: 10, RawFileKey: 1, RawLength: 10}}},
		{Segments: []storage.NZBSegment{{Number: 1, MessageID: "same@example", Bytes: 10, RawFileKey: 2, RawLength: 10}}},
	}}
	if _, err := applyLegacySegmentOrigins(legacy, candidate); !errors.Is(err, ErrLegacyNZBIdentityMismatch) {
		t.Fatalf("error = %v", err)
	}
}

func TestDeriveDirectMediaOriginsCoversFullyMissingSource(t *testing.T) {
	legacy := &storage.NZB{ID: "direct", Files: []storage.NZBFile{{
		Name: "movie.mkv", FileType: storage.NZBFileTypeMedia,
		Segments: []storage.NZBSegment{
			{Number: 1, MessageID: "first@example", Bytes: 8, StartOffset: 0, EndOffset: 7},
			{Number: 2, MessageID: "second@example", Bytes: 5, StartOffset: 8, EndOffset: 12},
		},
	}}}
	manifest := &recovery.Manifest{NZBID: legacy.ID, Files: []recovery.RawFile{
		{Key: 1, Articles: []recovery.Article{{Number: 1, MessageID: "first@example"}, {Number: 2, MessageID: "second@example"}}},
		{Key: 2, IsPAR2: true, Articles: []recovery.Article{{Number: 1, MessageID: "parity@example"}}},
	}}
	derived, err := deriveDirectMediaOrigins(legacy, manifest)
	if err != nil {
		t.Fatal(err)
	}
	segments := derived.Files[0].Segments
	if segments[0].RawFileKey != 1 || segments[0].RawOffset != 0 || segments[0].RawLength != 8 ||
		segments[1].RawFileKey != 1 || segments[1].RawOffset != 8 || segments[1].RawLength != 5 {
		t.Fatalf("derived origins = %+v", segments)
	}
	raw, ok := manifest.File(1)
	if !ok || raw.Articles[0].Layout != recovery.LayoutExact || raw.Articles[1].DecodedOffset != 8 || raw.Articles[1].DecodedSize != 5 {
		t.Fatalf("derived manifest layout = %+v", raw)
	}
	if legacy.Files[0].Segments[0].RawFileKey != 0 {
		t.Fatal("derive mutated legacy metadata")
	}
}

func TestDeriveDirectMediaOriginsRejectsArchiveSlice(t *testing.T) {
	legacy := &storage.NZB{ID: "archive", Files: []storage.NZBFile{{
		Name: "movie.mkv", InternalPath: "movie.mkv", FileType: storage.NZBFileTypeRar,
		Segments: []storage.NZBSegment{{Number: 1, MessageID: "archive@example", Bytes: 8, SegmentDataStart: 64}},
	}}}
	manifest := &recovery.Manifest{Files: []recovery.RawFile{{
		Key: 1, Articles: []recovery.Article{{Number: 1, MessageID: "archive@example"}},
	}}}
	if _, err := deriveDirectMediaOrigins(legacy, manifest); err == nil {
		t.Fatal("archive slice was treated as complete direct media")
	}
}

func TestLoadLegacyNZBSourceUsesOnlyIDKeyedLocalMetadata(t *testing.T) {
	metaDir := t.TempDir()
	store := &NZBStorage{metaDir: metaDir, logger: zerolog.Nop()}
	const id = "legacy-id"
	if err := store.AddNZB(&storage.NZB{ID: id, Name: "Release.Name", Status: NZBStatusCompleted}); err != nil {
		t.Fatal(err)
	}
	content := []byte("<?xml version=\"1.0\"?><nzb></nzb>")
	if err := os.WriteFile(filepath.Join(metaDir, id+".nzb"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	u := &Usenet{metadataDir: metaDir, nzbStorage: store}
	name, got, err := u.LoadLegacyNZBSource(id)
	if err != nil {
		t.Fatal(err)
	}
	if name != "Release.Name.nzb" || string(got) != string(content) {
		t.Fatalf("name=%q content=%q", name, got)
	}
	if _, _, err := u.LoadLegacyNZBSource("missing"); !errors.Is(err, ErrLegacyNZBSourceUnavailable) {
		t.Fatalf("missing source error = %v", err)
	}
}

func TestHydrateLegacyNZBMapsExactReleaseWithoutRetainingXML(t *testing.T) {
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
	server.AddArticle("<first@nntpd>", nntpd.Encode([]byte("12345678"), "movie.mkv", 1, 16, 0))
	host, port := server.Addr()
	client, err := nntp.NewClient(&config.Config{
		Usenet: config.Usenet{Providers: []config.UsenetProvider{{Host: host, Port: port, MaxConnections: 1}}},
		Repair: config.RepairConfig{NNTPConnectionPercent: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	metaDir := t.TempDir()
	nzbStorage := &NZBStorage{metaDir: metaDir, logger: zerolog.Nop()}
	legacy := &storage.NZB{ID: "legacy-hydrate", Name: "Release", Status: NZBStatusCompleted, Files: []storage.NZBFile{{
		NzbID: "legacy-hydrate", Name: "Arr Movie.mkv", FileType: storage.NZBFileTypeMedia, Size: 16,
		Segments: []storage.NZBSegment{
			{Number: 1, MessageID: "first@nntpd", Bytes: 8, StartOffset: 0, EndOffset: 7},
			{Number: 2, MessageID: "missing@nntpd", Bytes: 8, StartOffset: 8, EndOffset: 15},
		},
	}}}
	if err := nzbStorage.AddNZB(legacy); err != nil {
		t.Fatal(err)
	}

	store, err := recovery.Open(filepath.Join(t.TempDir(), "par2.db"))
	if err != nil {
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

	u := &Usenet{
		nntp: client, nzbStorage: nzbStorage, par2Store: store, par2Recovery: coordinator,
		processingMaxConnections: 1, logger: zerolog.Nop(), metadataDir: t.TempDir(),
	}
	content := []byte(`<?xml version="1.0"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="&quot;movie.mkv&quot; yEnc (1/2)"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="12" number="1">first@nntpd</segment><segment bytes="12" number="2">missing@nntpd</segment></segments></file>
  <file subject="&quot;movie.vol00+01.par2&quot; yEnc (1/1)"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="100" number="1">parity@nntpd</segment></segments></file>
</nzb>`)
	if err := u.HydrateLegacyNZB(t.Context(), legacy.ID, "Release.nzb", content); err != nil {
		t.Fatal(err)
	}

	upgraded, err := nzbStorage.GetNZB(legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.Files[0].Name != "Arr Movie.mkv" {
		t.Fatalf("logical filename changed to %q", upgraded.Files[0].Name)
	}
	segments := upgraded.Files[0].Segments
	if segments[0].RawFileKey != 1 || segments[0].RawOffset != 0 || segments[0].RawLength != 8 ||
		segments[1].RawFileKey != 1 || segments[1].RawOffset != 8 || segments[1].RawLength != 8 {
		t.Fatalf("hydrated origins = %+v", segments)
	}
	manifest, err := store.GetManifest(legacy.ID)
	if err != nil || !manifest.HasPAR2() {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	if server.Bodies.Load() != 1 {
		t.Fatalf("BODY calls = %d, want one bounded header probe", server.Bodies.Load())
	}
	if _, statErr := os.Stat(filepath.Join(u.metadataDir, legacy.ID+".nzb")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("reacquired XML was retained: %v", statErr)
	}
}

func TestHydrateLegacyNZBUsesStoredFullRangesWhenDirectMediaIsFullyMissing(t *testing.T) {
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
	host, port := server.Addr()
	client, err := nntp.NewClient(&config.Config{
		Usenet: config.Usenet{Providers: []config.UsenetProvider{{Host: host, Port: port, MaxConnections: 1}}},
		Repair: config.RepairConfig{NNTPConnectionPercent: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	nzbStorage := &NZBStorage{metaDir: t.TempDir(), logger: zerolog.Nop()}
	legacy := &storage.NZB{ID: "fully-missing", Name: "Release", Status: NZBStatusCompleted, Files: []storage.NZBFile{{
		NzbID: "fully-missing", Name: "Movie.mkv", FileType: storage.NZBFileTypeMedia, Size: 8,
		Segments: []storage.NZBSegment{{Number: 1, MessageID: "missing@nntpd", Bytes: 8, StartOffset: 0, EndOffset: 7}},
	}}}
	if err := nzbStorage.AddNZB(legacy); err != nil {
		t.Fatal(err)
	}
	store, err := recovery.Open(filepath.Join(t.TempDir(), "par2.db"))
	if err != nil {
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
	u := &Usenet{
		nntp: client, nzbStorage: nzbStorage, par2Store: store, par2Recovery: coordinator,
		processingMaxConnections: 1, logger: zerolog.Nop(), metadataDir: t.TempDir(),
	}
	content := []byte(`<?xml version="1.0"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="&quot;movie.mkv&quot; yEnc (1/1)"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="12" number="1">missing@nntpd</segment></segments></file>
  <file subject="&quot;movie.vol00+01.par2&quot; yEnc (1/1)"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="100" number="1">parity@nntpd</segment></segments></file>
</nzb>`)
	if err := u.HydrateLegacyNZB(t.Context(), legacy.ID, "Release.nzb", content); err != nil {
		t.Fatal(err)
	}
	upgraded, err := nzbStorage.GetNZB(legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	segment := upgraded.Files[0].Segments[0]
	if segment.RawFileKey != 1 || segment.RawOffset != 0 || segment.RawLength != 8 {
		t.Fatalf("hydrated origin = %+v", segment)
	}
	if server.Bodies.Load() != 0 {
		t.Fatalf("missing source unexpectedly consumed %d article bodies", server.Bodies.Load())
	}
}
