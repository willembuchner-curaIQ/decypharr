package usenet

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestNZBStorageUpdateLeavesBytesUntouchedOnMutatorError(t *testing.T) {
	dir := t.TempDir()
	store := &NZBStorage{metaDir: dir, logger: zerolog.Nop()}
	nzb := &storage.NZB{ID: "atomic", Status: NZBStatusCompleted, Files: []storage.NZBFile{{
		Name: "movie.mkv", Segments: []storage.NZBSegment{{MessageID: "one@example", Bytes: 8}},
	}}}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.metaFilePath(nzb.ID))
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("reject update")
	err = store.UpdateNZB(nzb.ID, func(current *storage.NZB) error {
		current.Status = NZBStatusFailed
		current.Files[0].Segments[0].RawFileKey = 9
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("UpdateNZB error = %v", err)
	}
	after, err := os.ReadFile(store.metaFilePath(nzb.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected update changed on-disk metadata")
	}
}

func TestNZBStorageUpdatePreservesCurrentScalars(t *testing.T) {
	dir := t.TempDir()
	store := &NZBStorage{metaDir: dir, logger: zerolog.Nop()}
	nzb := &storage.NZB{ID: "current", Status: NZBStatusCompleted, Files: []storage.NZBFile{{
		Name: "movie.mkv", Segments: []storage.NZBSegment{{MessageID: "one@example", Bytes: 8}},
	}}}
	if err := store.AddNZB(nzb); err != nil {
		t.Fatal(err)
	}
	newer := cloneNZBMetadata(nzb)
	newer.Status = NZBStatusDownloading
	newer.Progress = 0.75
	if err := store.AddNZB(newer); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateNZB(nzb.ID, func(current *storage.NZB) error {
		current.Files[0].Segments[0].RawFileKey = 4
		current.Files[0].Segments[0].RawOffset = 16
		current.Files[0].Segments[0].RawLength = 8
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetNZB(nzb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != NZBStatusDownloading || got.Progress != 0.75 {
		t.Fatalf("current scalars were overwritten: status=%q progress=%v", got.Status, got.Progress)
	}
	segment := got.Files[0].Segments[0]
	if segment.RawFileKey != 4 || segment.RawOffset != 16 || segment.RawLength != 8 {
		t.Fatalf("origin update = %+v", segment)
	}
}

func TestNZBStorageUpdateRejectsIDChange(t *testing.T) {
	store := &NZBStorage{metaDir: t.TempDir(), logger: zerolog.Nop()}
	if err := store.AddNZB(&storage.NZB{ID: "fixed"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateNZB("fixed", func(current *storage.NZB) error {
		current.ID = "changed"
		return nil
	}); err == nil {
		t.Fatal("UpdateNZB accepted an ID change")
	}
	if _, err := store.GetNZB("fixed"); err != nil {
		t.Fatalf("original metadata disappeared: %v", err)
	}
}
