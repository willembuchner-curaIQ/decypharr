package vfs

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type testDirectReader struct {
	closes atomic.Int32
}

func (r *testDirectReader) ReadAtContext(_ context.Context, p []byte, _ int64) (int, error) {
	clear(p)
	return len(p), nil
}

func (*testDirectReader) Prefetch(context.Context, int64, int64) {}
func (r *testDirectReader) Close() error {
	r.closes.Add(1)
	return nil
}

type directBackend struct {
	reader  *testDirectReader
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
	fail    atomic.Bool
}

func (b *directBackend) GetEntryByName(string, string) (*storage.Entry, error) {
	return &storage.Entry{}, nil
}
func (*directBackend) TrackStream(*storage.Entry, string, string) string { return "" }
func (*directBackend) UntrackStream(string)                              {}
func (*directBackend) OpenStreamUntracked(context.Context, *storage.Entry, string, int64) (manager.StreamReader, error) {
	return nil, errors.New("unused")
}
func (b *directBackend) OpenDirect(context.Context, *storage.Entry, string) (manager.DirectReader, error) {
	b.calls.Add(1)
	if b.fail.CompareAndSwap(true, false) {
		return nil, errors.New("temporary open failure")
	}
	if b.started != nil {
		close(b.started)
		<-b.release
	}
	return b.reader, nil
}

func newDirectStreamingFile(t *testing.T, backend *directBackend) *StreamingFile {
	t.Helper()
	item := &CacheItem{
		cache:    &Cache{manager: backend},
		entry:    &storage.Entry{},
		filename: "movie.mkv",
		info:     ItemInfo{Size: 1 << 20},
	}
	f := NewStreamingFile(item)
	if f == nil {
		t.Fatal("NewStreamingFile returned nil")
	}
	return f
}

func TestDirectOpenCanRetry(t *testing.T) {
	backend := &directBackend{reader: &testDirectReader{}}
	backend.fail.Store(true)
	f := newDirectStreamingFile(t, backend)
	defer f.Close()

	if _, err := f.ReadAt(make([]byte, 16), 0); err == nil {
		t.Fatal("first open unexpectedly succeeded")
	}
	if n, err := f.ReadAt(make([]byte, 16), 0); err != nil || n != 16 {
		t.Fatalf("retry: n=%d err=%v", n, err)
	}
	if backend.calls.Load() != 2 {
		t.Fatalf("open calls=%d, want 2", backend.calls.Load())
	}
}

func TestDirectOpenRacingCloseDoesNotLeak(t *testing.T) {
	backend := &directBackend{
		reader:  &testDirectReader{},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	f := newDirectStreamingFile(t, backend)

	readDone := make(chan error, 1)
	go func() {
		_, err := f.ReadAtContext(context.Background(), make([]byte, 16), 0)
		readDone <- err
	}()
	<-backend.started

	closeDone := make(chan error, 1)
	go func() { closeDone <- f.Close() }()
	close(backend.release)

	for name, ch := range map[string]<-chan error{"read": readDone, "close": closeDone} {
		select {
		case err := <-ch:
			if name == "close" && err != nil {
				t.Fatalf("Close: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}
	if backend.reader.closes.Load() != 1 {
		t.Fatalf("direct closes=%d, want 1", backend.reader.closes.Load())
	}
	if f.item.opens.Load() != 0 {
		t.Fatalf("item opens=%d, want 0", f.item.opens.Load())
	}
}

var _ manager.DirectReader = (*testDirectReader)(nil)
