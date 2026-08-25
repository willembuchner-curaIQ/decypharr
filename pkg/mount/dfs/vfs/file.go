package vfs

import (
	"context"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/pkg/manager"
)

// StreamingFile is the FUSE file interface for VFS
type StreamingFile struct {
	item     *CacheItem
	fileSize int64
	closed   atomic.Bool

	directMu     sync.Mutex
	directOpened bool
	direct       manager.DirectReader
}

// NewStreamingFile creates a new streaming file handle. It returns nil when
// the item has been claimed for teardown by the cache janitor — the caller
// must fetch a fresh item and try again (see Manager.GetFile).
func NewStreamingFile(item *CacheItem) *StreamingFile {
	if !item.Open() { // take an open reference; fails on a claimed item
		return nil
	}

	return &StreamingFile{
		item:     item,
		fileSize: item.info.Size,
	}
}

// ReadAt implements io.ReaderAt using a background context.
// Prefer ReadAtContext when a caller context is available (e.g. from a FUSE handle).
func (f *StreamingFile) ReadAt(p []byte, off int64) (int, error) {
	return f.ReadAtContext(context.Background(), p, off)
}

// ReadAtContext reads from the file, passing ctx into the download layer so
// the operation can be interrupted by a read timeout or client disconnect.
func (f *StreamingFile) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if f.closed.Load() {
		return 0, fs.ErrClosed
	}
	if off < 0 {
		return 0, fs.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= f.fileSize {
		return 0, io.EOF
	}

	readSize := int64(len(p))
	if readSize > f.fileSize-off {
		readSize = f.fileSize - off
		p = p[:readSize]
	}

	f.directMu.Lock()
	if f.closed.Load() {
		f.directMu.Unlock()
		return 0, fs.ErrClosed
	}
	if !f.directOpened {
		direct, err := f.item.cache.manager.OpenDirect(ctx, f.item.entry, f.item.filename)
		if err != nil {
			f.directMu.Unlock()
			return 0, err
		}
		if f.closed.Load() {
			if direct != nil {
				_ = direct.Close()
			}
			f.directMu.Unlock()
			return 0, fs.ErrClosed
		}
		f.direct = direct
		f.directOpened = true
	}
	src := f.direct
	f.directMu.Unlock()

	var n int
	var err error
	if src != nil {
		n, err = src.ReadAtContext(ctx, p, off)
	} else {
		n, err = f.item.ReadAtContext(ctx, p, off)
	}

	if n < int(readSize) && err == nil {
		err = io.EOF
	}
	return n, err
}

// Size returns the file size
func (f *StreamingFile) Size() int64 {
	return f.fileSize
}

// Close closes the file handle
func (f *StreamingFile) Close() error {
	if f.closed.Swap(true) {
		return nil
	}
	f.directMu.Lock()
	direct := f.direct
	f.direct = nil
	f.directMu.Unlock()
	var err error
	if direct != nil {
		err = direct.Close()
	}
	f.item.Release()
	return err
}
