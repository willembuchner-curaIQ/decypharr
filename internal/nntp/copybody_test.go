package nntp

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// sliverReader hands out the payload in tiny chunks, mimicking a yEnc
// decoder whose internal buffer caps every Read at a few KB.
type sliverReader struct {
	data  []byte
	chunk int
	// failAfter, when > 0, returns errSliver once that many bytes have
	// been delivered.
	failAfter int
	delivered int
}

var errSliver = errors.New("sliver read error")

func (r *sliverReader) Read(p []byte) (int, error) {
	if r.failAfter > 0 && r.delivered >= r.failAfter {
		return 0, errSliver
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(r.chunk, len(r.data), len(p))
	copy(p, r.data[:n])
	r.data = r.data[n:]
	r.delivered += n
	return n, nil
}

type sizeRecordingWriter struct {
	buf   bytes.Buffer
	sizes []int
}

func (w *sizeRecordingWriter) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	return w.buf.Write(p)
}

func newCopyBodyConn(t *testing.T) *Connection {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return &Connection{conn: client}
}

// TestCopyBodyCoalescesSmallReads pins the write granularity of the body
// copy: decoder slivers must be accumulated into large writes so the
// segment cache pays a handful of pwrite+lock cycles per segment instead of
// one per decoder Read.
func TestCopyBodyCoalescesSmallReads(t *testing.T) {
	c := newCopyBodyConn(t)

	payload := make([]byte, 300*1024)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	src := &sliverReader{data: append([]byte(nil), payload...), chunk: 3 * 1024}
	dst := &sizeRecordingWriter{}

	n, err := c.copyBodyWithIdleDeadline(dst, src, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("copied %d bytes, want %d", n, len(payload))
	}
	if !bytes.Equal(dst.buf.Bytes(), payload) {
		t.Fatal("payload corrupted by coalescing")
	}

	// The pooled copy buffer is 128KB and flushes at half-full, so every
	// write except the final tail must be at least 64KB.
	const wantMin = 64 * 1024
	for i, size := range dst.sizes[:len(dst.sizes)-1] {
		if size < wantMin {
			t.Fatalf("write %d was %d bytes, want >= %d (writes: %v)", i, size, wantMin, dst.sizes)
		}
	}
}

// TestCopyBodyFlushesBeforeError ensures bytes delivered before a read
// error are written out and counted, not dropped with the error.
func TestCopyBodyFlushesBeforeError(t *testing.T) {
	c := newCopyBodyConn(t)

	payload := make([]byte, 40*1024)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	src := &sliverReader{data: append([]byte(nil), payload...), chunk: 3 * 1024, failAfter: 10 * 1024}
	dst := &sizeRecordingWriter{}

	n, err := c.copyBodyWithIdleDeadline(dst, src, time.Minute)
	if !errors.Is(err, errSliver) {
		t.Fatalf("err = %v, want %v", err, errSliver)
	}
	// failAfter is checked between slivers, so delivery stops at the first
	// chunk boundary at or past the threshold.
	if n == 0 || n != int64(dst.buf.Len()) {
		t.Fatalf("returned %d with %d bytes written", n, dst.buf.Len())
	}
	if !bytes.Equal(dst.buf.Bytes(), payload[:n]) {
		t.Fatal("flushed prefix corrupted")
	}
}
