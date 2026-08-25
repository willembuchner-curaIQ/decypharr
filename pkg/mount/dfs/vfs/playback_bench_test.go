package vfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/manager"
	fuseconfig "github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Playback profile for a debrid origin: one round trip before the body starts,
// then a capped transfer rate.
const (
	originRTT   = 30 * time.Millisecond
	originBW    = 12 << 20 // bytes/sec
	linkResolve = 120 * time.Millisecond
)

// fakeOrigin serves byte ranges of a synthetic file with an RTT and a
// bandwidth cap, and counts requests so tests can see how many the read path
// actually issues.
type fakeOrigin struct {
	size     int64
	srv      *httptest.Server
	Requests atomic.Int64
	Served   atomic.Int64 // body bytes actually sent

	// originBW is shared across every in-flight request, the way a debrid
	// account's throughput is. Without this, abandoned read-ahead looks free.
	mu       sync.Mutex
	nextFree time.Time
}

// reserve claims n bytes of the shared pipe and returns how long the caller
// must wait for them.
func (o *fakeOrigin) reserve(n int64) time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now()
	if o.nextFree.Before(now) {
		o.nextFree = now
	}
	o.nextFree = o.nextFree.Add(time.Duration(float64(n) / float64(originBW) * float64(time.Second)))
	return time.Until(o.nextFree)
}

func newFakeOrigin(tb testing.TB, size int64) *fakeOrigin {
	tb.Helper()
	o := &fakeOrigin{size: size}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.Requests.Add(1)
		start, end := int64(0), o.size-1
		if h := r.Header.Get("Range"); strings.HasPrefix(h, "bytes=") {
			parts := strings.SplitN(strings.TrimPrefix(h, "bytes="), "-", 2)
			start, _ = strconv.ParseInt(parts[0], 10, 64)
			if len(parts) > 1 && parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			}
		}
		if end >= o.size {
			end = o.size - 1
		}
		n := end - start + 1
		time.Sleep(originRTT) // one round trip before the first body byte
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, o.size))
		w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		w.WriteHeader(http.StatusPartialContent)

		const slice = 64 << 10
		buf := make([]byte, slice)
		for sent := int64(0); sent < n; {
			c := min(int64(slice), n-sent)
			for i := range buf[:c] {
				buf[i] = byte((start + sent + int64(i)) % 251)
			}
			if _, err := w.Write(buf[:c]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			sent += c
			o.Served.Add(c)
			time.Sleep(o.reserve(c))
		}
	}))
	tb.Cleanup(o.srv.Close)
	return o
}

// benchBackend is a Backend whose sessions read from the fake origin. Opening
// one costs linkResolve, standing in for the debrid API call that turns an
// entry into a time-limited download URL.
type benchBackend struct {
	origin   *fakeOrigin
	entry    *storage.Entry
	filename string
	Opens    atomic.Int64
	Resolves atomic.Int64
	resolved atomic.Bool // the real client caches the link per account
}

func (b *benchBackend) GetEntryByName(string, string) (*storage.Entry, error) {
	return b.entry, nil
}
func (b *benchBackend) TrackStream(*storage.Entry, string, string) string { return "bench" }
func (b *benchBackend) UntrackStream(string)                              {}
func (b *benchBackend) OpenDirect(context.Context, *storage.Entry, string) (manager.DirectReader, error) {
	return nil, nil
}

func (b *benchBackend) OpenStreamUntracked(ctx context.Context, _ *storage.Entry, _ string, offset int64) (manager.StreamReader, error) {
	b.Opens.Add(1)
	if b.resolved.CompareAndSwap(false, true) {
		b.Resolves.Add(1)
		select {
		case <-time.After(linkResolve):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &originSession{origin: b.origin, pos: offset, ctx: ctx}, nil
}

// originSession is a seekable stream over the fake origin, reopening the HTTP
// request on each seek the way the real session does.
type originSession struct {
	origin *fakeOrigin
	ctx    context.Context
	pos    int64
	body   io.ReadCloser
	bodyAt int64
}

func (s *originSession) Seek(off int64, _ int) (int64, error) {
	if off != s.pos {
		s.closeBody()
	}
	s.pos = off
	return off, nil
}

func (s *originSession) Read(p []byte) (int, error) {
	if s.pos >= s.origin.size {
		return 0, io.EOF
	}
	if s.body == nil || s.bodyAt != s.pos {
		s.closeBody()
		req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, s.origin.srv.URL, nil)
		if err != nil {
			return 0, err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", s.pos))
		resp, err := s.origin.srv.Client().Do(req)
		if err != nil {
			return 0, err
		}
		s.body, s.bodyAt = resp.Body, s.pos
	}
	n, err := s.body.Read(p)
	s.pos += int64(n)
	s.bodyAt += int64(n)
	return n, err
}

func (s *originSession) closeBody() {
	if s.body != nil {
		_ = s.body.Close()
		s.body = nil
	}
}
func (s *originSession) Close() error { s.closeBody(); return nil }
func (s *originSession) Size() int64  { return s.origin.size }
func (s *originSession) Prime() error { return nil }

func newPlaybackCache(tb testing.TB, size int64, cfg *fuseconfig.FuseConfig) (*Cache, *benchBackend) {
	tb.Helper()
	origin := newFakeOrigin(tb, size)
	be := &benchBackend{
		origin:   origin,
		filename: "movie.mkv",
		entry: &storage.Entry{
			Name:  "Movie.2024",
			Files: map[string]*storage.File{"movie.mkv": {Name: "movie.mkv", Size: size}},
		},
	}
	cfg.CacheDir = tb.TempDir()
	c, err := NewCache(context.Background(), be, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	c.logger = zerolog.Nop()
	tb.Cleanup(func() { c.Close() })
	return c, be
}

func playbackConfig() *fuseconfig.FuseConfig {
	cfg := fuseconfig.DefaultFuseConfig()
	cfg.CacheDiskSize = 500 << 20
	cfg.BufferMemory = 512 << 20
	cfg.ChunkSize = 8 << 20
	cfg.ReadAheadSize = 62 << 20
	cfg.CacheCleanupInterval = time.Hour
	return cfg
}

// BenchmarkDFSPlaybackStart is the "user clicks a movie" sequence against a
// cold cache: container header at the front, the probe seek to the index near
// EOF, then the first decode read. Time per iteration is the spinner.
func BenchmarkDFSPlaybackStart(b *testing.B) {
	const size = int64(4) << 30 // 4GB movie
	cfg := playbackConfig()

	var durations []time.Duration
	var opens, resolves, requests int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c, be := newPlaybackCache(b, size, cfg)
		p := make([]byte, 128<<10)
		ctx := context.Background()
		b.StartTimer()

		start := time.Now()
		item, err := c.GetItem(be.entry.Name, be.filename, size)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := item.ReadAtContext(ctx, p[:64<<10], 0); err != nil {
			b.Fatal(err)
		}
		if _, err := item.ReadAtContext(ctx, p, size-int64(len(p))); err != nil {
			b.Fatal(err)
		}
		if _, err := item.ReadAtContext(ctx, p, 0); err != nil {
			b.Fatal(err)
		}
		durations = append(durations, time.Since(start))

		b.StopTimer()
		opens += be.Opens.Load()
		resolves += be.Resolves.Load()
		requests += be.origin.Requests.Load()
		b.StartTimer()
	}
	b.StopTimer()
	reportPlayback(b, durations)
	if b.N > 0 {
		b.ReportMetric(float64(opens)/float64(b.N), "sessions/play")
		b.ReportMetric(float64(resolves)/float64(b.N), "linkresolves/play")
		b.ReportMetric(float64(requests)/float64(b.N), "http-reqs/play")
	}
}

func reportPlayback(b *testing.B, d []time.Duration) {
	b.Helper()
	if len(d) == 0 {
		return
	}
	slices.Sort(d)
	ms := func(t time.Duration) float64 { return float64(t.Microseconds()) / 1000 }
	b.ReportMetric(ms(d[len(d)/2]), "p50-ms")
	b.ReportMetric(ms(d[len(d)-1]), "max-ms")
}

// BenchmarkDFSSeek models scrubbing: the player jumps to a series of cold
// offsets and waits for the first bytes at each. seek-p50 is how long the
// picture stays frozen after the user lets go of the scrubber.
//
// served-MB against wanted-MB shows whether downloaders abandoned by a seek
// keep pulling bytes nobody asked for — that bandwidth competes with the
// stream the user is actually waiting on.
func BenchmarkDFSSeek(b *testing.B) {
	const size = int64(4) << 30
	const seeks = 8
	const readSize = 128 << 10
	cfg := playbackConfig()

	var durations []time.Duration
	var served, requests int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c, be := newPlaybackCache(b, size, cfg)
		p := make([]byte, readSize)
		ctx := context.Background()
		item, err := c.GetItem(be.entry.Name, be.filename, size)
		if err != nil {
			b.Fatal(err)
		}
		// Start playback so a link is resolved and a downloader is running.
		if _, err := item.ReadAtContext(ctx, p, 0); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for s := 1; s <= seeks; s++ {
			off := size / (seeks + 1) * int64(s)
			start := time.Now()
			if _, err := item.ReadAtContext(ctx, p, off); err != nil {
				b.Fatal(err)
			}
			durations = append(durations, time.Since(start))
		}

		b.StopTimer()
		served += be.origin.Served.Load()
		requests += be.origin.Requests.Load()
		b.StartTimer()
	}
	b.StopTimer()
	reportPlayback(b, durations)
	if b.N > 0 {
		b.ReportMetric(float64(served)/float64(b.N)/(1<<20), "served-MB")
		b.ReportMetric(float64((seeks+1)*readSize)/(1<<20), "wanted-MB")
		b.ReportMetric(float64(requests)/float64(b.N), "http-reqs")
	}
}

// BenchmarkDFSSustain streams a stretch of the file at playback granularity
// from a cold cache. p99 and max are the reads a player would see as a hitch;
// MB/s must stay comfortably above the bitrate it is decoding.
func BenchmarkDFSSustain(b *testing.B) {
	const size = int64(4) << 30
	const span = 48 << 20 // enough to cross several chunks and read-ahead refills
	const readSize = 128 << 10
	cfg := playbackConfig()

	var durations []time.Duration
	var served, requests, total int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c, be := newPlaybackCache(b, size, cfg)
		p := make([]byte, readSize)
		ctx := context.Background()
		item, err := c.GetItem(be.entry.Name, be.filename, size)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for off := int64(0); off < span; off += readSize {
			start := time.Now()
			if _, err := item.ReadAtContext(ctx, p, off); err != nil {
				b.Fatal(err)
			}
			durations = append(durations, time.Since(start))
			total += readSize
		}

		b.StopTimer()
		served += be.origin.Served.Load()
		requests += be.origin.Requests.Load()
		b.StartTimer()
	}
	b.StopTimer()
	reportPlayback(b, durations)
	if el := b.Elapsed().Seconds(); el > 0 {
		b.ReportMetric(float64(total)/(1<<20)/el, "MB/s")
	}
	if b.N > 0 {
		b.ReportMetric(float64(served)/float64(b.N)/(1<<20), "served-MB")
		b.ReportMetric(float64(requests)/float64(b.N), "http-reqs")
	}
}
