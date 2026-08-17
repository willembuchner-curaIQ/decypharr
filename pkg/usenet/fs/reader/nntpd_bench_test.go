package reader

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/testutil/nntpd"
)

const (
	benchSegSize = int64(750 * 1024)
	benchSegs    = 32
)

// newBenchStack builds a fake NNTP server carrying benchSegs segments of
// Pattern data, and a client pointed at it.
func newBenchStack(b *testing.B, cfg nntpd.Config) (*nntpd.Server, *nntp.Client, []SegmentMeta) {
	b.Helper()
	srv, err := nntpd.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(srv.Close)

	segs := make([]SegmentMeta, benchSegs)
	fileSize := benchSegSize * benchSegs
	for i := range segs {
		off := int64(i) * benchSegSize
		id := fmt.Sprintf("<seg%d@nntpd>", i)
		srv.AddArticle(id, nntpd.Encode(nntpd.Pattern(off, int(benchSegSize)), "bench.bin", i+1, fileSize, off))
		segs[i] = SegmentMeta{
			MessageID:   id,
			Number:      i + 1,
			Bytes:       benchSegSize,
			StartOffset: off,
			EndOffset:   off + benchSegSize - 1,
		}
	}

	host, port := srv.Addr()
	client, err := nntp.NewClient(&config.Config{
		Usenet: config.Usenet{
			Providers: []config.UsenetProvider{{
				Host:           host,
				Port:           port,
				MaxConnections: 8,
			}},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = client.Close() })
	return srv, client, segs
}

func newBenchReader(b *testing.B, client *nntp.Client, segs []SegmentMeta) *StreamingReader {
	b.Helper()
	sr, err := NewStreamingReader(context.Background(), client, segs,
		WithDiskPath(b.TempDir()),
		WithMaxConnections(8),
		WithPrefetchAhead(8),
	)
	if err != nil {
		b.Fatal(err)
	}
	return sr
}

// BenchmarkColdStream reads the whole file sequentially through a fresh
// reader+cache each iteration: fetch, decode, cache write, cache read.
func BenchmarkColdStream(b *testing.B) {
	_, client, segs := newBenchStack(b, nntpd.Config{})
	fileSize := benchSegSize * benchSegs
	buf := make([]byte, 128*1024)

	b.SetBytes(fileSize)
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		sr := newBenchReader(b, client, segs)
		b.StartTimer()

		for off := int64(0); off < fileSize; off += int64(len(buf)) {
			n, err := sr.ReadAt(buf[:min(int64(len(buf)), fileSize-off)], off)
			if err != nil {
				b.Fatal(err)
			}
			if off == 0 && !bytes.Equal(buf[:n], nntpd.Pattern(0, n)) {
				b.Fatal("payload mismatch at offset 0")
			}
		}

		b.StopTimer()
		_ = sr.Close()
		b.StartTimer()
	}
}

// BenchmarkOpenToFirstByte measures reader construction plus the first 64KB
// read — the time-to-first-byte a mount open pays.
func BenchmarkOpenToFirstByte(b *testing.B) {
	_, client, segs := newBenchStack(b, nntpd.Config{})
	buf := make([]byte, 64*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		sr := newBenchReader(b, client, segs)
		if _, err := sr.ReadAt(buf, 0); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		_ = sr.Close()
		b.StartTimer()
	}
}

// BenchmarkSeekLatency measures the first-byte latency of 64KB reads landing
// on uncached segments while prefetch churn runs, as during real playback
// seeks. Segments are visited in descending order: the prefetch window only
// reaches forward, so every target stays cold, while the prefetcher still
// burns slots on segments the next seek abandons. Reports p50/p99 alongside
// the mean.
func BenchmarkSeekLatency(b *testing.B) {
	for _, rtt := range []time.Duration{0, 30 * time.Millisecond} {
		b.Run(fmt.Sprintf("rtt%dms", rtt/time.Millisecond), func(b *testing.B) {
			_, client, segs := newBenchStack(b, nntpd.Config{RTT: rtt})
			buf := make([]byte, 64*1024)

			var durations []time.Duration
			var sr *StreamingReader
			visited := benchSegs // force a fresh reader on first iteration

			b.ResetTimer()
			for i := range b.N {
				if visited == benchSegs {
					b.StopTimer()
					if sr != nil {
						_ = sr.Close()
					}
					sr = newBenchReader(b, client, segs)
					visited = 0
					b.StartTimer()
				}
				seg := benchSegs - 1 - (i % benchSegs)
				start := time.Now()
				if _, err := sr.ReadAt(buf, int64(seg)*benchSegSize); err != nil {
					b.Fatal(err)
				}
				durations = append(durations, time.Since(start))
				visited++
			}
			b.StopTimer()
			if sr != nil {
				_ = sr.Close()
			}

			slices.Sort(durations)
			if len(durations) > 0 {
				p50 := durations[len(durations)/2]
				p99 := durations[len(durations)*99/100]
				b.ReportMetric(float64(p50.Microseconds())/1000, "p50-ms")
				b.ReportMetric(float64(p99.Microseconds())/1000, "p99-ms")
			}
		})
	}
}

// BenchmarkWarmReread reads a fully cached file, isolating the cache read
// path from any network or decode work.
func BenchmarkWarmReread(b *testing.B) {
	_, client, segs := newBenchStack(b, nntpd.Config{})
	fileSize := benchSegSize * benchSegs
	buf := make([]byte, 128*1024)

	sr := newBenchReader(b, client, segs)
	defer sr.Close()
	readAll := func() {
		for off := int64(0); off < fileSize; off += int64(len(buf)) {
			if _, err := sr.ReadAt(buf[:min(int64(len(buf)), fileSize-off)], off); err != nil {
				b.Fatal(err)
			}
		}
	}
	readAll()

	b.SetBytes(fileSize)
	b.ResetTimer()
	for range b.N {
		readAll()
	}
}
