package reader

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/testutil/nntpd"
)

// These workloads run unchanged on beta and current for direct comparison.
const (
	playbackRTT = 30 * time.Millisecond
	playbackBW  = 8 << 20 // bytes/sec per connection
)

// BenchmarkPlaybackStart reads the header, probes near EOF, then resumes at 0.
func BenchmarkPlaybackStart(b *testing.B) {
	for _, readSize := range []int{128 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("read%dKB", readSize>>10), func(b *testing.B) {
			_, client, segs := newBenchStack(b, nntpd.Config{RTT: playbackRTT, Bandwidth: playbackBW})
			fileSize := benchSegSize * benchSegs
			buf := make([]byte, readSize)

			var durations []time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				dir := b.TempDir()
				b.StartTimer()

				start := time.Now()
				sr := newBenchReader(b, client, segs, true, dir)
				// 1. container header
				if _, err := sr.ReadAt(buf[:64<<10], 0); err != nil {
					b.Fatal(err)
				}
				// 2. probe seek to the index near EOF
				if _, err := sr.ReadAt(buf, fileSize-int64(readSize)); err != nil {
					b.Fatal(err)
				}
				// 3. first decode read
				if _, err := sr.ReadAt(buf, 0); err != nil {
					b.Fatal(err)
				}
				durations = append(durations, time.Since(start))

				b.StopTimer()
				_ = sr.Close()
				b.StartTimer()
			}
			b.StopTimer()
			reportLatency(b, durations)
		})
	}
}

// BenchmarkPlaybackSustain streams the whole file from a cold cache.
func BenchmarkPlaybackSustain(b *testing.B) {
	for _, readSize := range []int{128 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("read%dKB", readSize>>10), func(b *testing.B) {
			_, client, segs := newBenchStack(b, nntpd.Config{RTT: playbackRTT, Bandwidth: playbackBW})
			fileSize := benchSegSize * benchSegs
			buf := make([]byte, readSize)

			var durations []time.Duration
			var total int64
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				dir := b.TempDir()
				sr := newBenchReader(b, client, segs, true, dir)
				b.StartTimer()

				for off := int64(0); off < fileSize; off += int64(readSize) {
					n := min(int64(readSize), fileSize-off)
					start := time.Now()
					if _, err := sr.ReadAt(buf[:n], off); err != nil {
						b.Fatal(err)
					}
					durations = append(durations, time.Since(start))
					total += n
				}

				b.StopTimer()
				_ = sr.Close()
				b.StartTimer()
			}
			b.StopTimer()
			reportLatency(b, durations)
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(total)/(1<<20)/elapsed, "MB/s")
			}
		})
	}
}

func reportLatency(b *testing.B, d []time.Duration) {
	b.Helper()
	if len(d) == 0 {
		return
	}
	slices.Sort(d)
	ms := func(t time.Duration) float64 { return float64(t.Microseconds()) / 1000 }
	b.ReportMetric(ms(d[len(d)/2]), "p50-ms")
	b.ReportMetric(ms(d[len(d)*99/100]), "p99-ms")
	b.ReportMetric(ms(d[len(d)-1]), "max-ms")
}
