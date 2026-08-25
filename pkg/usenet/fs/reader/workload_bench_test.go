package reader

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/testutil/nntpd"
)

func BenchmarkPlaybackPaced(b *testing.B) {
	for _, streams := range []int{1, 4} {
		b.Run(fmt.Sprintf("streams%d", streams), func(b *testing.B) {
			srv, client, segs := newBenchStack(b, nntpd.Config{RTT: 20 * time.Millisecond, Bandwidth: 8 << 20})
			scheduler := NewFetchScheduler(8)
			b.Cleanup(scheduler.Close)
			const playBytes = int64(4 << 20)
			const rate = int64(5_000_000)
			const chunk = 128 << 10
			var latencies []time.Duration
			var peakReaderGoroutines int

			b.SetBytes(playBytes * int64(streams))
			b.ResetTimer()
			for range b.N {
				baseGoroutines := runtime.NumGoroutine()
				readers := make([]*StreamingReader, streams)
				for i := range readers {
					var err error
					readers[i], err = NewStreamingReader(context.Background(), client, segs,
						WithDiskPath(b.TempDir()),
						WithMaxConnections(8),
						WithPrefetchAhead(8),
						WithRetention(RetentionRewind),
						WithFetchScheduler(scheduler),
					)
					if err != nil {
						b.Fatal(err)
					}
				}
				peakReaderGoroutines = max(peakReaderGoroutines, runtime.NumGoroutine()-baseGoroutines)

				start := make(chan struct{})
				perStream := make([][]time.Duration, streams)
				var wg sync.WaitGroup
				for i, sr := range readers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						buf := make([]byte, chunk)
						read := func(off int64, size int) bool {
							t0 := time.Now()
							_, err := sr.ReadAt(buf[:size], off)
							perStream[i] = append(perStream[i], time.Since(t0))
							if err != nil {
								b.Errorf("stream %d read at %d: %v", i, off, err)
								return false
							}
							return true
						}
						if !read(0, 64<<10) || !read(int64(benchSegs)*benchSegSize-chunk, chunk) {
							return
						}
						next := time.Now()
						pace := time.Duration(float64(chunk) / float64(rate) * float64(time.Second))
						for off := int64(0); off < playBytes; off += chunk {
							next = next.Add(pace)
							if wait := time.Until(next); wait > 0 {
								time.Sleep(wait)
							}
							if off == 2<<20 && !read(12<<20, chunk) {
								return
							}
							if off == 3<<20 && !read(1<<20, chunk) {
								return
							}
							if !read(off, chunk) {
								return
							}
						}
					}()
				}
				close(start)
				wg.Wait()
				for i, sr := range readers {
					latencies = append(latencies, perStream[i]...)
					_ = sr.Close()
				}
			}
			b.StopTimer()
			reportLatency(b, latencies)
			b.ReportMetric(float64(peakReaderGoroutines), "reader-goroutines")
			b.ReportMetric(float64(srv.Bodies.Load())/float64(b.N), "source-BODY/op")
		})
	}
}

func BenchmarkRewindOwnership(b *testing.B) {
	type mode struct {
		name       string
		retention  Retention
		downstream bool
	}
	for _, tc := range []mode{
		{"application", RetentionRewind, false},
		{"downstream-full", RetentionWindow, true},
		{"owner-mismatch", RetentionWindow, false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			srv, client, segs := newBenchStack(b, nntpd.Config{RTT: 20 * time.Millisecond, Bandwidth: 8 << 20})
			const consumed = int64(16 << 20)
			buf := make([]byte, 256<<10)
			var rewindLatency []time.Duration
			var rewindRequests int64
			var peakAppRAM int64
			b.ResetTimer()
			for range b.N {
				sr, err := NewStreamingReader(context.Background(), client, segs,
					WithDiskPath(b.TempDir()),
					WithMaxConnections(8),
					WithPrefetchAhead(0),
					WithRetention(tc.retention),
				)
				if err != nil {
					b.Fatal(err)
				}
				var downstream *os.File
				if tc.downstream {
					downstream, err = os.CreateTemp(b.TempDir(), "downstream-*")
					if err != nil {
						b.Fatal(err)
					}
				}
				for off := int64(0); off < consumed; off += int64(len(buf)) {
					n := min(int64(len(buf)), consumed-off)
					if _, err := sr.ReadAt(buf[:n], off); err != nil {
						b.Fatal(err)
					}
					if downstream != nil {
						if _, err := downstream.WriteAt(buf[:n], off); err != nil {
							b.Fatal(err)
						}
					}
				}
				peakAppRAM = max(peakAppRAM, sr.cache.residentN.Load())

				before := srv.Bodies.Load()
				t0 := time.Now()
				if downstream != nil {
					if _, err := downstream.ReadAt(buf, 0); err != nil {
						b.Fatal(err)
					}
				} else if _, err := sr.ReadAt(buf, 0); err != nil {
					b.Fatal(err)
				}
				rewindLatency = append(rewindLatency, time.Since(t0))
				rewindRequests += srv.Bodies.Load() - before
				if downstream != nil {
					_ = downstream.Close()
				}
				_ = sr.Close()
			}
			b.StopTimer()
			slices.Sort(rewindLatency)
			b.ReportMetric(float64(rewindLatency[len(rewindLatency)*99/100].Microseconds())/1000, "rewind-p99-ms")
			b.ReportMetric(float64(rewindRequests)/float64(b.N), "rewind-source-BODY/op")
			b.ReportMetric(float64(peakAppRAM)/(1<<20), "app-live-RAM-MB")
		})
	}
}
