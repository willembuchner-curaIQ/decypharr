package reader

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestMemoryWindowSize: the memory-mode RAM ceiling tracks the configured
// read-ahead (3× the prefetch window), floored at 32MB for probe-style
// readers and capped at MaxDisk.
func TestMemoryWindowSize(t *testing.T) {
	seg750 := []SegmentMeta{{Bytes: 750 * 1024}}
	base := DefaultConfig() // MaxDisk 256MB
	cases := []struct {
		name     string
		prefetch int
		segs     []SegmentMeta
		want     int64
	}{
		{"probe-no-readahead", 0, seg750, 32 << 20},
		{"small-readahead-floors", 8, seg750, 32 << 20}, // 3*8*750KB = 17.6MB < floor
		{"default-16MB-readahead", 22, seg750, 3 * 22 * 750 * 1024},
		{"large-readahead-caps", 256, seg750, 256 << 20}, // 3*256*750KB = 576MB > MaxDisk
		{"no-segment-size-fallback", 22, nil, 3 * 22 * 750 * 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.PrefetchAhead = tc.prefetch
			if got := memoryWindowSize(cfg, tc.segs); got != tc.want {
				t.Fatalf("memoryWindowSize = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestMemoryBufferDropsWhenOverBudget: a memory-mode stream that outgrows
// its RAM budget must shed its oldest cached segments (marked Empty for
// re-fetch) and still never write segment data to the disk file.
func TestMemoryBufferDropsWhenOverBudget(t *testing.T) {
	const segSize = 750 * 1024
	const segCount = 8 // 6MB total against a 2MB budget
	segs := make([]SegmentMeta, segCount)
	for i := range segs {
		segs[i] = SegmentMeta{
			MessageID:   fmt.Sprintf("<seg%d@test>", i),
			Number:      i + 1,
			Bytes:       segSize,
			StartOffset: int64(i) * segSize,
			EndOffset:   int64(i+1)*segSize - 1,
		}
	}
	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.MaxDisk = 2 << 20 // caps memoryWindowSize, so drops must fire

	cache, err := NewSegmentCache(context.Background(), segs, cfg, &ReaderStats{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	data := make([]byte, segSize)
	for i := 0; i < segCount; i++ {
		for j := range data {
			data[j] = byte(i + j)
		}
		putSegment(t, cache, i, data)
	}

	// The newest segment survives in RAM; the oldest was shed for re-fetch.
	got := make([]byte, segSize)
	if n, ok := cache.ReadRangeInto(segCount-1, 0, segSize, got); !ok || n != segSize {
		t.Fatalf("newest segment unreadable: n=%d ok=%v", n, ok)
	}
	dropped := 0
	for i := 0; i < segCount; i++ {
		if cache.GetState(i) == StateEmpty {
			dropped++
		}
	}
	if dropped == 0 {
		t.Fatal("expected over-budget stream to shed segments back to StateEmpty")
	}

	// And through it all, no cache directory was ever created — memory mode
	// owns no file.
	entries, err := os.ReadDir(cfg.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("memory mode created cache files: %v", entries)
	}
}

// TestMemorySweeperLeadsEviction: during paced sequential playback the
// budget-derived sweep window reclaims consumed segments BEFORE the RAM
// budget fills, so the buffer's pin-blind block drop (buf Stats.Evictions)
// never fires — the segment-aligned sweeper does all the eviction.
func TestMemorySweeperLeadsEviction(t *testing.T) {
	const segSize = 750 * 1024
	const segCount = 20
	segs := make([]SegmentMeta, segCount)
	for i := range segs {
		segs[i] = SegmentMeta{
			MessageID:   fmt.Sprintf("<seg%d@test>", i),
			Number:      i + 1,
			Bytes:       segSize,
			StartOffset: int64(i) * segSize,
			EndOffset:   int64(i+1)*segSize - 1,
		}
	}
	cfg := DefaultConfig()
	cfg.DiskPath = t.TempDir()
	cfg.MaxDisk = 8 << 20 // caps the budget at 8MB → sweep back-window 4MB

	cache, err := NewSegmentCache(context.Background(), segs, cfg, &ReaderStats{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	data := make([]byte, segSize)
	for i := 0; i < segCount; i++ {
		for j := range data {
			data[j] = byte(i + j)
		}
		putSegment(t, cache, i, data)
		cache.SetConsumedFloor(cache.segOffsets[i+1])
		if i < 7 {
			continue
		}
		// Paced playback: wait for the sweeper to reclaim the segment that
		// fell behind the back-window before streaming on. The sweep that
		// put(i)'s completion signal triggered saw at least put(i-1)'s
		// floor, which puts segment i-7 conservatively past the cutoff.
		lag := i - 7
		deadline := time.Now().Add(5 * time.Second)
		for cache.GetState(lag) != StateEmpty {
			if time.Now().After(deadline) {
				t.Fatalf("sweeper never reclaimed segment %d (state=%v)", lag, cache.GetState(lag))
			}
			time.Sleep(time.Millisecond)
		}
	}

	if drops := cache.buf.Stats().Evictions; drops != 0 {
		t.Fatalf("buffer block-drop fired %d times; the sweeper must lead eviction", drops)
	}
}

// putSegment stores a segment through the production write path
// (StreamWriter + Finalize).
func putSegment(t *testing.T, sc *SegmentCache, segIdx int, data []byte) {
	t.Helper()
	w := sc.StreamWriter(segIdx)
	if w == nil {
		t.Fatalf("StreamWriter(%d) returned nil", segIdx)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("write segment %d: %v", segIdx, err)
	}
	w.Finalize()
}

// TestMemoryBufferKeepsDiskFileEmpty: with Config.MemoryBuffer the segment
// cache must serve every read from resident RAM blocks and create no cache
// directory at all. The disk-backed control proves segments.bin otherwise
// receives the data.
func TestMemoryBufferKeepsDiskFileEmpty(t *testing.T) {
	if !DefaultConfig().MemoryBuffer {
		t.Fatal("MemoryBuffer must default to true: memory mode is the shipped default")
	}
	for _, memory := range []bool{true, false} {
		name := "disk"
		if memory {
			name = "memory"
		}
		t.Run(name, func(t *testing.T) {
			const segSize = 64 << 10
			const segCount = 8
			segs := make([]SegmentMeta, segCount)
			for i := range segs {
				segs[i] = SegmentMeta{
					MessageID:   fmt.Sprintf("<seg%d@test>", i),
					Number:      i + 1,
					Bytes:       segSize,
					StartOffset: int64(i) * segSize,
					EndOffset:   int64(i+1)*segSize - 1,
				}
			}
			cfg := DefaultConfig()
			cfg.DiskPath = t.TempDir()
			cfg.MemoryBuffer = memory

			cache, err := NewSegmentCache(context.Background(), segs, cfg, &ReaderStats{}, zerolog.Nop())
			if err != nil {
				t.Fatalf("NewSegmentCache: %v", err)
			}
			t.Cleanup(func() { _ = cache.Close() })

			data := make([]byte, segSize)
			for i := 0; i < segCount; i++ {
				for j := range data {
					data[j] = byte(i + j)
				}
				putSegment(t, cache, i, data)
			}
			got := make([]byte, segSize)
			for i := 0; i < segCount; i++ {
				n, ok := cache.ReadRangeInto(i, 0, segSize, got)
				if !ok || n != segSize {
					t.Fatalf("ReadRangeInto(%d) = %d, %v", i, n, ok)
				}
				if got[0] != byte(i) || got[segSize-1] != byte(i+segSize-1) {
					t.Fatalf("segment %d data mismatch", i)
				}
			}

			if memory {
				// Memory mode owns no file: the DiskPath dir stays empty.
				entries, err := os.ReadDir(cfg.DiskPath)
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) != 0 {
					t.Fatalf("memory mode created cache files: %v", entries)
				}
				return
			}
			matches, err := filepath.Glob(filepath.Join(cfg.DiskPath, "cache-*", "segments.bin"))
			if err != nil || len(matches) != 1 {
				t.Fatalf("locate segments.bin: %v (%d matches)", err, len(matches))
			}
			onDisk, err := os.ReadFile(matches[0])
			if err != nil {
				t.Fatal(err)
			}
			if len(bytes.Trim(onDisk, "\x00")) == 0 {
				t.Fatal("disk mode wrote nothing to segments.bin")
			}
		})
	}
}
