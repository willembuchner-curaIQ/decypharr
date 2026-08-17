package buffer

import (
	"testing"
)

// TestDiskModeFastPathState: every disk-mode write pwrites and publishes,
// and fully-covered blocks flip to the lock-free fast-read state.
func TestDiskModeFastPathState(t *testing.T) {
	p := newTestPool(t, PoolConfig{})
	b := newTestBuffer(t, p, Config{Mode: ModeDisk, TotalSize: 8 << 20})

	data := make([]byte, 4<<20)
	fillPattern(data, 0)
	if _, err := b.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	// Fully-covered blocks must be on the lock-free fast path.
	for blkOff := int64(0); blkOff < 4<<20; blkOff += blockSize {
		if slot := b.stateSlot(blkOff); slot == nil || slot.Load() != stateFastDisk {
			t.Fatalf("block %d not stateFastDisk after full coverage", blkOff)
		}
	}

	// And the bytes are exact.
	got := make([]byte, 4<<20)
	if _, err := b.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	checkPattern(t, got, 0)
	if b.Stats().Misses == 0 {
		t.Fatal("expected the read to be served via the disk path")
	}
}

// TestDiskModeRewriteVisibility: overwrites are immediately visible and
// don't disturb neighbouring bytes.
func TestDiskModeRewriteVisibility(t *testing.T) {
	p := newTestPool(t, PoolConfig{})
	b := newTestBuffer(t, p, Config{Mode: ModeDisk, TotalSize: 4 << 20})

	first := make([]byte, 1<<20)
	fillPattern(first, 0)
	if _, err := b.WriteAt(first, 0); err != nil {
		t.Fatal(err)
	}
	// Overwrite a sub-range and verify the rewrite is what reads back.
	over := make([]byte, 128<<10)
	fillPattern(over, 1<<30) // different pattern seed
	if _, err := b.WriteAt(over, 256<<10); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 128<<10)
	if _, err := b.ReadAt(got, 256<<10); err != nil {
		t.Fatal(err)
	}
	checkPattern(t, got, 1<<30)
	// Neighbouring bytes untouched.
	head := make([]byte, 256<<10)
	if _, err := b.ReadAt(head, 0); err != nil {
		t.Fatal(err)
	}
	checkPattern(t, head, 0)
}
