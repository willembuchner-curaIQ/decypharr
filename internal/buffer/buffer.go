// Package buffer is a high-performance byte buffer for streaming caches.
// It has exactly two modes, selected by Config.Mode:
//
// # ModeDisk (the default)
//
// A sparse disk-backing file fronted by the kernel page cache. WriteAt
// pwrites straight to the file (outside any lock); ReadAt preads, served by
// the page cache when warm. The buffer itself holds no data in process
// memory — it is a range tracker plus a file. Suits callers whose cache may
// outlive the process (DFS reopens files across runs via InitialRanges) or
// whose working set is larger than RAM is worth.
//
// # ModeMemory
//
// Data lives only in RAM blocks; the buffer owns no file and never touches
// disk. When admitting a block would exceed Config.MemorySize or the pool's
// MemoryBudget, a victim block is dropped: its bytes cease to exist, ReadAt
// for them returns ErrNotPresent, and OnEvict reports the lost ranges so the
// owner can re-fetch on demand. The victim is chosen against the published
// read head (SetReadHead): furthest behind it first, else furthest ahead —
// never the block the consumer is about to read (see pickVictimLocked).
// Suits pure caches whose source of truth is elsewhere (usenet re-downloads
// segments).
//
// Eviction is deliberately inline, not a background worker: dropping a block
// is a bounded scan, a map delete, and a small range-tree edit under a lock
// the write path already holds. A worker would let RAM overshoot between
// signal and trim and still contend on the same lock. The expensive part —
// returning block memory to the OS — is already deferred to the allocator
// (see alloc.go).
//
// # Shared machinery
//
//   - Fixed 1 MB blocks (memory mode), aligned with the natural unit of
//     streaming HTTP/NNTP chunks.
//   - A rangeSet tracks the byte ranges present. ReadAt consults it first:
//     any byte not present returns ErrNotPresent — callers building caches
//     need to distinguish "never written" from "written as zero".
//   - Discard releases a byte range: memory mode frees the covering blocks,
//     disk mode punches a hole (fallocate PUNCH_HOLE on Linux).
//   - A Pool (see pool.go) budgets RAM across memory-mode buffers and disk
//     across disk-mode buffers.
package buffer

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Block size and default RAM budget are internal constants. The package
// shouldn't expose tuning knobs callers won't know how to set; if a future
// workload needs different defaults we'll adjust here based on data.
const (
	blockSize         = 1 << 20 // 1 MB
	blockSizeLog2     = 20      // matches blockSize; used to index the per-block state slot
	defaultMemorySize = 32 << 20

	// Per-block fast-path states (disk mode). The states slice lets ReadAt
	// skip mu and the range tree entirely when every block in the requested
	// range is fully on disk — turning the hot path into a bare pread.
	stateSlow     uint32 = 0 // anything else: take the locked slow path
	stateFastDisk uint32 = 1 // fully present on disk — bare pread is safe
)

// Sentinel errors returned by Buffer methods.
var (
	// ErrNotPresent indicates ReadAt covered a byte range where at least
	// one byte was never written or was subsequently Discarded (or, in
	// memory mode, dropped under RAM pressure).
	ErrNotPresent = errors.New("buffer: range not present")
	// ErrClosed is returned by any operation on a Buffer after Close.
	ErrClosed = errors.New("buffer: closed")
	// ErrOutOfRange is returned for negative offsets or sizes.
	ErrOutOfRange = errors.New("buffer: offset/length out of range")
)

// Range describes a contiguous half-open byte interval [Off, Off+Size).
type Range struct {
	Off, Size int64
}

// Mode selects where a Buffer keeps its bytes.
type Mode int

const (
	// ModeDisk (default): pwrite to a sparse backing file, pread to serve;
	// the kernel page cache is the memory tier. The buffer holds no data in
	// process RAM.
	ModeDisk Mode = iota
	// ModeMemory: RAM blocks only, no backing file, disk is never touched.
	// Over budget, the LRU block is dropped and OnEvict reports the lost
	// ranges so the owner can re-fetch.
	ModeMemory
)

// Stats reports counters for observability. Returned by Buffer.Stats.
type Stats struct {
	BlocksInRAM    int
	BytesInRAM     int64
	BytesPresent   int64 // sum of all present ranges
	Hits           int64 // ReadAt served from RAM (memory mode)
	Misses         int64 // ReadAt served from disk (disk mode)
	DiskWrites     int64 // WriteAt pwrites (disk mode; always 0 in memory mode)
	Evictions      int64 // memory mode: LRU blocks dropped to admit new writes
	HolesPunched   int64
	BytesReclaimed int64
}

// Config configures a new Buffer.
type Config struct {
	// Mode selects disk-backed (default) or memory-only operation.
	Mode Mode

	// MemorySize is the RAM budget for ModeMemory: the maximum bytes held
	// across all blocks before the LRU block is dropped to admit a new one.
	// Default 32 MB when zero; values below one block (1 MB) are clamped up
	// to it. Ignored in ModeDisk, which holds no data in process RAM.
	MemorySize int64

	// DiskPath is the file path for the ModeDisk sparse backing file. If
	// empty, a temp file in os.TempDir() is created and removed on Close.
	// Ignored in ModeMemory — no file is ever created.
	DiskPath string

	// TotalSize is the logical size of the buffer. In ModeDisk it
	// pre-truncates the sparse file (so WriteAt at any offset lands without
	// lazy growth) and sizes the lock-free read fast path. Set to 0 if
	// unknown. Ignored in ModeMemory.
	TotalSize int64

	// InitialRanges, if non-nil, seeds the range tracker with byte ranges
	// already valid on disk from prior runs — ModeDisk reopens only.
	// Without this seed a reopened buffer would treat its file as empty.
	// Ignored in ModeMemory: there is no backing data to reopen.
	InitialRanges []Range

	// OnEvict, if non-nil, reports byte ranges this Buffer released on its
	// own initiative so the owner can keep its metadata in sync:
	//
	//   - ModeDisk: after the owning Pool punches a hole behind the read
	//     head to reclaim disk (DiskLimit pressure).
	//   - ModeMemory: after an LRU block is dropped under RAM pressure.
	//
	// It is NOT called for caller-initiated Discard (the caller already
	// knows). Called with no buffer lock held; must not call back into the
	// buffer in a way that blocks.
	OnEvict func(off, length int64)
}

// Buffer is the public type. Methods are safe for concurrent use.
type Buffer struct {
	cfg Config

	// pool owns this Buffer's RAM/disk budgets and eviction policy. Every
	// Buffer belongs to exactly one Pool.
	pool *Pool

	// onEvict mirrors Config.OnEvict.
	onEvict func(off, length int64)

	// file is the ModeDisk backing file; nil in ModeMemory.
	file     *os.File
	diskTemp bool // remove the file on Close

	// mu guards blocks, the LRU list, bytesInRAM, and ranges. Reads
	// (ReadAt, HasRange, Present, Stats) take RLock so multiple readers
	// proceed in parallel; writers (memory-mode WriteAt, the disk-mode
	// range publish, Discard) take Lock. Disk mode keeps only the pwrite
	// itself outside the lock.
	//
	// Caller contract: do not WriteAt/Discard and ReadAt the same byte
	// range concurrently. Both call sites (usenet SegmentCache, DFS
	// CacheItem) enforce this — segments transition to OnDisk before
	// reads, DFS uses HasRange before serving.
	mu         sync.RWMutex
	blocks     map[int64]*block // ModeMemory resident blocks, keyed by block.off
	bytesInRAM int64
	maxBytes   int64

	ranges *rangeSet

	// states carries the ModeDisk lock-free fast path: when a slot reads
	// stateFastDisk, ReadAt bypasses mu and the range tree and just
	// preads, adding nothing more than one atomic.Load per block.
	states []atomic.Uint32

	// readHead is the caller's current read position (see SetReadHead). It
	// is the frontier the owning Pool's disk backstop punches behind (it
	// reclaims [0, readHead-BackWindow)). Zero means "no hint".
	readHead atomic.Int64

	// diskBytes is this Buffer's running total of on-disk present bytes
	// (ModeDisk only), mirrored into pool.diskInUse to enforce DiskLimit.
	diskBytes atomic.Int64

	// alloc owns block memory for this Buffer (ModeMemory). Per-Buffer
	// rather than package-global: each Buffer is one stream with its own
	// working set. Blocks outside a small reuse window are returned to the
	// OS promptly (asynchronously — see alloc.go), so RSS tracks the live
	// working set.
	alloc blockAllocator

	closed atomic.Bool

	// dropBehindPos is the high-water offset up to which the disk file's
	// page cache has been dropped by DropBehind. Monotonic.
	dropBehindPos atomic.Int64

	// Stats counters. Atomic to allow Stats() without holding mu.
	statsHits       atomic.Int64
	statsMisses     atomic.Int64
	statsDiskWrites atomic.Int64
	statsEvictions  atomic.Int64
	statsPunches    atomic.Int64
	statsReclaimed  atomic.Int64
}

// newBuffer creates a Buffer bound to pool p. Callers go through
// Pool.NewBuffer.
func newBuffer(p *Pool, cfg Config) (*Buffer, error) {
	if cfg.TotalSize < 0 {
		return nil, ErrOutOfRange
	}

	b := &Buffer{
		cfg:     cfg,
		pool:    p,
		onEvict: cfg.OnEvict,
		blocks:  make(map[int64]*block),
		ranges:  newRangeSet(),
	}

	switch cfg.Mode {
	case ModeMemory:
		if cfg.MemorySize <= 0 {
			cfg.MemorySize = defaultMemorySize
		}
		if cfg.MemorySize < blockSize {
			// Block-granular cache: a budget below one block would make
			// the first allocation unsatisfiable.
			cfg.MemorySize = blockSize
		}
		b.cfg = cfg
		b.maxBytes = cfg.MemorySize
		// Size the reuse free list to the working set, capped — a Buffer
		// never needs to retain more idle blocks than it can hold resident.
		b.alloc.maxFree = min(int(b.maxBytes/blockSize), maxReuseBlocks)
		if b.alloc.maxFree < 1 {
			b.alloc.maxFree = 1
		}

	case ModeDisk:
		var (
			file     *os.File
			diskTemp bool
			err      error
		)
		if cfg.DiskPath == "" {
			file, err = os.CreateTemp("", "buffer-*")
			diskTemp = true
		} else {
			if dir := filepath.Dir(cfg.DiskPath); dir != "" && dir != "." {
				if err = os.MkdirAll(dir, 0o755); err != nil {
					return nil, fmt.Errorf("buffer: create disk dir: %w", err)
				}
			}
			file, err = os.OpenFile(cfg.DiskPath, os.O_RDWR|os.O_CREATE, 0o600)
		}
		if err != nil {
			return nil, fmt.Errorf("buffer: open disk file: %w", err)
		}
		if cfg.TotalSize > 0 {
			if err := file.Truncate(cfg.TotalSize); err != nil {
				_ = file.Close()
				if diskTemp {
					_ = os.Remove(file.Name())
				}
				return nil, fmt.Errorf("buffer: truncate disk file: %w", err)
			}
			n := int((cfg.TotalSize + blockSize - 1) / blockSize)
			b.states = make([]atomic.Uint32, n)
		}
		b.file = file
		b.diskTemp = diskTemp
		for _, r := range cfg.InitialRanges {
			if r.Size > 0 {
				// Seeded ranges are already on disk from a prior run: count
				// them toward this Buffer's (and the pool's) disk footprint.
				b.rangesInsert(r.Off, r.Size)
			}
		}
		// Seed fast-path state for any block fully covered by InitialRanges.
		// Cheap one-shot walk; the alternative is paying the slow path
		// forever on a reopen of a fully-cached file.
		if b.states != nil {
			for i := range b.states {
				blkOff := int64(i) << blockSizeLog2
				if b.ranges.present(blkOff, blockSize) {
					b.states[i].Store(stateFastDisk)
				}
			}
		}
		// Linux page-cache hint: the workload is dominated by sequential
		// streaming, so let the kernel ramp readahead aggressively.
		adviseSequential(file)

	default:
		return nil, fmt.Errorf("buffer: unknown mode %d", cfg.Mode)
	}

	return b, nil
}

// WriteAt writes p at offset off. Returns the number of bytes written.
// Data is visible to ReadAt as soon as WriteAt returns.
//
//   - ModeDisk: pwrite outside the lock, then publish the range. Concurrent
//     readers are never stalled behind the write syscall.
//   - ModeMemory: copy into a RAM block, dropping LRU blocks first if the
//     budget requires it (their lost ranges are reported via OnEvict).
func (b *Buffer) WriteAt(p []byte, off int64) (int, error) {
	if b.closed.Load() {
		return 0, ErrClosed
	}
	if off < 0 || len(p) == 0 {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, ErrOutOfRange
	}

	end := off + int64(len(p))
	for cur := off; cur < end; {
		blockOff := alignDown(cur)
		blockEnd := blockOff + blockSize
		lo := int(cur - blockOff)
		hi := int(min(end, blockEnd) - blockOff)
		srcLo := int(cur - off)
		var err error
		if b.cfg.Mode == ModeMemory {
			err = b.writeMemRegion(blockOff, lo, hi, p[srcLo:srcLo+(hi-lo)])
		} else {
			err = b.writeDiskRegion(blockOff, lo, hi, p[srcLo:srcLo+(hi-lo)])
		}
		if err != nil {
			return srcLo, err
		}
		cur += int64(hi - lo)
	}
	return len(p), nil
}

// writeMemRegion copies src into the RAM block at blockOff covering [lo, hi),
// admitting the block (and dropping LRU blocks for room) if needed.
func (b *Buffer) writeMemRegion(blockOff int64, lo, hi int, src []byte) error {
	b.mu.Lock()
	// Re-check under the lock: a WriteAt past the entry closed-check that
	// loses the race with Close must not allocate blocks or publish ranges —
	// the pool accounting Close just settled would drift permanently.
	if b.closed.Load() {
		b.mu.Unlock()
		return ErrClosed
	}
	blk, ok := b.blocks[blockOff]
	var dropped []Range
	if !ok {
		var err error
		if blk, dropped, err = b.acquireBlockLocked(blockOff); err != nil {
			b.mu.Unlock()
			return err
		}
	}
	copy(blk.data[lo:hi], src)
	b.rangesInsert(blockOff+int64(lo), int64(hi-lo))
	b.mu.Unlock()

	// Report any blocks dropped to make room, outside the lock (OnEvict
	// contract), so the owner can mark the lost data for re-fetch.
	if b.onEvict != nil {
		for _, r := range dropped {
			b.onEvict(r.Off, r.Size)
		}
	}
	return nil
}

// writeDiskRegion pwrites src at its absolute offset, then publishes the
// range. The range isn't in b.ranges until the publish, so no reader can
// reach the file for these bytes mid-write — the pwrite needs no lock.
func (b *Buffer) writeDiskRegion(blockOff int64, lo, hi int, src []byte) error {
	diskOff := blockOff + int64(lo)
	if _, err := b.file.WriteAt(src, diskOff); err != nil {
		return fmt.Errorf("buffer: disk write at %d: %w", diskOff, err)
	}
	b.statsDiskWrites.Add(1)

	b.mu.Lock()
	// Same closed re-check as the memory path: never publish into the range
	// tracker after Close has settled the disk accounting.
	if b.closed.Load() {
		b.mu.Unlock()
		return ErrClosed
	}
	b.rangesInsert(diskOff, int64(hi-lo))
	// Fast-path: if this write completed the block, future reads can skip
	// all locking and go straight to pread.
	b.markStateForBlockLocked(blockOff)
	b.mu.Unlock()
	return nil
}

// ReadAt reads len(p) bytes from offset off. Every byte in the range must be
// present (see the rangeSet notes in the package doc); otherwise it returns
// ErrNotPresent.
func (b *Buffer) ReadAt(p []byte, off int64) (int, error) {
	if b.closed.Load() {
		return 0, ErrClosed
	}
	if off < 0 || len(p) == 0 {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, ErrOutOfRange
	}
	if b.cfg.Mode == ModeMemory {
		return b.readMem(p, off)
	}
	return b.readDisk(p, off)
}

// readMem serves the range from resident RAM blocks. Presence implies
// residency in memory mode — blocks and ranges are only ever released
// together — so a present range with a missing block is a broken invariant.
func (b *Buffer) readMem(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	b.mu.RLock()
	if !b.ranges.present(off, int64(len(p))) {
		b.mu.RUnlock()
		return 0, ErrNotPresent
	}
	for cur := off; cur < end; {
		blockOff := alignDown(cur)
		blockEnd := blockOff + blockSize

		readLo := int(cur - blockOff)
		readHi := int(min(end, blockEnd) - blockOff)
		dstLo := int(cur - off)

		blk, ok := b.blocks[blockOff]
		if !ok {
			b.mu.RUnlock()
			return dstLo, io.ErrUnexpectedEOF
		}
		copy(p[dstLo:dstLo+(readHi-readLo)], blk.data[readLo:readHi])
		cur += int64(readHi - readLo)
	}
	b.mu.RUnlock()
	b.statsHits.Add(1)
	return len(p), nil
}

// readDisk serves the range with pread, warm from the kernel page cache.
func (b *Buffer) readDisk(p []byte, off int64) (int, error) {
	end := off + int64(len(p))

	// Fast path: if every block in the read range is stateFastDisk (fully
	// on disk) we can bypass the lock and the range tree entirely and just
	// pread, with one atomic.Load per block as the only overhead.
	//
	// Safety relies on the same caller contract that the locked path does:
	// a range being read isn't being concurrently written or Discarded.
	//
	// One concurrent Discarder is NOT the caller: the pool's disk backstop
	// (punchBehindWindow) punches everything below readHead-BackWindow on
	// its own goroutine. Because this path preads after the lock-free state
	// load with no re-validation, a punch landing between the two would read
	// back a hole as zeros. The invariant that prevents it is the readHead
	// contract: callers MUST publish a read head that covers off BEFORE
	// issuing the read (see SetReadHead), so the backstop's ceiling never
	// reaches the range in flight.
	if b.states != nil {
		allFast := true
		for cur := off; cur < end; cur = alignDown(cur) + blockSize {
			slot := b.stateSlot(alignDown(cur))
			if slot == nil || slot.Load() != stateFastDisk {
				allFast = false
				break
			}
		}
		if allFast {
			n, err := b.file.ReadAt(p, off)
			if err != nil && !errors.Is(err, io.EOF) {
				return n, fmt.Errorf("buffer: disk read at %d: %w", off, err)
			}
			if n < len(p) {
				// The state slots said this range is fully on disk; a short
				// pread of a present range means an invariant broke.
				return n, io.ErrUnexpectedEOF
			}
			b.statsMisses.Add(1)
			return n, nil
		}
	}

	// Slow path: the presence check and the pread share one critical
	// section so a concurrent Discard can't flip state between check and
	// read. pread itself is thread-safe; the caller contract forbids a
	// concurrent write/discard of THIS range.
	b.mu.RLock()
	if !b.ranges.present(off, int64(len(p))) {
		b.mu.RUnlock()
		return 0, ErrNotPresent
	}
	n, err := b.file.ReadAt(p, off)
	b.mu.RUnlock()
	if err != nil && !errors.Is(err, io.EOF) {
		return n, fmt.Errorf("buffer: disk read at %d: %w", off, err)
	}
	if n < len(p) {
		return n, io.ErrUnexpectedEOF
	}
	b.statsMisses.Add(1)
	return n, nil
}

// Discard releases the byte range [off, off+length): memory mode frees the
// covering RAM blocks, disk mode punches a hole in the file. After Discard
// returns, ReadAt for any byte in the range returns ErrNotPresent until it
// is re-written.
func (b *Buffer) Discard(off, length int64) error {
	if b.closed.Load() {
		return ErrClosed
	}
	if off < 0 || length <= 0 {
		if length == 0 {
			return nil
		}
		return ErrOutOfRange
	}
	b.discard(off, length)
	return nil
}

// discard is the core of Discard and of the pool's punch-behind backstop.
// Returns the number of present bytes reclaimed. It does NOT fire onEvict —
// that is the backstop's job, since caller-initiated Discard already knows
// what it released.
func (b *Buffer) discard(off, length int64) int64 {
	end := off + length

	b.mu.Lock()
	// Re-check under the lock: the pool's punch backstop passes Discard's
	// entry check on its own goroutine and can lose the race with Close.
	// Close settles this Buffer's footprint against the pool exactly once;
	// a discard slipping in afterwards would corrupt that accounting.
	if b.closed.Load() {
		b.mu.Unlock()
		return 0
	}
	if b.cfg.Mode == ModeMemory {
		// Blocks fully inside the discard range are dropped now; boundary
		// blocks are handled below once the range removal has settled.
		for blkOff := alignDown(off); blkOff < end; blkOff += blockSize {
			if blk, ok := b.blocks[blkOff]; ok && blkOff >= off && blkOff+blockSize <= end {
				b.dropBlockLocked(blk)
			}
		}
	}
	removed := b.rangesRemove(off, length)
	if b.cfg.Mode == ModeMemory {
		// A boundary block whose cumulative trims (this call plus earlier
		// ones) removed its last present byte holds no live data — drop it,
		// or its RAM stays pinned until Close. Only the two boundary blocks
		// can be in this state; interior blocks were dropped above.
		for _, blkOff := range [2]int64{alignDown(off), alignDown(end - 1)} {
			if blk, ok := b.blocks[blkOff]; ok && !b.ranges.anyPresent(blkOff, blockSize) {
				b.dropBlockLocked(blk)
			}
		}
	} else if b.states != nil {
		// Any FastDisk block whose bytes we're punching must drop back to
		// the slow path so readers don't pread the soon-to-be-hole.
		for blkOff := alignDown(off); blkOff < end; blkOff += blockSize {
			b.markStateForBlockLocked(blkOff)
		}
	}
	b.mu.Unlock()

	if b.cfg.Mode == ModeDisk {
		// Punch on disk outside the lock — file ops are thread-safe and the
		// caller doesn't want to block readers behind a syscall.
		if err := punchHole(b.file, off, length); err == nil {
			b.statsPunches.Add(1)
			b.statsReclaimed.Add(removed)
		}
		// Drop the kernel's page-cache mirror of the discarded range too.
		adviseDontNeed(b.file, off, length)
	}
	return removed
}

// punchBehindWindow reclaims disk by punching every present range below
// readHead-backWindow. Invoked by the owning Pool when it is over its
// DiskLimit; ModeMemory buffers hold no disk and are exempt. Fires onEvict
// for each reclaimed range so the owner can update its persisted metadata.
func (b *Buffer) punchBehindWindow(backWindow int64) int64 {
	if b.closed.Load() || b.cfg.Mode != ModeDisk {
		return 0
	}
	head := b.readHead.Load()
	if head <= 0 {
		return 0
	}
	ceiling := head - backWindow
	if ceiling <= 0 {
		return 0
	}
	b.mu.RLock()
	present := b.ranges.presentRanges(0, ceiling)
	b.mu.RUnlock()
	if len(present) == 0 {
		return 0
	}
	var reclaimed int64
	for _, r := range present {
		n := b.discard(r.Off, r.Size)
		if n > 0 {
			reclaimed += n
			if b.onEvict != nil {
				b.onEvict(r.Off, r.Size)
			}
		}
	}
	if reclaimed > 0 {
		b.pool.statsPunches.Add(1)
		b.pool.statsReclaimed.Add(reclaimed)
	}
	return reclaimed
}

// rangesInsert records presence of [off, off+length) in the range tracker.
// In ModeDisk the newly-covered bytes also count toward this Buffer's and
// the pool's disk footprint (DiskLimit enforcement); ModeMemory bytes are
// accounted block-granular via bytesInRAM instead. Caller holds b.mu (or is
// in single-threaded construction).
func (b *Buffer) rangesInsert(off, length int64) {
	added := b.ranges.insert(off, length)
	if added > 0 && b.cfg.Mode == ModeDisk {
		b.diskBytes.Add(added)
		b.pool.addDisk(added)
	}
}

// rangesRemove drops [off, off+length) from the range tracker, mirroring the
// ModeDisk footprint accounting of rangesInsert. Returns bytes removed.
// Caller holds b.mu.
func (b *Buffer) rangesRemove(off, length int64) int64 {
	removed := b.ranges.remove(off, length)
	if removed > 0 && b.cfg.Mode == ModeDisk {
		b.diskBytes.Add(-removed)
		b.pool.subDisk(removed)
	}
	return removed
}

// HasRange reports whether [off, off+length) is fully present.
func (b *Buffer) HasRange(off, length int64) bool {
	if b.closed.Load() || length <= 0 {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.ranges.present(off, length)
}

// Present returns the buffered subranges within [off, off+length). Useful
// for "what's still missing?" decisions in cache callers.
func (b *Buffer) Present(off, length int64) []Range {
	if b.closed.Load() || length <= 0 {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.ranges.presentRanges(off, length)
}

// WillRead hints to the kernel that the given byte range will be read soon,
// so it can prefetch on-disk pages into its page cache. ModeDisk only;
// non-blocking, cheap, and a no-op on non-Linux platforms.
func (b *Buffer) WillRead(off, length int64) {
	if b.closed.Load() || length <= 0 || b.file == nil {
		return
	}
	adviseWillNeed(b.file, off, length)
}

// DropBehind releases the disk file's page cache for the region more than
// `margin` bytes behind `offset` (the current read position). Unlike Discard
// it does NOT punch a hole — the bytes stay on disk, so a seek-back past the
// margin re-reads from disk rather than re-downloading. ModeDisk only;
// monotonic, lock-free, and a no-op on non-Linux platforms.
func (b *Buffer) DropBehind(offset, margin int64) {
	if b.closed.Load() || margin <= 0 || offset <= margin || b.file == nil {
		return
	}
	target := offset - margin
	// Advance the high-water mark atomically; only one caller wins a given
	// advance, so we never re-fadvise an already-dropped range.
	for {
		prev := b.dropBehindPos.Load()
		if target <= prev {
			return
		}
		if b.dropBehindPos.CompareAndSwap(prev, target) {
			adviseDropBehind(b.file, prev, target)
			return
		}
	}
}

// Sync fsyncs the ModeDisk backing file. ModeMemory has nothing to sync —
// disk is never written — so it is a no-op there. Note that ModeDisk writes
// are buffered pwrites: data is durable in the page cache immediately and on
// stable storage only after Sync (or kernel writeback).
func (b *Buffer) Sync() error {
	if b.closed.Load() {
		return ErrClosed
	}
	if b.file == nil {
		return nil
	}
	return b.file.Sync()
}

// Close releases everything the Buffer owns. Memory mode: every resident
// block is unmapped and the allocator's reuse list drained, so the whole RAM
// footprint returns to the OS the instant the owner closes it. Disk mode:
// the page cache is released, the file closed, and (if it was a temp file)
// removed. Subsequent calls return ErrClosed.
func (b *Buffer) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}

	// Holding the lock excludes readers (RLock), so no goroutine can be
	// touching a block's memory as we unmap it.
	b.mu.Lock()
	for off, blk := range b.blocks {
		munmapBlock(blk.bufPtr)
		delete(b.blocks, off)
	}
	b.pool.dropBytes(b.bytesInRAM) // release this Buffer's share of the pool RAM budget
	b.bytesInRAM = 0
	// Release this Buffer's share of the pool disk footprint and unregister
	// it so the disk backstop stops considering it.
	if db := b.diskBytes.Swap(0); db > 0 {
		b.pool.subDisk(db)
	}
	// Empty the range tracker so a racing discard/publish that somehow
	// reaches it (belt and suspenders on top of the closed re-checks) finds
	// nothing to remove and cannot perturb the pool accounting again.
	b.ranges = newRangeSet()
	b.mu.Unlock()
	b.pool.remove(b)

	// Return the reuse free list to the OS as well.
	b.alloc.drain()

	if b.file == nil {
		return nil
	}
	// Release the kernel's page-cache footprint for this file before
	// closing it. Owners routinely follow Close with removing the file;
	// any pages still cached are dead weight. No-op on non-Linux.
	adviseDontNeedAll(b.file)
	closeErr := b.file.Close()
	if b.diskTemp {
		_ = os.Remove(b.file.Name())
	}
	return closeErr
}

// SetReadHead publishes the caller's current read position. Disk mode: it
// is the frontier the owning Pool's disk backstop punches behind (under
// DiskLimit pressure the pool reclaims [0, off-BackWindow)). Memory mode:
// it steers drop-victim selection away from the data the consumer is about
// to read (see pickVictimLocked). Pass 0 to clear. Cheap, atomic, no lock.
//
// Contract for backstop safety (ModeDisk with DiskLimit > 0): publish a read
// head that covers the offset you are about to read BEFORE issuing that
// read. The lock-free fast read path does not re-validate after its state
// load, so a read below readHead-BackWindow can race a punch and read a hole
// back as zeros. Calling SetReadHead(off) first closes that window (notably
// on a seek-back).
func (b *Buffer) SetReadHead(off int64) {
	if off < 0 {
		off = 0
	}
	// Plain store, not monotonic: usenet feeds an already-monotonic consumed
	// cursor, while DFS feeds its actual read position so a real seek-back
	// pulls the frontier back and re-protects the region now being read.
	b.readHead.Store(off)
}

// Stats returns the current observability counters.
func (b *Buffer) Stats() Stats {
	b.mu.RLock()
	blocksInRAM := len(b.blocks)
	bytesInRAM := b.bytesInRAM
	bytesPresent := b.ranges.totalSize()
	b.mu.RUnlock()
	return Stats{
		BlocksInRAM:    blocksInRAM,
		BytesInRAM:     bytesInRAM,
		BytesPresent:   bytesPresent,
		Hits:           b.statsHits.Load(),
		Misses:         b.statsMisses.Load(),
		DiskWrites:     b.statsDiskWrites.Load(),
		Evictions:      b.statsEvictions.Load(),
		HolesPunched:   b.statsPunches.Load(),
		BytesReclaimed: b.statsReclaimed.Load(),
	}
}

// -----------------------------------------------------------------------
// Internal: memory-mode block cache
// -----------------------------------------------------------------------

// acquireBlockLocked returns the resident block at blockOff, allocating one
// if needed. Making room is inline: while admitting the block would exceed
// the per-stream budget or the pool's, a victim block is dropped — its data
// ceases to exist (ranges removed, so reads return ErrNotPresent) and its
// present ranges are returned for the caller to report via OnEvict after
// releasing b.mu. Caller holds b.mu.
func (b *Buffer) acquireBlockLocked(blockOff int64) (*block, []Range, error) {
	if blk, ok := b.blocks[blockOff]; ok {
		return blk, nil, nil
	}

	var dropped []Range
	for b.bytesInRAM+blockSize > b.maxBytes || b.pool.wouldExceedMemory() {
		victim := b.pickVictimLocked()
		if victim == nil {
			// Pure pool pressure with nothing of our own to drop: allocate
			// anyway (bounded overshoot — one block per actively-allocating
			// Buffer). Looping here would spin forever under the lock.
			break
		}
		dropped = append(dropped, b.ranges.presentRanges(victim.off, blockSize)...)
		b.rangesRemove(victim.off, blockSize)
		b.dropBlockLocked(victim)
		b.statsEvictions.Add(1)
	}

	bufPtr := b.alloc.get()
	blk := &block{
		off:    blockOff,
		data:   (*bufPtr)[:blockSize],
		bufPtr: bufPtr,
	}
	b.blocks[blockOff] = blk
	b.bytesInRAM += int64(blockSize)
	b.pool.addBlock()
	return blk, dropped, nil
}

// pickVictimLocked selects the resident block whose loss hurts a streaming
// consumer least, judged against the published read head (SetReadHead —
// usenet feeds its consumed floor): the block furthest BEHIND the head
// first (already-played history), else the block furthest AHEAD (deepest
// prefetch, re-fetched asynchronously long before the player arrives).
// Never the block nearest the head — write-order (LRU) picked exactly that
// block whenever pressure arrived while the consumer trailed the download
// frontier, dropping the bytes about to be read and stalling playback in a
// re-fetch loop. With no head published (0), everything counts as ahead, so
// the deepest prefetch goes first and the start of the file — what a fresh
// consumer reads next — survives. O(resident blocks), which the budget caps
// at a few hundred. Caller holds b.mu; returns nil when no block is
// resident.
func (b *Buffer) pickVictimLocked() *block {
	head := b.readHead.Load()
	var behind, ahead *block
	for _, blk := range b.blocks {
		if blk.off+blockSize <= head {
			if behind == nil || blk.off < behind.off {
				behind = blk
			}
		} else if ahead == nil || blk.off > ahead.off {
			ahead = blk
		}
	}
	if behind != nil {
		return behind
	}
	return ahead
}

// dropBlockLocked removes a block from the cache and returns its buffer to
// the allocator. The block's data is gone — callers must have settled the
// range tracker accordingly. Caller holds b.mu.
func (b *Buffer) dropBlockLocked(blk *block) {
	delete(b.blocks, blk.off)
	b.bytesInRAM -= int64(blockSize)
	b.pool.dropBytes(int64(blockSize))
	b.alloc.put(blk.bufPtr)
}

// -----------------------------------------------------------------------
// Disk-mode fast-path state
// -----------------------------------------------------------------------

func alignDown(off int64) int64 { return off &^ (blockSize - 1) }

// stateSlot returns the atomic state slot for the block containing off, or
// nil if the buffer was constructed without a TotalSize (so no slot was
// allocated). Safe for concurrent use; readers may load lock-free.
func (b *Buffer) stateSlot(blockOff int64) *atomic.Uint32 {
	if b.states == nil {
		return nil
	}
	idx := blockOff >> blockSizeLog2
	if idx < 0 || int(idx) >= len(b.states) {
		return nil
	}
	return &b.states[idx]
}

// markStateForBlockLocked recomputes the fast-path state for blockOff after
// a write or discard: stateFastDisk when the block is fully covered by
// ranges, stateSlow otherwise. Caller holds b.mu.
func (b *Buffer) markStateForBlockLocked(blockOff int64) {
	slot := b.stateSlot(blockOff)
	if slot == nil {
		return
	}
	if b.ranges.present(blockOff, blockSize) {
		slot.Store(stateFastDisk)
	} else {
		slot.Store(stateSlow)
	}
}
