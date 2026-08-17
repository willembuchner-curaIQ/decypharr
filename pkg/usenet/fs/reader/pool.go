package reader

import (
	"sync"

	"github.com/sirrobot01/decypharr/internal/buffer"
	"github.com/sirrobot01/decypharr/internal/config"
)

// usenet owns its streaming-buffer pool here rather than the buffer package
// owning a "usenet" singleton — the buffer package stays generic. The pool is
// created once with the configured usenet RAM budget and shared across every
// SegmentCache. Disk is bounded per-stream by the sliding-window sweep
// (see SegmentCache.sweepWindow), so the pool runs no disk backstop.
var (
	bufPoolOnce sync.Once
	bufPool     *buffer.Pool
)

func usenetBufferPool() *buffer.Pool {
	bufPoolOnce.Do(func() {
		bufPool = buffer.NewPool(buffer.PoolConfig{
			Name:         "usenet",
			MemoryBudget: config.Get().Usenet.BufferMemoryBytes(),
		})
	})
	return bufPool
}

// poolMemoryPressured reports whether the shared pool is at ≥7/8 of its RAM
// budget — the signal for memory-mode sweeps to tighten their back-windows
// (see sweepWindow) so trailing history is given back before the buffers
// must drop live blocks. Always false with an unlimited budget.
func poolMemoryPressured() bool {
	st := usenetBufferPool().Stats()
	return st.MemoryBudget > 0 && st.MemoryInUse >= st.MemoryBudget-st.MemoryBudget/8
}
