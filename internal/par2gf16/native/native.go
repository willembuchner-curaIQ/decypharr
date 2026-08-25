//go:build cgo && ((darwin && (amd64 || arm64)) || (linux && (amd64 || arm64)) || (windows && amd64))

package native

/*
#cgo darwin LDFLAGS: ${SRCDIR}/libgf16_darwin.a -lc++ -lm
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/libgf16_linux_amd64.a -lstdc++ -lm
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/libgf16_linux_arm64.a -lstdc++ -lm
#cgo windows,amd64 LDFLAGS: ${SRCDIR}/libgf16_windows_amd64.a -lstdc++ -lm
#include "bridge.h"
*/
import "C"

import (
	"errors"
	"runtime"
	"unsafe"
)

var ErrInitialization = errors.New("par2gf16: native backend initialization failed")

type Context struct {
	pointer      *C.goblack_gf16_context
	sliceSize    int
	bufferSize   int
	alignment    int
	stride       int
	batchSize    int
	needsPrepare bool
}

func NewContext(sliceSize int) (*Context, error) {
	if sliceSize <= 0 || sliceSize%2 != 0 {
		return nil, errors.New("par2gf16: native slice size must be positive and even")
	}
	pointer := C.goblack_gf16_create(C.size_t(sliceSize))
	if pointer == nil {
		return nil, ErrInitialization
	}
	context := &Context{
		pointer:      pointer,
		sliceSize:    sliceSize,
		bufferSize:   int(C.goblack_gf16_buffer_size(pointer)),
		alignment:    int(C.goblack_gf16_alignment(pointer)),
		stride:       int(C.goblack_gf16_stride(pointer)),
		batchSize:    int(C.goblack_gf16_batch_size(pointer)),
		needsPrepare: C.goblack_gf16_needs_prepare(pointer) != 0,
	}
	runtime.SetFinalizer(context, (*Context).Close)
	return context, nil
}

func (c *Context) Method() string {
	return C.GoString(C.goblack_gf16_method(c.pointer))
}

func (c *Context) BufferSize() int {
	return c.bufferSize
}

func (c *Context) Stride() int {
	return c.stride
}

func (c *Context) BatchSize() int {
	return c.batchSize
}

func (c *Context) NeedsPrepare() bool {
	return c.needsPrepare
}

func (c *Context) NewBuffers(count int) []byte {
	if count <= 0 || count > int(^uint(0)>>1)/c.bufferSize {
		panic("par2gf16: invalid native buffer count")
	}
	size := c.bufferSize * count
	storage := make([]byte, size+c.alignment-1)
	offset := int(uintptr(unsafe.Pointer(&storage[0])) & uintptr(c.alignment-1))
	if offset != 0 {
		offset = c.alignment - offset
	}
	return storage[offset : offset+size : offset+size]
}

func (c *Context) Buffer(storage []byte, index int) []byte {
	if index < 0 || index >= len(storage)/c.bufferSize {
		panic("par2gf16: native buffer index out of bounds")
	}
	start := index * c.bufferSize
	return storage[start : start+c.bufferSize : start+c.bufferSize]
}

func (c *Context) Prepare(destination, source []byte) {
	if len(destination) != c.bufferSize || len(source) == 0 || len(source) > c.sliceSize {
		panic("par2gf16: native prepare buffer size mismatch")
	}
	C.goblack_gf16_prepare(c.pointer, unsafe.Pointer(&destination[0]),
		unsafe.Pointer(&source[0]), C.size_t(len(source)))
	runtime.KeepAlive(c)
}

func (c *Context) Finish(buffer, destination []byte) {
	if len(buffer) != c.bufferSize || len(destination) == 0 || len(destination) > c.sliceSize {
		panic("par2gf16: native finish buffer size mismatch")
	}
	C.goblack_gf16_finish(c.pointer, unsafe.Pointer(&buffer[0]))
	copy(destination, buffer)
	runtime.KeepAlive(c)
}

func (c *Context) MulAddMulti(destination, sources []byte, sourceStride, offset, length int, coefficients []uint16) {
	if len(coefficients) == 0 {
		return
	}
	if len(destination) != c.bufferSize || sourceStride < c.bufferSize ||
		len(coefficients) > len(sources)/sourceStride {
		panic("par2gf16: native multiply buffer size mismatch")
	}
	if offset < 0 || length < 0 || offset%c.stride != 0 || length%c.stride != 0 ||
		offset > c.bufferSize-length {
		panic("par2gf16: native multiply range is not stride aligned")
	}
	if len(coefficients) > 64 {
		panic("par2gf16: native multiply batch exceeds 64 inputs")
	}
	if length == 0 {
		return
	}
	ok := C.goblack_gf16_muladd_multi(c.pointer, C.unsigned(len(coefficients)),
		C.size_t(offset), unsafe.Pointer(&destination[0]), unsafe.Pointer(&sources[0]),
		C.size_t(sourceStride), C.size_t(length), (*C.uint16_t)(unsafe.Pointer(&coefficients[0])))
	if ok == 0 {
		panic("par2gf16: native multiply failed")
	}
	runtime.KeepAlive(c)
}

func (c *Context) Close() {
	if c == nil || c.pointer == nil {
		return
	}
	runtime.SetFinalizer(c, nil)
	C.goblack_gf16_destroy(c.pointer)
	c.pointer = nil
}
