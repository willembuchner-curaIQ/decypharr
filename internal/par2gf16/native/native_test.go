//go:build cgo && ((darwin && (amd64 || arm64)) || (linux && (amd64 || arm64)) || (windows && amd64))

package native

import (
	"bytes"
	"math/rand"
	"testing"
	"unsafe"
)

func TestContextValidation(t *testing.T) {
	for _, size := range []int{-2, 0, 1, 257} {
		if context, err := NewContext(size); err == nil {
			context.Close()
			t.Fatalf("NewContext(%d) succeeded", size)
		}
	}
	context, err := NewContext(258)
	if err != nil {
		t.Fatal(err)
	}
	var nilContext *Context
	nilContext.Close()
	defer context.Close()

	expectPanic(t, func() { context.NewBuffers(0) })
	storage := context.NewBuffers(2)
	expectPanic(t, func() { context.Buffer(storage, -1) })
	expectPanic(t, func() { context.Buffer(storage, 2) })

	destination := context.Buffer(storage, 0)
	source := context.Buffer(storage, 1)
	expectPanic(t, func() { context.Prepare(destination[:len(destination)-1], []byte{0, 0}) })
	expectPanic(t, func() { context.Prepare(destination, nil) })
	expectPanic(t, func() { context.Finish(destination[:len(destination)-1], make([]byte, 2)) })
	expectPanic(t, func() { context.Finish(destination, nil) })

	context.MulAddMulti(nil, nil, 0, 0, 0, nil)
	context.MulAddMulti(destination, source, context.BufferSize(), 0, 0, []uint16{1})
	expectPanic(t, func() {
		context.MulAddMulti(destination[:len(destination)-1], source, context.BufferSize(), 0, context.Stride(), []uint16{1})
	})
	expectPanic(t, func() {
		context.MulAddMulti(destination, source, context.BufferSize()-1, 0, context.Stride(), []uint16{1})
	})
	expectPanic(t, func() {
		context.MulAddMulti(destination, source, context.BufferSize(), 0, context.Stride(), []uint16{1, 1})
	})
	expectPanic(t, func() {
		context.MulAddMulti(destination, source, context.BufferSize(), 1, context.Stride(), []uint16{1})
	})
	expectPanic(t, func() {
		context.MulAddMulti(destination, source, context.BufferSize(), context.BufferSize(), context.Stride(), []uint16{1})
	})
	manySources := context.NewBuffers(65)
	expectPanic(t, func() {
		context.MulAddMulti(destination, manySources, context.BufferSize(), 0, context.Stride(), make([]uint16, 65))
	})

	context.Close()
	context.Close()
}

func TestContextBuffersAndIdentityMultiply(t *testing.T) {
	const sliceSize = 65538
	context, err := NewContext(sliceSize)
	if err != nil {
		t.Fatal(err)
	}
	defer context.Close()
	if context.Method() == "" || context.BufferSize() < sliceSize || context.Stride() <= 0 || context.BatchSize() <= 0 {
		t.Fatalf("invalid context: method=%q buffer=%d stride=%d batch=%d",
			context.Method(), context.BufferSize(), context.Stride(), context.BatchSize())
	}
	t.Logf("method=%s buffer=%d stride=%d batch=%d needs_prepare=%t",
		context.Method(), context.BufferSize(), context.Stride(), context.BatchSize(), context.NeedsPrepare())

	raw := make([]byte, sliceSize)
	if _, err := rand.New(rand.NewSource(17)).Read(raw); err != nil {
		t.Fatal(err)
	}
	sources := context.NewBuffers(2)
	for index := range 2 {
		if uintptr(unsafe.Pointer(&context.Buffer(sources, index)[0]))%uintptr(context.alignment) != 0 {
			t.Fatalf("buffer %d is not %d-byte aligned", index, context.alignment)
		}
	}
	context.Prepare(context.Buffer(sources, 0), raw)
	context.Prepare(context.Buffer(sources, 1), raw)
	destination := context.NewBuffers(1)
	zero := make([]byte, sliceSize)
	context.Prepare(destination, zero)
	context.MulAddMulti(destination, sources, context.BufferSize(), 0, context.BufferSize(), []uint16{1, 1})
	got := make([]byte, sliceSize)
	context.Finish(destination, got)
	for index, value := range got {
		if value != 0 {
			t.Fatalf("double identity multiply byte %d = %02x", index, value)
		}
	}

	context.Prepare(destination, zero)
	context.MulAddMulti(destination, sources, context.BufferSize(), 0, context.BufferSize(), []uint16{1})
	context.Finish(destination, got)
	for index := range raw {
		if got[index] != raw[index] {
			t.Fatalf("identity multiply byte %d = %02x, want %02x", index, got[index], raw[index])
		}
	}
}

func TestContextArbitraryCoefficients(t *testing.T) {
	const (
		sliceSize = 131074
		count     = 16
	)
	context, err := NewContext(sliceSize)
	if err != nil {
		t.Fatal(err)
	}
	defer context.Close()

	rng := rand.New(rand.NewSource(91))
	raw := make([][]byte, count)
	coefficients := make([]uint16, count)
	prepared := context.NewBuffers(count)
	for index := range count {
		raw[index] = make([]byte, sliceSize)
		if _, err := rng.Read(raw[index]); err != nil {
			t.Fatal(err)
		}
		coefficients[index] = uint16(rng.Uint32())
		context.Prepare(context.Buffer(prepared, index), raw[index])
	}

	destination := context.NewBuffers(1)
	context.Prepare(destination, make([]byte, sliceSize))
	context.MulAddMulti(destination, prepared, context.BufferSize(), 0,
		context.BufferSize(), coefficients)
	got := make([]byte, sliceSize)
	context.Finish(destination, got)

	want := make([]byte, sliceSize)
	for offset := 0; offset < sliceSize; offset += 2 {
		var symbol uint16
		for input := range count {
			value := uint16(raw[input][offset]) | uint16(raw[input][offset+1])<<8
			symbol ^= referenceProduct(value, coefficients[input])
		}
		want[offset] = byte(symbol)
		want[offset+1] = byte(symbol >> 8)
	}
	if !bytes.Equal(got, want) {
		for index := range got {
			if got[index] != want[index] {
				t.Fatalf("byte %d = %02x, want %02x using %s", index, got[index], want[index], context.Method())
			}
		}
	}
}

func referenceProduct(left, right uint16) uint16 {
	product := uint32(0)
	x := uint32(left)
	y := uint32(right)
	for y != 0 {
		if y&1 != 0 {
			product ^= x
		}
		y >>= 1
		x <<= 1
		if x&(1<<16) != 0 {
			x ^= 0x1100b
		}
	}
	return uint16(product)
}

func expectPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("call did not panic")
		}
	}()
	fn()
}
