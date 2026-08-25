//go:build 386 || amd64 || arm || arm64 || loong64 || mips64le || mipsle || ppc64le || riscv64 || wasm

package par2gf16

import "unsafe"

func mulAddBytes(destination, source []byte, table *byteMultiplicationTable) {
	input := unsafe.Slice((*Element)(unsafe.Pointer(unsafe.SliceData(source))), len(source)/2)
	output := unsafe.Slice((*Element)(unsafe.Pointer(unsafe.SliceData(destination))), len(destination)/2)
	for index, symbol := range input {
		output[index] ^= table.low[byte(symbol)] ^ table.high[byte(symbol>>8)]
	}
}
