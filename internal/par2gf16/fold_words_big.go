//go:build mips || mips64 || ppc64 || s390x

package par2gf16

func mulAddBytes(destination, source []byte, table *byteMultiplicationTable) {
	for index := 0; index < len(source); index += 2 {
		value := table.low[source[index]] ^ table.high[source[index+1]]
		destination[index] ^= byte(value)
		destination[index+1] ^= byte(value >> 8)
	}
}
