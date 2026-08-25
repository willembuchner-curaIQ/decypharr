package par2test

func Recovery(data [][]byte, exponent uint16) []byte {
	return generate(data, []uint16{exponent})[0]
}

func RecoverySet(data [][]byte, count int) [][]byte {
	if count <= 0 || count > 1<<16-1 {
		panic("par2test: invalid recovery count")
	}
	exponents := make([]uint16, count)
	for index := range exponents {
		exponents[index] = uint16(index)
	}
	return generate(data, exponents)
}

func generate(data [][]byte, exponents []uint16) [][]byte {
	if len(data) == 0 || len(data) > 32768 || len(data[0]) == 0 || len(data[0])%2 != 0 {
		panic("par2test: invalid data shards")
	}
	sliceSize := len(data[0])
	for _, shard := range data {
		if len(shard) != sliceSize {
			panic("par2test: unequal data shards")
		}
	}

	generators := make([]uint16, 0, len(data))
	for power := 0; len(generators) < len(data); power++ {
		if power%3 == 0 || power%5 == 0 || power%17 == 0 || power%257 == 0 {
			continue
		}
		generators = append(generators, fieldPower(2, uint32(power)))
	}
	coefficients := make([][]uint16, len(exponents))
	output := make([][]byte, len(exponents))
	for row, exponent := range exponents {
		coefficients[row] = make([]uint16, len(data))
		output[row] = make([]byte, sliceSize)
		for column, generator := range generators {
			coefficients[row][column] = fieldPower(generator, uint32(exponent))
		}
	}

	for column, shard := range data {
		for offset := 0; offset < sliceSize; offset += 2 {
			symbol := uint16(shard[offset]) | uint16(shard[offset+1])<<8
			for row := range output {
				value := fieldProduct(symbol, coefficients[row][column])
				output[row][offset] ^= byte(value)
				output[row][offset+1] ^= byte(value >> 8)
			}
		}
	}
	return output
}

func fieldPower(value uint16, power uint32) uint16 {
	result := uint16(1)
	for power != 0 {
		if power&1 != 0 {
			result = fieldProduct(result, value)
		}
		value = fieldProduct(value, value)
		power >>= 1
	}
	return result
}

func fieldProduct(left, right uint16) uint16 {
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
