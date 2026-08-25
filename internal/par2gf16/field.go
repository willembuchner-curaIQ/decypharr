package par2gf16

const multiplicativeOrder = 1<<16 - 1

var (
	logarithm [1 << 16]uint16
	exponent  [2 * multiplicativeOrder]Element
)

type Element uint16

func init() {
	x := uint32(1)
	for power := range multiplicativeOrder {
		element := Element(x)
		exponent[power] = element
		logarithm[element] = uint16(power)
		x <<= 1
		if x&(1<<16) != 0 {
			x ^= 0x1100b
		}
	}
	copy(exponent[multiplicativeOrder:], exponent[:multiplicativeOrder])
}

func (x Element) Mul(y Element) Element {
	if x == 0 || y == 0 {
		return 0
	}
	return exponent[int(logarithm[x])+int(logarithm[y])]
}

func (x Element) Inv() Element {
	if x == 0 {
		panic("par2gf16: inverse of zero")
	}
	return exponent[multiplicativeOrder-int(logarithm[x])]
}

func (x Element) Pow(power uint32) Element {
	if x == 0 {
		if power == 0 {
			return 1
		}
		return 0
	}
	index := uint64(logarithm[x]) * uint64(power) % multiplicativeOrder
	return exponent[index]
}
