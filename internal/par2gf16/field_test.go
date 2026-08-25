package par2gf16

import "testing"

func TestEveryElementAgainstPolynomialArithmetic(t *testing.T) {
	multipliers := [...]Element{0, 1, 2, 3, 0x100b, 0x8000, 0xffff}
	for value := range 1 << 16 {
		x := Element(value)
		for _, y := range multipliers {
			if got, want := x.Mul(y), referenceProduct(x, y); got != want {
				t.Fatalf("%04x * %04x = %04x, want %04x", x, y, got, want)
			}
		}
		if x == 0 {
			continue
		}
		if got := x.Mul(x.Inv()); got != 1 {
			t.Fatalf("%04x * inverse = %04x", x, got)
		}
	}
}

func TestExponentTableCoversNonzeroField(t *testing.T) {
	seen := make([]bool, 1<<16)
	x := Element(1)
	for power := range multiplicativeOrder {
		if x == 0 || seen[x] {
			t.Fatalf("generator repeated %04x at power %d", x, power)
		}
		seen[x] = true
		if got := Element(2).Pow(uint32(power)); got != x {
			t.Fatalf("2^%d = %04x, want %04x", power, got, x)
		}
		x = referenceProduct(x, 2)
	}
	if x != 1 {
		t.Fatalf("generator cycle ended at %04x", x)
	}
}

func TestPowerAgainstReference(t *testing.T) {
	powers := [...]uint32{0, 1, 2, 3, 17, 257, 65534, 65535, 65536, 1<<31 + 7}
	for value := range 1 << 16 {
		x := Element(value)
		for _, power := range powers {
			if got, want := x.Pow(power), referencePower(x, power); got != want {
				t.Fatalf("%04x^%d = %04x, want %04x", x, power, got, want)
			}
		}
	}
}

func TestInverseOfZeroPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("inverse of zero did not panic")
		}
	}()
	Element(0).Inv()
}

func FuzzElementArithmetic(f *testing.F) {
	for _, seed := range []struct {
		left, right uint16
		power       uint32
	}{
		{0, 0, 0},
		{1, 1, 1},
		{2, 0xffff, 65535},
		{0x100b, 0x8000, 1<<31 + 7},
	} {
		f.Add(seed.left, seed.right, seed.power)
	}
	f.Fuzz(func(t *testing.T, left, right uint16, power uint32) {
		x := Element(left)
		y := Element(right)
		if got, want := x.Mul(y), referenceProduct(x, y); got != want {
			t.Fatalf("%04x * %04x = %04x, want %04x", x, y, got, want)
		}
		if got, want := x.Pow(power), referencePower(x, power); got != want {
			t.Fatalf("%04x^%d = %04x, want %04x", x, power, got, want)
		}
		if x != 0 && x.Mul(x.Inv()) != 1 {
			t.Fatalf("%04x has an invalid inverse", x)
		}
	})
}

func referenceProduct(x, y Element) Element {
	product := uint32(0)
	a := uint32(x)
	b := uint32(y)
	for b != 0 {
		if b&1 != 0 {
			product ^= a
		}
		b >>= 1
		a <<= 1
		if a&(1<<16) != 0 {
			a ^= 0x1100b
		}
	}
	return Element(product)
}

func referencePower(x Element, power uint32) Element {
	result := Element(1)
	for power != 0 {
		if power&1 != 0 {
			result = referenceProduct(result, x)
		}
		x = referenceProduct(x, x)
		power >>= 1
	}
	return result
}
