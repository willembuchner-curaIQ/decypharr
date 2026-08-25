package par2gf16

import (
	"errors"
	"testing"
)

func TestVandermondeInverse(t *testing.T) {
	for size := 1; size <= 32; size++ {
		matrix := NewMatrix(size, size, func(row, column int) Element {
			return Element(column + 1).Pow(uint32(row))
		})
		inverse, err := matrix.Inverse()
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		assertProductIsIdentity(t, matrix, inverse)
		assertProductIsIdentity(t, inverse, matrix)
	}
}

func TestInversePivotsAndRejectsSingularMatrix(t *testing.T) {
	pivot := NewMatrix(3, 3, func(row, column int) Element {
		values := [...]Element{0, 1, 0, 0, 0, 1, 1, 0, 0}
		return values[row*3+column]
	})
	inverse, err := pivot.Inverse()
	if err != nil {
		t.Fatal(err)
	}
	assertProductIsIdentity(t, pivot, inverse)

	singular := NewMatrix(3, 3, func(row, column int) Element {
		values := [...]Element{1, 2, 3, 4, 5, 6, 1, 2, 3}
		return values[row*3+column]
	})
	if _, err := singular.Inverse(); !errors.Is(err, ErrSingular) {
		t.Fatalf("Inverse error = %v, want %v", err, ErrSingular)
	}
}

func TestInverseDoesNotMutateMatrix(t *testing.T) {
	matrix := NewMatrix(8, 8, func(row, column int) Element {
		return Element(column + 1).Pow(uint32(row))
	})
	before := append([]Element(nil), matrix.values...)
	if _, err := matrix.Inverse(); err != nil {
		t.Fatal(err)
	}
	for i := range before {
		if matrix.values[i] != before[i] {
			t.Fatalf("matrix value %d changed from %04x to %04x", i, before[i], matrix.values[i])
		}
	}
}

func assertProductIsIdentity(t *testing.T, left, right Matrix) {
	t.Helper()
	if left.columns != right.rows {
		t.Fatalf("incompatible matrices %dx%d and %dx%d", left.rows, left.columns, right.rows, right.columns)
	}
	for row := range left.rows {
		for column := range right.columns {
			var value Element
			for inner := range left.columns {
				value ^= left.At(row, inner).Mul(right.At(inner, column))
			}
			want := Element(0)
			if row == column {
				want = 1
			}
			if value != want {
				t.Fatalf("product[%d,%d] = %04x, want %04x", row, column, value, want)
			}
		}
	}
}
