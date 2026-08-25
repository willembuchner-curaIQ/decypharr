package par2gf16

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
)

func TestFolderAgainstPolynomialArithmetic(t *testing.T) {
	matrix := NewMatrix(7, 19, func(row, column int) Element {
		return Element((row+1)*7919 + column*3571)
	})
	testFolder(t, matrix, 131074, func() (foldBackend, error) {
		folder, err := NewFolder(matrix, 131074, 4)
		if err != nil {
			return nil, err
		}
		return folder.backend, nil
	})
}

func TestPureFolderAgainstPolynomialArithmetic(t *testing.T) {
	matrix := NewMatrix(5, 11, func(row, column int) Element {
		if row == column {
			return 1
		}
		return Element((row+3)*1237 + column*541)
	})
	testFolder(t, matrix, 8194, func() (foldBackend, error) {
		return newPureBackend(matrix, 8194, 3)
	})
}

func TestFolderPropagatesReadErrorAndCanBeReused(t *testing.T) {
	matrix := NewMatrix(2, 5, func(row, column int) Element {
		return Element(row*5 + column + 1)
	})
	folder, err := NewFolder(matrix, 4098, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer folder.Close()

	errRead := errors.New("read failed")
	if err := folder.Fold(func(index int, buffer []byte) error {
		if index == 3 {
			return errRead
		}
		return nil
	}); !errors.Is(err, errRead) {
		t.Fatalf("Fold error = %v, want %v", err, errRead)
	}
	if err := folder.FoldSize(2050, func(index int, buffer []byte) error {
		for offset := range buffer {
			buffer[offset] = byte(index + offset)
		}
		return nil
	}); err != nil {
		t.Fatalf("reused Fold: %v", err)
	}
}

func TestFolderValidation(t *testing.T) {
	matrix := NewMatrix(1, 1, func(int, int) Element { return 1 })
	for _, test := range []struct {
		size    int
		workers int
	}{
		{size: 0, workers: 1},
		{size: 3, workers: 1},
		{size: 2, workers: 0},
	} {
		if _, err := NewFolder(matrix, test.size, test.workers); err == nil {
			t.Fatalf("NewFolder(size=%d, workers=%d) succeeded", test.size, test.workers)
		}
	}
	if _, err := NewFolder(Matrix{}, 2, 1); err == nil {
		t.Fatal("NewFolder accepted an empty matrix")
	}
}

func testFolder(t *testing.T, matrix Matrix, sliceSize int, construct func() (foldBackend, error)) {
	t.Helper()
	backend, err := construct()
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	t.Logf("backend=%s accelerated=%t", backend.Method(), backend.Accelerated())

	for iteration := range 2 {
		rng := rand.New(rand.NewSource(int64(101 + iteration)))
		inputs := make([][]byte, matrix.columns)
		for column := range inputs {
			inputs[column] = make([]byte, sliceSize)
			if _, err := rng.Read(inputs[column]); err != nil {
				t.Fatal(err)
			}
		}
		nextIndex := 0
		err := backend.Fold(sliceSize, func(index int, buffer []byte) error {
			if index != nextIndex {
				return errors.New("input callback order changed")
			}
			nextIndex++
			copy(buffer, inputs[index])
			return nil
		})
		if err != nil {
			t.Fatalf("iteration %d: %v", iteration, err)
		}
		if nextIndex != matrix.columns {
			t.Fatalf("read %d inputs, want %d", nextIndex, matrix.columns)
		}
		for row := range matrix.rows {
			want := referenceFold(matrix.row(row), inputs)
			if got := backend.Output(row); !bytes.Equal(got, want) {
				for index := range want {
					if got[index] != want[index] {
						t.Fatalf("iteration %d row %d byte %d = %02x, want %02x", iteration, row, index, got[index], want[index])
					}
				}
			}
		}
	}
}

func referenceFold(coefficients []Element, inputs [][]byte) []byte {
	output := make([]byte, len(inputs[0]))
	for offset := 0; offset < len(output); offset += 2 {
		var value Element
		for input, coefficient := range coefficients {
			symbol := Element(inputs[input][offset]) | Element(inputs[input][offset+1])<<8
			value ^= referenceProduct(symbol, coefficient)
		}
		output[offset] = byte(value)
		output[offset+1] = byte(value >> 8)
	}
	return output
}
