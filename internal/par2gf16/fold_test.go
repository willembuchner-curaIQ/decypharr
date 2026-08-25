package par2gf16

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
)

func TestFolderLifecycle(t *testing.T) {
	var nilFolder *Folder
	if !errors.Is(nilFolder.Fold(func(int, []byte) error { return nil }), ErrInvalidFold) {
		t.Fatal("nil Folder.Fold did not return ErrInvalidFold")
	}
	if !errors.Is(nilFolder.FoldSize(2, func(int, []byte) error { return nil }), ErrInvalidFold) {
		t.Fatal("nil Folder.FoldSize did not return ErrInvalidFold")
	}
	if nilFolder.Method() != "" || nilFolder.Accelerated() {
		t.Fatal("nil folder reported a backend")
	}
	nilFolder.Close()
	expectPanic(t, func() { nilFolder.Output(0) })

	workers := DefaultWorkers()
	if workers < 1 || workers > runtime.GOMAXPROCS(0) {
		t.Fatalf("DefaultWorkers() = %d, GOMAXPROCS = %d", workers, runtime.GOMAXPROCS(0))
	}
	matrix := NewMatrix(1, 1, func(int, int) Element { return 1 })
	folder, err := NewFolder(matrix, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	if folder.Method() == "" {
		t.Fatal("folder method is empty")
	}
	source := make([]byte, 64)
	for index := range source {
		source[index] = byte(index)
	}
	if err := folder.Fold(func(_ int, buffer []byte) error {
		copy(buffer, source)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(folder.Output(0), source) {
		t.Fatal("identity fold changed source")
	}
	folder.Close()
	folder.Close()
	if !errors.Is(folder.Fold(func(int, []byte) error { return nil }), ErrInvalidFold) {
		t.Fatal("closed Folder.Fold did not return ErrInvalidFold")
	}
	if folder.Method() != "" || folder.Accelerated() {
		t.Fatal("closed folder reported a backend")
	}
	expectPanic(t, func() { folder.Output(0) })
}

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
	inputs := make([][]byte, matrix.Columns())
	for index := range inputs {
		inputs[index] = make([]byte, 2050)
		for offset := range inputs[index] {
			inputs[index][offset] = byte(index + offset)
		}
	}
	if err := folder.FoldSize(2050, func(index int, buffer []byte) error {
		if len(buffer) != 2050 {
			return fmt.Errorf("input buffer length = %d, want 2050", len(buffer))
		}
		copy(buffer, inputs[index])
		return nil
	}); err != nil {
		t.Fatalf("reused Fold: %v", err)
	}
	for row := range matrix.Rows() {
		got := folder.Output(row)
		if len(got) != 2050 {
			t.Fatalf("output %d length = %d, want 2050", row, len(got))
		}
		if want := referenceFold(matrix.row(row), inputs); !bytes.Equal(got, want) {
			t.Fatalf("output %d mismatch after partial-size reuse", row)
		}
	}
}

func TestFolderFoldSizeValidation(t *testing.T) {
	matrix := NewMatrix(1, 1, func(int, int) Element { return 1 })
	folder, err := NewFolder(matrix, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer folder.Close()

	for _, size := range []int{-2, 0, 1, 63, 66} {
		if err := folder.FoldSize(size, func(int, []byte) error { return nil }); err == nil {
			t.Fatalf("FoldSize(%d) succeeded", size)
		}
	}
	if err := folder.FoldSize(2, nil); err == nil {
		t.Fatal("FoldSize accepted a nil input callback")
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

func expectPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("call did not panic")
		}
	}()
	fn()
}
