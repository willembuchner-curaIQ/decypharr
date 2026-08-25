package par2gf16

import (
	"errors"
	"runtime"

	"github.com/klauspost/cpuid/v2"
)

var ErrInvalidFold = errors.New("par2gf16: invalid fold configuration")

type Input func(index int, buffer []byte) error

type Folder struct {
	sliceSize int
	backend   foldBackend
}

type foldBackend interface {
	Fold(int, Input) error
	Output(int) []byte
	Method() string
	Accelerated() bool
	Close()
}

func DefaultWorkers() int {
	workers := runtime.GOMAXPROCS(0)
	if physical := cpuid.CPU.PhysicalCores; physical > 0 && physical < workers {
		workers = physical
	}
	return max(workers, 1)
}

func NewFolder(matrix Matrix, sliceSize, workers int) (*Folder, error) {
	backend, err := newFoldBackend(matrix, sliceSize, workers)
	if err != nil {
		return nil, err
	}
	return &Folder{sliceSize: sliceSize, backend: backend}, nil
}

func (f *Folder) Fold(next Input) error {
	if f == nil {
		return ErrInvalidFold
	}
	return f.FoldSize(f.sliceSize, next)
}

func (f *Folder) FoldSize(size int, next Input) error {
	if f == nil || f.backend == nil {
		return ErrInvalidFold
	}
	if size <= 0 || size > f.sliceSize || size%2 != 0 {
		return errors.New("par2gf16: fold size must be positive, even, and within capacity")
	}
	if next == nil {
		return errors.New("par2gf16: nil fold input")
	}
	return f.backend.Fold(size, next)
}

func (f *Folder) Output(row int) []byte {
	if f == nil || f.backend == nil {
		panic("par2gf16: closed folder")
	}
	return f.backend.Output(row)
}

func (f *Folder) Method() string {
	if f == nil || f.backend == nil {
		return ""
	}
	return f.backend.Method()
}

func (f *Folder) Accelerated() bool {
	return f != nil && f.backend != nil && f.backend.Accelerated()
}

func (f *Folder) Close() {
	if f == nil || f.backend == nil {
		return
	}
	f.backend.Close()
	f.backend = nil
}

func validateFold(matrix Matrix, sliceSize, workers int) error {
	if matrix.rows <= 0 || matrix.columns <= 0 {
		return errors.New("par2gf16: fold matrix must not be empty")
	}
	if sliceSize <= 0 || sliceSize%2 != 0 {
		return errors.New("par2gf16: fold slice size must be positive and even")
	}
	if workers <= 0 {
		return errors.New("par2gf16: fold worker count must be positive")
	}
	return nil
}
