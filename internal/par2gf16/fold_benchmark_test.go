package par2gf16

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
)

const (
	benchmarkInputs    = 256
	benchmarkSliceSize = 2380956
)

func BenchmarkFolder(b *testing.B) {
	for _, outputs := range []int{1, 4, 28} {
		b.Run(fmt.Sprintf("%d-outputs", outputs), func(b *testing.B) {
			inputs, matrix := benchmarkData(b, outputs)
			folder, err := NewFolder(matrix, benchmarkSliceSize, runtime.GOMAXPROCS(0))
			if err != nil {
				b.Fatal(err)
			}
			defer folder.Close()
			fill := func(index int, buffer []byte) error {
				copy(buffer, inputs[index])
				return nil
			}
			if err := folder.Fold(fill); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(benchmarkInputs * benchmarkSliceSize * outputs))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := folder.Fold(fill); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(int64(b.N)*benchmarkInputs*benchmarkSliceSize)/b.Elapsed().Seconds()/1e6, "input-MB/s")
			b.Logf("backend=%s", folder.Method())
		})
	}
}

func BenchmarkFolderCold(b *testing.B) {
	inputs, matrix := benchmarkData(b, 28)
	b.SetBytes(int64(benchmarkInputs * benchmarkSliceSize * 28))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		folder, err := NewFolder(matrix, benchmarkSliceSize, runtime.GOMAXPROCS(0))
		if err != nil {
			b.Fatal(err)
		}
		err = folder.Fold(func(index int, buffer []byte) error {
			copy(buffer, inputs[index])
			return nil
		})
		folder.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkData(b *testing.B, outputs int) ([][]byte, Matrix) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	inputs := make([][]byte, benchmarkInputs)
	for index := range inputs {
		inputs[index] = make([]byte, benchmarkSliceSize)
		if _, err := rng.Read(inputs[index]); err != nil {
			b.Fatal(err)
		}
	}
	matrix := NewMatrix(outputs, benchmarkInputs, func(int, int) Element {
		return Element(rng.Intn(65535) + 1)
	})
	return inputs, matrix
}
