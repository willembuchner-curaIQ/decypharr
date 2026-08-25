//go:build !cgo || (!darwin && !linux && !windows) || (darwin && !amd64 && !arm64) || (linux && !amd64 && !arm64) || (windows && !amd64)

package par2gf16

func newFoldBackend(matrix Matrix, sliceSize, workers int) (foldBackend, error) {
	return newPureBackend(matrix, sliceSize, workers)
}
