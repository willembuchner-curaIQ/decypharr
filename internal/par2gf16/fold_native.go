//go:build cgo && ((darwin && (amd64 || arm64)) || (linux && (amd64 || arm64)) || (windows && amd64))

package par2gf16

import (
	"sync"

	"github.com/sirrobot01/decypharr/internal/par2gf16/native"
)

const nativeRangeSize = 128 << 10

type nativeFoldBackend struct {
	matrix       Matrix
	sliceSize    int
	workerCount  int
	outputSize   int
	batchSize    int
	bufferSize   int
	rangeSize    int
	rangeCount   int
	layout       *native.Context
	workers      []*native.Context
	coefficients []uint16
	accumulators []byte
	banks        [2][]byte
	raw          []byte
	jobs         chan nativeFoldJob
	completed    chan struct{}
	reads        chan nativeRead
	readResults  chan nativeReadResult
	wait         sync.WaitGroup
}

type nativeFoldJob struct {
	destination  []byte
	sources      []byte
	coefficients []uint16
	offset       int
	length       int
}

type nativeRead struct {
	next  Input
	start int
	count int
	size  int
	bank  []byte
}

type nativeReadResult struct {
	count int
	err   error
}

func newFoldBackend(matrix Matrix, sliceSize, workers int) (foldBackend, error) {
	if err := validateFold(matrix, sliceSize, workers); err != nil {
		return nil, err
	}
	layout, err := native.NewContext(sliceSize)
	if err != nil {
		return nil, err
	}
	workers = min(workers, matrix.rows*((layout.BufferSize()+nativeRangeSize-1)/nativeRangeSize))
	contexts := make([]*native.Context, workers)
	for index := range contexts {
		contexts[index], err = native.NewContext(sliceSize)
		if err != nil {
			for _, context := range contexts[:index] {
				context.Close()
			}
			layout.Close()
			return nil, err
		}
	}

	batchSize := min(max(layout.BatchSize(), 1), 64)
	rangeSize := nativeRangeSize - nativeRangeSize%layout.Stride()
	if rangeSize < layout.Stride() {
		rangeSize = layout.Stride()
	}
	if rangeSize > layout.BufferSize() {
		rangeSize = layout.BufferSize()
	}
	rangeCount := (layout.BufferSize() + rangeSize - 1) / rangeSize
	jobCount := matrix.rows * rangeCount
	backend := &nativeFoldBackend{
		matrix:       matrix,
		sliceSize:    sliceSize,
		workerCount:  workers,
		batchSize:    batchSize,
		bufferSize:   layout.BufferSize(),
		rangeSize:    rangeSize,
		rangeCount:   rangeCount,
		layout:       layout,
		workers:      contexts,
		coefficients: make([]uint16, len(matrix.values)),
		accumulators: layout.NewBuffers(matrix.rows),
		raw:          make([]byte, sliceSize),
		jobs:         make(chan nativeFoldJob, jobCount),
		completed:    make(chan struct{}, jobCount),
		reads:        make(chan nativeRead),
		readResults:  make(chan nativeReadResult, 1),
	}
	for index, value := range matrix.values {
		backend.coefficients[index] = uint16(value)
	}
	storage := layout.NewBuffers(2 * batchSize)
	bankSize := batchSize * backend.bufferSize
	backend.banks[0] = storage[:bankSize:bankSize]
	backend.banks[1] = storage[bankSize:]
	backend.wait.Add(workers + 1)
	for _, context := range contexts {
		go backend.work(context)
	}
	go backend.read()
	return backend, nil
}

func (b *nativeFoldBackend) Fold(size int, next Input) error {
	b.outputSize = size
	clear(b.accumulators)
	firstCount := min(b.batchSize, b.matrix.columns)
	b.reads <- nativeRead{next: next, count: firstCount, size: size, bank: b.banks[0]}
	result := <-b.readResults
	if result.err != nil {
		return result.err
	}

	bank := 0
	for start := 0; start < b.matrix.columns; {
		count := result.count
		nextStart := start + count
		if nextStart < b.matrix.columns {
			nextCount := min(b.batchSize, b.matrix.columns-nextStart)
			b.reads <- nativeRead{next: next, start: nextStart, count: nextCount, size: size, bank: b.banks[bank^1]}
		}

		jobs := 0
		for interval := range b.rangeCount {
			offset := interval * b.rangeSize
			length := min(b.rangeSize, b.bufferSize-offset)
			for row := range b.matrix.rows {
				coefficientStart := row*b.matrix.columns + start
				b.jobs <- nativeFoldJob{
					destination:  b.layout.Buffer(b.accumulators, row),
					sources:      b.banks[bank],
					coefficients: b.coefficients[coefficientStart : coefficientStart+count],
					offset:       offset,
					length:       length,
				}
				jobs++
			}
		}
		for range jobs {
			<-b.completed
		}
		if nextStart < b.matrix.columns {
			result = <-b.readResults
			if result.err != nil {
				return result.err
			}
			bank ^= 1
		}
		start = nextStart
	}

	for row := range b.matrix.rows {
		b.jobs <- nativeFoldJob{destination: b.layout.Buffer(b.accumulators, row)}
	}
	for range b.matrix.rows {
		<-b.completed
	}
	return nil
}

func (b *nativeFoldBackend) Output(row int) []byte {
	if row < 0 || row >= b.matrix.rows {
		panic("par2gf16: fold output row out of bounds")
	}
	return b.layout.Buffer(b.accumulators, row)[:b.outputSize:b.outputSize]
}

func (b *nativeFoldBackend) Method() string {
	return b.layout.Method()
}

func (b *nativeFoldBackend) Accelerated() bool {
	return true
}

func (b *nativeFoldBackend) Close() {
	close(b.reads)
	close(b.jobs)
	b.wait.Wait()
	for _, context := range b.workers {
		context.Close()
	}
	b.layout.Close()
}

func (b *nativeFoldBackend) read() {
	defer b.wait.Done()
	for request := range b.reads {
		result := nativeReadResult{count: request.count}
		for index := range request.count {
			clear(b.raw)
			if err := request.next(request.start+index, b.raw[:request.size]); err != nil {
				result.err = err
				break
			}
			b.layout.Prepare(b.layout.Buffer(request.bank, index), b.raw[:request.size])
		}
		b.readResults <- result
	}
}

func (b *nativeFoldBackend) work(context *native.Context) {
	defer b.wait.Done()
	for job := range b.jobs {
		if job.coefficients == nil {
			context.Finish(job.destination, job.destination[:b.sliceSize])
		} else {
			context.MulAddMulti(job.destination, job.sources, b.bufferSize,
				job.offset, job.length, job.coefficients)
		}
		b.completed <- struct{}{}
	}
}
