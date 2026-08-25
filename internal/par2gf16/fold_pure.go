package par2gf16

import "sync"

type byteMultiplicationTable struct {
	low  [256]Element
	high [256]Element
}

type pureFoldBackend struct {
	matrix      Matrix
	sliceSize   int
	workerCount int
	outputSize  int
	outputs     []byte
	inputs      [2][]byte
	tables      []*byteMultiplicationTable
	jobs        chan pureFoldJob
	completed   chan struct{}
	reads       chan pureRead
	readResults chan error
	wait        sync.WaitGroup
}

type pureFoldJob struct {
	column int
	source []byte
	worker int
}

type pureRead struct {
	next   Input
	index  int
	buffer []byte
}

func newPureBackend(matrix Matrix, sliceSize, workers int) (*pureFoldBackend, error) {
	if err := validateFold(matrix, sliceSize, workers); err != nil {
		return nil, err
	}
	workers = min(workers, matrix.rows)
	backend := &pureFoldBackend{
		matrix:      matrix,
		sliceSize:   sliceSize,
		workerCount: workers,
		outputs:     make([]byte, matrix.rows*sliceSize),
		tables:      multiplicationTables(matrix),
		jobs:        make(chan pureFoldJob, workers),
		completed:   make(chan struct{}, workers),
		reads:       make(chan pureRead),
		readResults: make(chan error, 1),
	}
	backend.inputs[0] = make([]byte, sliceSize)
	backend.inputs[1] = make([]byte, sliceSize)
	backend.wait.Add(workers + 1)
	for range workers {
		go backend.work()
	}
	go backend.read()
	return backend, nil
}

func multiplicationTables(matrix Matrix) []*byteMultiplicationTable {
	tables := make([]*byteMultiplicationTable, len(matrix.values))
	unique := make(map[Element]*byteMultiplicationTable)
	for index, coefficient := range matrix.values {
		if coefficient == 0 || coefficient == 1 {
			continue
		}
		table := unique[coefficient]
		if table == nil {
			table = &byteMultiplicationTable{}
			for value := range 256 {
				table.low[value] = coefficient.Mul(Element(value))
				table.high[value] = coefficient.Mul(Element(value << 8))
			}
			unique[coefficient] = table
		}
		tables[index] = table
	}
	return tables
}

func (b *pureFoldBackend) Fold(size int, next Input) error {
	b.outputSize = size
	for row := range b.matrix.rows {
		start := row * b.sliceSize
		clear(b.outputs[start : start+size])
	}
	b.reads <- pureRead{next: next, buffer: b.inputs[0][:size]}
	if err := <-b.readResults; err != nil {
		return err
	}

	for column := range b.matrix.columns {
		bank := column & 1
		if column+1 < b.matrix.columns {
			b.reads <- pureRead{next: next, index: column + 1, buffer: b.inputs[bank^1][:size]}
		}
		for worker := range b.workerCount {
			b.jobs <- pureFoldJob{column: column, source: b.inputs[bank][:size], worker: worker}
		}
		for range b.workerCount {
			<-b.completed
		}
		if column+1 < b.matrix.columns {
			if err := <-b.readResults; err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *pureFoldBackend) Output(row int) []byte {
	if row < 0 || row >= b.matrix.rows {
		panic("par2gf16: fold output row out of bounds")
	}
	start := row * b.sliceSize
	return b.outputs[start : start+b.outputSize : start+b.outputSize]
}

func (b *pureFoldBackend) Method() string {
	return "pure Go lookup"
}

func (b *pureFoldBackend) Accelerated() bool {
	return false
}

func (b *pureFoldBackend) Close() {
	close(b.reads)
	close(b.jobs)
	b.wait.Wait()
}

func (b *pureFoldBackend) read() {
	defer b.wait.Done()
	for read := range b.reads {
		b.readResults <- read.next(read.index, read.buffer)
	}
}

func (b *pureFoldBackend) work() {
	defer b.wait.Done()
	for job := range b.jobs {
		for row := job.worker; row < b.matrix.rows; row += b.workerCount {
			index := row*b.matrix.columns + job.column
			coefficient := b.matrix.values[index]
			destination := b.Output(row)
			switch coefficient {
			case 0:
			case 1:
				xorBytes(destination, job.source)
			default:
				mulAddBytes(destination, job.source, b.tables[index])
			}
		}
		b.completed <- struct{}{}
	}
}

func xorBytes(destination, source []byte) {
	for index, value := range source {
		destination[index] ^= value
	}
}
