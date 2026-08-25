package par2gf16

import "errors"

var ErrSingular = errors.New("par2gf16: singular matrix")

type Matrix struct {
	rows    int
	columns int
	values  []Element
}

func NewMatrix(rows, columns int, fill func(row, column int) Element) Matrix {
	if rows <= 0 || columns <= 0 {
		panic("par2gf16: matrix dimensions must be positive")
	}
	if fill == nil {
		panic("par2gf16: nil matrix initializer")
	}
	values := make([]Element, rows*columns)
	for row := range rows {
		for column := range columns {
			values[row*columns+column] = fill(row, column)
		}
	}
	return Matrix{rows: rows, columns: columns, values: values}
}

func (m Matrix) Rows() int {
	return m.rows
}

func (m Matrix) Columns() int {
	return m.columns
}

func (m Matrix) At(row, column int) Element {
	if row < 0 || row >= m.rows || column < 0 || column >= m.columns {
		panic("par2gf16: matrix index out of bounds")
	}
	return m.values[row*m.columns+column]
}

func (m Matrix) Inverse() (Matrix, error) {
	if m.rows != m.columns || m.rows == 0 {
		panic("par2gf16: inverse requires a non-empty square matrix")
	}

	size := m.rows
	left := append([]Element(nil), m.values...)
	right := make([]Element, size*size)
	for row := range size {
		right[row*size+row] = 1
	}

	for column := range size {
		pivot := column
		for pivot < size && left[pivot*size+column] == 0 {
			pivot++
		}
		if pivot == size {
			return Matrix{}, ErrSingular
		}
		if pivot != column {
			swapRows(left, size, pivot, column)
			swapRows(right, size, pivot, column)
		}

		inverse := left[column*size+column].Inv()
		scaleRow(left, size, column, inverse)
		scaleRow(right, size, column, inverse)
		for row := range size {
			if row == column {
				continue
			}
			factor := left[row*size+column]
			if factor == 0 {
				continue
			}
			addScaledRow(left, size, row, column, factor)
			addScaledRow(right, size, row, column, factor)
		}
	}

	return Matrix{rows: size, columns: size, values: right}, nil
}

func (m Matrix) row(index int) []Element {
	return m.values[index*m.columns : (index+1)*m.columns]
}

func swapRows(values []Element, columns, first, second int) {
	firstRow := values[first*columns : (first+1)*columns]
	secondRow := values[second*columns : (second+1)*columns]
	for column := range columns {
		firstRow[column], secondRow[column] = secondRow[column], firstRow[column]
	}
}

func scaleRow(values []Element, columns, row int, factor Element) {
	values = values[row*columns : (row+1)*columns]
	for column := range columns {
		values[column] = values[column].Mul(factor)
	}
}

func addScaledRow(values []Element, columns, destination, source int, factor Element) {
	destinationRow := values[destination*columns : (destination+1)*columns]
	sourceRow := values[source*columns : (source+1)*columns]
	for column := range columns {
		destinationRow[column] ^= sourceRow[column].Mul(factor)
	}
}
