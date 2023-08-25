package column_handler

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type BinaryColumnHandler struct {
	values    [][]byte
	batchSize int64
	variable  *[]byte
	field     arrow.Field
	index     int
}

func NewBinaryColumnHandler(name string, index int, batchSize int64) *BinaryColumnHandler {
	res := &BinaryColumnHandler{
		variable: new([]byte),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.BinaryTypes.Binary,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([][]byte, batchSize),
	}

	return res
}

func (th *BinaryColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *BinaryColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *BinaryColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *BinaryColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.BinaryBuilder).AppendValues(th.values[:rows], nil)
}
