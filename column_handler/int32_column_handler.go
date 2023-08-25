package column_handler

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type Int32ColumnHandler struct {
	values    []int32
	batchSize int64
	variable  *int32
	field     arrow.Field
	index     int
}

func NewInt32ColumnHandler(name string, index int, batchSize int64) *Int32ColumnHandler {
	res := &Int32ColumnHandler{
		variable: new(int32),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Int32,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]int32, batchSize),
	}

	return res
}

func (th *Int32ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Int32ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Int32ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Int32ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Int32Builder).AppendValues(th.values[:rows], nil)
}
