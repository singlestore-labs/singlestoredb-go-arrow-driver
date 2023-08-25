package column_handler

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type Int8ColumnHandler struct {
	values    []int8
	batchSize int64
	variable  *int8
	field     arrow.Field
	index     int
}

func NewInt8ColumnHandler(name string, index int, batchSize int64) *Int8ColumnHandler {
	res := &Int8ColumnHandler{
		variable: new(int8),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Int8,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]int8, batchSize),
	}

	return res
}

func (th *Int8ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Int8ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Int8ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Int8ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Int8Builder).AppendValues(th.values[:rows], nil)
}
