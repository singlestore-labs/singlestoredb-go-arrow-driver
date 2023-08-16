package column_handler

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type Int64ColumnHandler struct {
	values    []int64
	batchSize int64
	variable  *int64
	field     arrow.Field
	index     int
}

func NewInt64ColumnHandler(name string, index int, batchSize int64) *Int64ColumnHandler {
	res := &Int64ColumnHandler{
		variable: new(int64),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Int64,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]int64, batchSize),
	}

	return res
}

func (th *Int64ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Int64ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Int64ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Int64ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Int64Builder).AppendValues(th.values[:rows], nil)
}
