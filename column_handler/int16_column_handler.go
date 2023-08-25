package column_handler

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type Int16ColumnHandler struct {
	values    []int16
	batchSize int64
	variable  *int16
	field     arrow.Field
	index     int
}

func NewInt16ColumnHandler(name string, index int, batchSize int64) *Int16ColumnHandler {
	res := &Int16ColumnHandler{
		variable: new(int16),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Int16,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]int16, batchSize),
	}

	return res
}

func (th *Int16ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Int16ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Int16ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Int16ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Int16Builder).AppendValues(th.values[:rows], nil)
}
