package column_handler

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type Float64ColumnHandler struct {
	values    []float64
	batchSize int64
	variable  *float64
	field     arrow.Field
	index     int
}

func NewFloat64ColumnHandler(name string, index int, batchSize int64) *Float64ColumnHandler {
	res := &Float64ColumnHandler{
		variable: new(float64),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Float64,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]float64, batchSize),
	}

	return res
}

func (th *Float64ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Float64ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Float64ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Float64ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Float64Builder).AppendValues(th.values[:rows], nil)
}
