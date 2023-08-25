package column_handler

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type Float32ColumnHandler struct {
	values    []float32
	batchSize int64
	variable  *float32
	field     arrow.Field
	index     int
}

func NewFloat32ColumnHandler(name string, index int, batchSize int64) *Float32ColumnHandler {
	res := &Float32ColumnHandler{
		variable: new(float32),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Float32,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]float32, batchSize),
	}

	return res
}

func (th *Float32ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Float32ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Float32ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Float32ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Float32Builder).AppendValues(th.values[:rows], nil)
}
