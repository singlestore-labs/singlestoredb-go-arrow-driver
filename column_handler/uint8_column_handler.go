package column_handler

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type Uint8ColumnHandler struct {
	values    []uint8
	batchSize int64
	variable  *uint8
	field     arrow.Field
	index     int
}

func NewUint8ColumnHandler(name string, index int, batchSize int64) *Uint8ColumnHandler {
	res := &Uint8ColumnHandler{
		variable: new(uint8),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Uint8,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]uint8, batchSize),
	}

	return res
}

func (th *Uint8ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Uint8ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Uint8ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Uint8ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Uint8Builder).AppendValues(th.values[:rows], nil)
}
