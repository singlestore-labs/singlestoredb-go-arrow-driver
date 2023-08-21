package column_handler

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type Uint32ColumnHandler struct {
	values    []uint32
	batchSize int64
	variable  *uint32
	field     arrow.Field
	index     int
}

func NewUint32ColumnHandler(name string, index int, batchSize int64) *Uint32ColumnHandler {
	res := &Uint32ColumnHandler{
		variable: new(uint32),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Uint32,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]uint32, batchSize),
	}

	return res
}

func (th *Uint32ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Uint32ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Uint32ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Uint32ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Uint32Builder).AppendValues(th.values[:rows], nil)
}
