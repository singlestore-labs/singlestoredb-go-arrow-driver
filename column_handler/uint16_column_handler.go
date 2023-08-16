package column_handler

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type Uint16ColumnHandler struct {
	values    []uint16
	batchSize int64
	variable  *uint16
	field     arrow.Field
	index     int
}

func NewUint16ColumnHandler(name string, index int, batchSize int64) *Uint16ColumnHandler {
	res := &Uint16ColumnHandler{
		variable: new(uint16),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Uint16,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]uint16, batchSize),
	}

	return res
}

func (th *Uint16ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Uint16ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Uint16ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Uint16ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Uint16Builder).AppendValues(th.values[:rows], nil)
}
