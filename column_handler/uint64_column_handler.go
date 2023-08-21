package column_handler

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type Uint64ColumnHandler struct {
	values    []uint64
	batchSize int64
	variable  *uint64
	field     arrow.Field
	index     int
}

func NewUint64ColumnHandler(name string, index int, batchSize int64) *Uint64ColumnHandler {
	res := &Uint64ColumnHandler{
		variable: new(uint64),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.PrimitiveTypes.Uint64,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]uint64, batchSize),
	}

	return res
}

func (th *Uint64ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *Uint64ColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *Uint64ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *Uint64ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Uint64Builder).AppendValues(th.values[:rows], nil)
}
