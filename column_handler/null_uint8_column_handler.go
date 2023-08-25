package column_handler

import (
	"github.com/Thor-x86/nullable"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type NullUint8ColumnHandler struct {
	values    []uint8
	notNull   []bool
	batchSize int64
	variable  *nullable.Uint8
	field     arrow.Field
	index     int
}

func NewNullUint8ColumnHandler(name string, index int, batchSize int64) *NullUint8ColumnHandler {
	res := &NullUint8ColumnHandler{
		variable: new(nullable.Uint8),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Uint8,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]uint8, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullUint8ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullUint8ColumnHandler) SetVariable(row int64) {
	valuePointer := th.variable.Get()
	if valuePointer == nil {
		th.notNull[row] = false
	} else {
		th.values[row] = *valuePointer
		th.notNull[row] = true
	}
}

func (th *NullUint8ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullUint8ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Uint8Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
