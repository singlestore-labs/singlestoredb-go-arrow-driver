package column_handler

import (
	"github.com/Thor-x86/nullable"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullUint16ColumnHandler struct {
	values    []uint16
	notNull   []bool
	batchSize int64
	variable  *nullable.Uint16
	field     arrow.Field
	index     int
}

func NewNullUint16ColumnHandler(name string, index int, batchSize int64) *NullUint16ColumnHandler {
	res := &NullUint16ColumnHandler{
		variable: new(nullable.Uint16),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Uint16,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]uint16, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullUint16ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullUint16ColumnHandler) SetVariable(row int64) {
	valuePointer := th.variable.Get()
	if valuePointer == nil {
		th.notNull[row] = false
	} else {
		th.values[row] = *valuePointer
		th.notNull[row] = true
	}
}

func (th *NullUint16ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullUint16ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Uint16Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
