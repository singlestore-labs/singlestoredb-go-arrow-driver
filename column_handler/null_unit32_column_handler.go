package column_handler

import (
	"github.com/Thor-x86/nullable"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type NullUint32ColumnHandler struct {
	values    []uint32
	notNull   []bool
	batchSize int64
	variable  *nullable.Uint32
	field     arrow.Field
	index     int
}

func NewNullUint32ColumnHandler(name string, index int, batchSize int64) *NullUint32ColumnHandler {
	res := &NullUint32ColumnHandler{
		variable: new(nullable.Uint32),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Uint32,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]uint32, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullUint32ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullUint32ColumnHandler) SetVariable(row int64) {
	valuePointer := th.variable.Get()
	if valuePointer == nil {
		th.notNull[row] = false
	} else {
		th.values[row] = *valuePointer
		th.notNull[row] = true
	}
}

func (th *NullUint32ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullUint32ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Uint32Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
