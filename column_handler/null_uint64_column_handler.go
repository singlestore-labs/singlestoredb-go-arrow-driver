package column_handler

import (
	"github.com/Thor-x86/nullable"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullUint64ColumnHandler struct {
	values    []uint64
	notNull   []bool
	batchSize int64
	variable  *nullable.Uint64
	field     arrow.Field
	index     int
}

func NewNullUint64ColumnHandler(name string, index int, batchSize int64) *NullUint64ColumnHandler {
	res := &NullUint64ColumnHandler{
		variable: new(nullable.Uint64),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Uint64,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]uint64, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullUint64ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullUint64ColumnHandler) SetVariable(row int64) {
	valuePointer := th.variable.Get()
	if valuePointer == nil {
		th.notNull[row] = false
	} else {
		th.values[row] = *valuePointer
		th.notNull[row] = true
	}
}

func (th *NullUint64ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullUint64ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Uint64Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
