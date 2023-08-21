package column_handler

import (
	"github.com/Thor-x86/nullable"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullBinaryColumnHandler struct {
	values    [][]byte
	notNull   []bool
	batchSize int64
	variable  *nullable.Bytes
	field     arrow.Field
	index     int
}

func NewNullBinaryColumnHandler(name string, index int, batchSize int64) *NullBinaryColumnHandler {
	res := &NullBinaryColumnHandler{
		variable: new(nullable.Bytes),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.BinaryTypes.Binary,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([][]byte, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullBinaryColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullBinaryColumnHandler) SetVariable(row int64) {
	valuePointer := th.variable.Get()
	if valuePointer == nil {
		th.notNull[row] = false
	} else {
		th.values[row] = *valuePointer
		th.notNull[row] = true
	}
}

func (th *NullBinaryColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullBinaryColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.BinaryBuilder).AppendValues(th.values[:rows], th.notNull[:rows])
}
