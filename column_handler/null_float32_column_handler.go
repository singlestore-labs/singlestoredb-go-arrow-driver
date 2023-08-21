package column_handler

import (
	"github.com/Thor-x86/nullable"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullFloat32ColumnHandler struct {
	values    []float32
	notNull   []bool
	batchSize int64
	variable  *nullable.Float32
	field     arrow.Field
	index     int
}

func NewNullFloat32ColumnHandler(name string, index int, batchSize int64) *NullFloat32ColumnHandler {
	res := &NullFloat32ColumnHandler{
		variable: new(nullable.Float32),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Float32,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]float32, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullFloat32ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullFloat32ColumnHandler) SetVariable(row int64) {
	valuePointer := th.variable.Get()
	if valuePointer == nil {
		th.notNull[row] = false
	} else {
		th.values[row] = *valuePointer
		th.notNull[row] = true
	}
}

func (th *NullFloat32ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullFloat32ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Float32Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
