package column_handler

import (
	"github.com/Thor-x86/nullable"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type NullFloat64ColumnHandler struct {
	values    []float64
	notNull   []bool
	batchSize int64
	variable  *nullable.Float64
	field     arrow.Field
	index     int
}

func NewNullFloat64ColumnHandler(name string, index int, batchSize int64) *NullFloat64ColumnHandler {
	res := &NullFloat64ColumnHandler{
		variable: new(nullable.Float64),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Float64,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]float64, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullFloat64ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullFloat64ColumnHandler) SetVariable(row int64) {
	valuePointer := th.variable.Get()
	if valuePointer == nil {
		th.notNull[row] = false
	} else {
		th.values[row] = *valuePointer
		th.notNull[row] = true
	}
}

func (th *NullFloat64ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullFloat64ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Float64Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
