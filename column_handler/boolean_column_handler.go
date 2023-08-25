package column_handler

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type BooleanColumnHandler struct {
	values    []bool
	batchSize int64
	variable  *bool
	field     arrow.Field
	index     int
}

func NewBooleanColumnHandler(name string, index int, batchSize int64) *BooleanColumnHandler {
	res := &BooleanColumnHandler{
		variable: new(bool),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.FixedWidthTypes.Boolean,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]bool, batchSize),
	}

	return res
}

func (th *BooleanColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *BooleanColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *BooleanColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *BooleanColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.BooleanBuilder).AppendValues(th.values[:rows], nil)
}
