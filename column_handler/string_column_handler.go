package column_handler

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type StringColumnHandler struct {
	values    []string
	batchSize int64
	variable  *string
	field     arrow.Field
	index     int
}

func NewStringColumnHandler(name string, index int, batchSize int64) *StringColumnHandler {
	res := &StringColumnHandler{
		variable: new(string),
		field: arrow.Field{
			Name:     name,
			Nullable: false,
			Type:     arrow.BinaryTypes.String,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]string, batchSize),
	}

	return res
}

func (th *StringColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *StringColumnHandler) SetVariable(row int64) {
	th.values[row] = *th.variable
}

func (th *StringColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *StringColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.StringBuilder).AppendValues(th.values[:rows], nil)
}
