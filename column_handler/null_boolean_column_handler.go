package column_handler

import (
	"database/sql"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullBooleanColumnHandler struct {
	values    []bool
	notNull   []bool
	batchSize int64
	variable  *sql.NullBool
	field     arrow.Field
	index     int
}

func NewNullBooleanColumnHandler(name string, index int, batchSize int64) *NullBooleanColumnHandler {
	res := &NullBooleanColumnHandler{
		variable: new(sql.NullBool),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.FixedWidthTypes.Boolean,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]bool, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullBooleanColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullBooleanColumnHandler) SetVariable(row int64) {
	th.values[row] = th.variable.Bool
	th.notNull[row] = th.variable.Valid
}

func (th *NullBooleanColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullBooleanColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.BooleanBuilder).AppendValues(th.values[:rows], th.notNull[:rows])
}
