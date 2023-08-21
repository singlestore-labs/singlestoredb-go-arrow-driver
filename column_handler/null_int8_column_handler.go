package column_handler

import (
	"database/sql"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullInt8ColumnHandler struct {
	values    []int8
	notNull   []bool
	batchSize int64
	variable  *sql.NullInt16
	field     arrow.Field
	index     int
}

func NewNullInt8ColumnHandler(name string, index int, batchSize int64) *NullInt8ColumnHandler {
	res := &NullInt8ColumnHandler{
		variable: new(sql.NullInt16),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Int8,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]int8, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullInt8ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullInt8ColumnHandler) SetVariable(row int64) {
	th.values[row] = int8(th.variable.Int16)
	th.notNull[row] = th.variable.Valid
}

func (th *NullInt8ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullInt8ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Int8Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
