package column_handler

import (
	"database/sql"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullInt32ColumnHandler struct {
	values    []int32
	notNull   []bool
	batchSize int64
	variable  *sql.NullInt32
	field     arrow.Field
	index     int
}

func NewNullInt32ColumnHandler(name string, index int, batchSize int64) *NullInt32ColumnHandler {
	res := &NullInt32ColumnHandler{
		variable: new(sql.NullInt32),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Int32,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]int32, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullInt32ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullInt32ColumnHandler) SetVariable(row int64) {
	th.values[row] = th.variable.Int32
	th.notNull[row] = th.variable.Valid
}

func (th *NullInt32ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullInt32ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Int32Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
