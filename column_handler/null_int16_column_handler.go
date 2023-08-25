package column_handler

import (
	"database/sql"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type NullInt16ColumnHandler struct {
	values    []int16
	notNull   []bool
	batchSize int64
	variable  *sql.NullInt16
	field     arrow.Field
	index     int
}

func NewNullInt16ColumnHandler(name string, index int, batchSize int64) *NullInt16ColumnHandler {
	res := &NullInt16ColumnHandler{
		variable: new(sql.NullInt16),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Int16,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]int16, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullInt16ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullInt16ColumnHandler) SetVariable(row int64) {
	th.values[row] = th.variable.Int16
	th.notNull[row] = th.variable.Valid
}

func (th *NullInt16ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullInt16ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Int16Builder).AppendValues(th.values[:rows], th.notNull[:rows])
}
