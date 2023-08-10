package column_handler

import (
	"database/sql"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullInt64ColumnHandler struct {
	values    []int64
	isNull    []bool
	batchSize int64
	variable  *sql.NullInt64
	field     arrow.Field
	index     int
}

func NewNullInt64ColumnHandler(name string, index int, batchSize int64) *NullInt64ColumnHandler {
	res := &NullInt64ColumnHandler{
		variable: new(sql.NullInt64),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Int64,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]int64, batchSize),
		isNull:    make([]bool, batchSize),
	}

	return res
}

func (th *NullInt64ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullInt64ColumnHandler) SetVariable(row int64) {
	th.values[row] = th.variable.Int64
	th.isNull[row] = th.variable.Valid
}

func (th *NullInt64ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullInt64ColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.Int64Builder).AppendValues(th.values[:rows], th.isNull[:rows])
}
