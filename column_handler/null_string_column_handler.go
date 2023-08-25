package column_handler

import (
	"database/sql"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type NullStringColumnHandler struct {
	values    []string
	notNull   []bool
	batchSize int64
	variable  *sql.NullString
	field     arrow.Field
	index     int
}

func NewNullStringColumnHandler(name string, index int, batchSize int64) *NullStringColumnHandler {
	res := &NullStringColumnHandler{
		variable: new(sql.NullString),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.BinaryTypes.String,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]string, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullStringColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullStringColumnHandler) SetVariable(row int64) {
	th.values[row] = th.variable.String
	th.notNull[row] = th.variable.Valid
}

func (th *NullStringColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullStringColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.StringBuilder).AppendValues(th.values[:rows], th.notNull[:rows])
}
