package column_handler

import (
	"database/sql"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

type NullTimestampColumnHandler struct {
	values    []arrow.Timestamp
	notNull   []bool
	batchSize int64
	variable  *sql.NullTime
	field     arrow.Field
	index     int
}

func NewNullTimestampColumnHandler(name string, index int, batchSize int64) *NullTimestampColumnHandler {
	res := &NullTimestampColumnHandler{
		variable: new(sql.NullTime),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Int64,
		},
		index:     index,
		batchSize: batchSize,
		values:    make([]arrow.Timestamp, batchSize),
		notNull:   make([]bool, batchSize),
	}

	return res
}

func (th *NullTimestampColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullTimestampColumnHandler) SetVariable(row int64) {
	th.values[row] = arrow.Timestamp(th.variable.Time.UnixNano())
	th.notNull[row] = th.variable.Valid
}

func (th *NullTimestampColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullTimestampColumnHandler) AppendValues(builder *array.RecordBuilder, rows int64) {
	builder.Field(th.index).(*array.TimestampBuilder).AppendValues(th.values[:rows], th.notNull[:rows])
}
