package column_handler

import (
	"database/sql"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type NullInt64ColumnHandler struct {
	variable *sql.NullInt64
	field    arrow.Field
	index    int
}

func NewNullInt64ColumnHandler(name string, index int) *NullInt64ColumnHandler {
	res := &NullInt64ColumnHandler{
		variable: new(sql.NullInt64),
		field: arrow.Field{
			Name:     name,
			Nullable: true,
			Type:     arrow.PrimitiveTypes.Int64,
		},
		index: index,
	}

	return res
}

func (th *NullInt64ColumnHandler) GetVariable() interface{} {
	return th.variable
}

func (th *NullInt64ColumnHandler) GetField() arrow.Field {
	return th.field
}

func (th *NullInt64ColumnHandler) AppendValue(builder *array.RecordBuilder) {
	if !th.variable.Valid {
		builder.Field(th.index).(*array.Int64Builder).AppendNull()
	} else {
		builder.Field(th.index).(*array.Int64Builder).Append(th.variable.Int64)
	}
}
