package column_handler

import (
	"database/sql"
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

type ColumnHandler interface {
	GetVariable() interface{}
	GetField() arrow.Field
	AppendValue(builder *array.RecordBuilder)
}

func GetColumnHandler(index int, column *sql.ColumnType) (ColumnHandler, error) {
	nullable, ok := column.Nullable()
	if !ok {
		panic("SQL driver doesn't support nullable property")
	}

	switch column.DatabaseTypeName() {
	case "BIGINT":
		if nullable {
			return NewNullInt64ColumnHandler(column.Name(), index), nil
		} else {
			return nil, fmt.Errorf("unsupported data type '%s'", column.DatabaseTypeName())
		}
	default:
		return nil, fmt.Errorf("unsupported data type '%s'", column.DatabaseTypeName())
	}
}
