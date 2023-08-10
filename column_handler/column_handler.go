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
	AppendValues(builder *array.RecordBuilder, rows int64)
	SetVariable(row int64)
}

func GetColumnHandler(index int, column *sql.ColumnType, batchSize int64) (ColumnHandler, error) {
	nullable, ok := column.Nullable()
	if !ok {
		panic("SQL driver doesn't support nullable property")
	}

	switch column.DatabaseTypeName() {
	case "BIGINT":
		if nullable {
			return NewNullInt64ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return nil, fmt.Errorf("unsupported data type '%s'", column.DatabaseTypeName())
		}
	default:
		return nil, fmt.Errorf("unsupported data type '%s'", column.DatabaseTypeName())
	}
}
