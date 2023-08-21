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
	case "UNSIGNED TINYINT":
		if nullable {
			return NewNullUint8ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewUint8ColumnHandler(column.Name(), index, batchSize), nil
		}
	case "UNSIGNED SMALLINT":
		if nullable {
			return NewNullUint16ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewUint16ColumnHandler(column.Name(), index, batchSize), nil
		}
	case "UNSIGNED MEDIUMINT":
		if nullable {
			return NewNullUint32ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewUint32ColumnHandler(column.Name(), index, batchSize), nil
		}
	case "UNSIGNED INT":
		if nullable {
			return NewNullUint32ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewUint32ColumnHandler(column.Name(), index, batchSize), nil
		}
	case "UNSIGNED BIGINT":
		if nullable {
			return NewNullUint64ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewUint64ColumnHandler(column.Name(), index, batchSize), nil
		}

	case "TINYINT":
		if nullable {
			return NewNullInt8ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewBooleanColumnHandler(column.Name(), index, batchSize), nil
		}
	case "SMALLINT":
		if nullable {
			return NewNullInt16ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewInt16ColumnHandler(column.Name(), index, batchSize), nil
		}
	case "MEDIUMINT":
		if nullable {
			return NewNullInt32ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewInt32ColumnHandler(column.Name(), index, batchSize), nil
		}
	case "INT":
		if nullable {
			return NewNullInt32ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewInt32ColumnHandler(column.Name(), index, batchSize), nil
		}
	case "BIGINT":
		if nullable {
			return NewNullInt64ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewInt64ColumnHandler(column.Name(), index, batchSize), nil
		}

	case "FLOAT":
		if nullable {
			return NewNullFloat32ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewFloat32ColumnHandler(column.Name(), index, batchSize), nil
		}
	case "DOUBLE":
		if nullable {
			return NewNullFloat64ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewFloat64ColumnHandler(column.Name(), index, batchSize), nil
		}

	case "DATE", "TIME", "DATETIME", "TIMESTAMP":
		// TODO: choose a better data type
		if nullable {
			return NewNullStringColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewStringColumnHandler(column.Name(), index, batchSize), nil
		}

	case "YEAR":
		// TODO: choose a better data type
		if nullable {
			return NewNullUint16ColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewUint16ColumnHandler(column.Name(), index, batchSize), nil
		}

	case "DECIMAL":
		if nullable {
			return NewNullStringColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewStringColumnHandler(column.Name(), index, batchSize), nil
		}

	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "JSON":
		if nullable {
			return NewNullStringColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewStringColumnHandler(column.Name(), index, batchSize), nil
		}

	case "BIT", "BINARY", "VARBINARY", "TINYBLOB", "MEDIUMBLOB", "BLOB", "LONGBLOB":
		if nullable {
			return NewNullBinaryColumnHandler(column.Name(), index, batchSize), nil
		} else {
			return NewBinaryColumnHandler(column.Name(), index, batchSize), nil
		}
	default:
		return nil, fmt.Errorf("unsupported data type '%s'", column.DatabaseTypeName())
	}
}
