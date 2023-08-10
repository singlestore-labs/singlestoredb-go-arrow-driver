package s2db_arrow_driver

import (
	"context"
	"database/sql"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/singlestore-labs/singlestoredb-go-arrow-driver/column_handler"
)

// S2DBArrow implements S2DBArrowExecutor
type S2DBArrow struct {
	Conn           S2DB
	rows           *sql.Rows
	recordSize     int64
	recordBuilder  *array.RecordBuilder
	columnHandlers []column_handler.ColumnHandler
	variables      []interface{}
}

func (s2db *S2DBArrow) Execute(ctx context.Context, recordSize int64, query string, args ...interface{}) error {
	s2db.recordSize = recordSize

	var err error = nil
	s2db.rows, err = s2db.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	cols, err := s2db.rows.ColumnTypes()
	if err != nil {
		return err
	}

	s2db.columnHandlers = make([]column_handler.ColumnHandler, len(cols))
	fields := make([]arrow.Field, len(cols))
	s2db.variables = make([]interface{}, len(cols))
	for index, col := range cols {
		s2db.columnHandlers[index], err = column_handler.GetColumnHandler(index, col, s2db.recordSize)
		if err != nil {
			return err
		}

		fields[index] = s2db.columnHandlers[index].GetField()
		s2db.variables[index] = s2db.columnHandlers[index].GetVariable()
	}

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		fields,
		nil,
	)
	s2db.recordBuilder = array.NewRecordBuilder(pool, schema)

	return nil
}

func (s2db *S2DBArrow) GetNextArrowRecordBatch() (array.Record, error) {
	var err error = nil
	rowsRead := int64(0)

	for ; rowsRead < s2db.recordSize && s2db.rows.Next(); rowsRead++ {
		err = s2db.rows.Scan(s2db.variables...)
		if err != nil {
			return nil, err
		}

		for _, handler := range s2db.columnHandlers {
			handler.SetVariable(rowsRead)
		}
	}
	if rowsRead == 0 {
		return nil, nil
	}

	for _, handler := range s2db.columnHandlers {
		handler.AppendValues(s2db.recordBuilder, rowsRead)
	}

	return s2db.recordBuilder.NewRecord(), nil
}

func (s2db *S2DBArrow) Close() error {
	defer s2db.recordBuilder.Release()

	return s2db.rows.Close()
}
