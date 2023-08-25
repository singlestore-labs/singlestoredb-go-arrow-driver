package s2db_arrow_driver

import (
	"context"
	"database/sql"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/singlestore-labs/singlestoredb-go-arrow-driver/column_handler"
)

// S2DBArrow implements S2DBArrowReader
type S2DBArrowReaderImpl struct {
	conn           S2SqlDbWrapper
	rows           *sql.Rows
	recordSize     int64
	recordBuilder  *array.RecordBuilder
	columnHandlers []column_handler.ColumnHandler
	variables      []interface{}
}

func NewS2DBArrowReaderImpl(ctx context.Context, conf S2DBArrowReaderConfig) (S2DBArrowReader, error) {
	var err error = nil
	rows, err := conf.Conn.QueryContext(ctx, conf.Query, conf.Args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		// If we failed to create a reader - clean up rows
		if err != nil {
			rows.Close()
		}
	}()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columnHandlers := make([]column_handler.ColumnHandler, len(cols))
	fields := make([]arrow.Field, len(cols))
	variables := make([]interface{}, len(cols))
	for index, col := range cols {
		columnHandlers[index], err = column_handler.GetColumnHandler(index, col, conf.RecordSize)
		if err != nil {
			return nil, err
		}

		fields[index] = columnHandlers[index].GetField()
		variables[index] = columnHandlers[index].GetVariable()
	}

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		fields,
		nil,
	)
	recordBuilder := array.NewRecordBuilder(pool, schema)

	return &S2DBArrowReaderImpl{
		conn:           conf.Conn,
		rows:           rows,
		recordSize:     conf.RecordSize,
		recordBuilder:  recordBuilder,
		columnHandlers: columnHandlers,
		variables:      variables,
	}, err
}

func (s2db *S2DBArrowReaderImpl) GetNextArrowRecordBatch() (arrow.Record, error) {
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

func (s2db *S2DBArrowReaderImpl) Close() error {
	defer func() {
		if s2db.recordBuilder != nil {
			s2db.recordBuilder.Release()
		}
	}()

	if s2db.rows == nil {
		return nil
	}

	return s2db.rows.Close()
}
