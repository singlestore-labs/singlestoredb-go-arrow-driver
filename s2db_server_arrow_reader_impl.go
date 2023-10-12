package s2db_arrow_driver

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// S2DBServerArrowReaderImpl implements S2DBArrowReader
type S2DBServerArrowReaderImpl struct {
	conn           S2SqlDbWrapper
	rows           *sql.Rows
	recordSize     int64
	alloc          *memory.GoAllocator
}

func NewS2DBServerArrowReaderImpl(ctx context.Context, conf S2DBArrowReaderConfig) (S2DBArrowReader, error) {
	var err error = nil
	query := fmt.Sprintf("%s OPTION(result_arrow_batch = %d)", conf.Query, conf.RecordSize)
	rows, err := queryContext(ctx, conf.Conn, query, conf.EnableQueryLogging, conf.Args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		// If we failed to create a reader - clean up rows
		if err != nil {
			rows.Close()
		}
	}()

	pool := memory.NewGoAllocator()

	return &S2DBServerArrowReaderImpl{
		conn:       conf.Conn,
		rows:       rows,
		recordSize: conf.RecordSize,
		alloc:      pool,
	}, nil
}

func (s2db *S2DBServerArrowReaderImpl) GetNextArrowRecordBatch() (arrow.Record, error) {
	var err error = nil

	if !s2db.rows.Next() {
		return nil, s2db.rows.Err()
	}
	b := new([]byte)
	if err = s2db.rows.Scan(&b); err != nil {
		return nil, err
	}
	r, err := ipc.NewReader(bytes.NewReader(*b))
	if err != nil {
		return nil, err
	}
	if !r.Next() {
		return nil, errors.New("could not extract Record from row")
	}
	return r.Record(), nil
}

func (s2db *S2DBServerArrowReaderImpl) Close() error {
	return s2db.rows.Close()
}
