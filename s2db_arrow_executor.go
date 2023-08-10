package s2db_arrow_driver

import (
	"context"

	"github.com/apache/arrow/go/v13/arrow"
)

// S2DBArrowExecutor provides an API for methods exposed to the clients
type S2DBArrowExecutor interface {
	Execute(ctx context.Context, recordSize int64, query string, args ...interface{}) error
	GetNextArrowRecordBatch() (*arrow.Record, error)
	Close() error
}
