package s2db_arrow_driver

import "github.com/apache/arrow/go/arrow/array"

// S2DBArrowReader provides an API for methods exposed to the clients
type S2DBArrowReader interface {
	GetNextArrowRecordBatch() (array.Record, error)
	Close() error
}
