package s2db_arrow_driver

import "github.com/apache/arrow/go/arrow/array"

// S2DBArrowReader provides an API for reading arrow data from the SingleStore database
// The NewS2DBArrowReader function should be used to create a new instance of the S2DBArrowReader
type S2DBArrowReader interface {
	// GetNextArrowRecordBatch fetches a single arrow.Record from the server
	// It returns nil as the first part of the result tuple if there are no more rows to fetch
	GetNextArrowRecordBatch() (array.Record, error)
	// Close finalizes reading of the query results
	// It releases all acquired resources
	Close() error
}
