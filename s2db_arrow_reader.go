package s2db_arrow_driver

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/arrow/array"
)

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

type S2DbArrowReaderConfig struct {
	// Conn is a sql.DB object which will be used to communicate with the database
	Conn S2SqlDbWrapper
	// Query is a SQL query that will be executed
	Query string
	// Args are arguments for placeholder parameters in the query
	Args []interface{}
	// RecordSize identifies maximum number of rows in the resulting records
	// By default it is 10000
	RecordSize int64
	// ParallelReadConfig specifies aditional configurations for parallel read
	// By default it is nil and it means that parallel read is not used
	ParallelReadConfig *S2DBParallelReadConfig
}

type S2DBParallelReadConfig struct {
	// DatabaseName is a name of the SingleStore database
	DatabaseName string
	// ChannelSize specifies size of the channel buffer
	// Channel is used to communicate between reading threads
	ChannelSize int64
}

// NewS2DBArrowReader creates an instance of S2DBArrowReader
// It sends a query to the database server for execution
func NewS2DBArrowReader(ctx context.Context, conf S2DbArrowReaderConfig) (S2DBArrowReader, error) {
	if conf.Conn == nil {
		return nil, errors.New("conn is a required configuration")
	}

	if conf.Query == "" {
		return nil, errors.New("query is a required configuration")
	}

	if conf.RecordSize == 0 {
		conf.RecordSize = 10000
	}

	if conf.ParallelReadConfig == nil {
		return NewS2DBArrowReaderImpl(ctx, conf)
	} else {
		if conf.ParallelReadConfig.DatabaseName == "" {
			return nil, errors.New("database name is a required configuration for parallel read")
		}

		if conf.ParallelReadConfig.ChannelSize == 0 {
			conf.ParallelReadConfig.ChannelSize = 10000
		}
		return NewS2DBArrowReaderParallelImpl(ctx, conf)
	}
}
