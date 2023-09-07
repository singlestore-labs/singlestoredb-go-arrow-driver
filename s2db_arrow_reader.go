package s2db_arrow_driver

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/v12/arrow"
)

// S2DBArrowReader provides an API for reading arrow data from the SingleStore database
// The NewS2DBArrowReader function should be used to create a new instance of the S2DBArrowReader
type S2DBArrowReader interface {
	// GetNextArrowRecordBatch fetches a single arrow.Record from the server
	// It returns nil as the first part of the result tuple if there are no more rows to fetch
	// The returned Record must be Release()'d after use.
	GetNextArrowRecordBatch() (arrow.Record, error)
	// Close finalizes reading of the query results
	// It releases all acquired resources
	Close() error
}

type S2DBArrowReaderConfig struct {
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
	// EnableQueryLogging controls whether the driver should generate debug logs
	// Debug logs are printed to the standard output
	EnableQueryLogging bool
}

type S2DBParallelReadConfig struct {
	// DatabaseName is a name of the SingleStore database
	// It is needed to get number of partitions from the database for parallel read
	DatabaseName string
	// ChannelSize specifies size of the channel buffer
	// Channel is used to store references to Arrow Records while reading is happening
	// and transfer them to the main goroutine
	// The default value is 10000
	ChannelSize int64
	// Controls whether to profile the query
	// Profiling result is printed to the standart output
	EnableDebugProfiling bool
}

// NewS2DBArrowReader creates an instance of S2DBArrowReader
// It sends a query to the database server for execution
func NewS2DBArrowReader(ctx context.Context, conf S2DBArrowReaderConfig) (S2DBArrowReader, error) {
	if conf.Conn == nil {
		return nil, errors.New("'Conn' is a required configuration")
	}

	if conf.Query == "" {
		return nil, errors.New("'Query' is a required configuration")
	}

	if conf.RecordSize == 0 {
		conf.RecordSize = 10000
	}

	if conf.ParallelReadConfig == nil {
		return NewS2DBArrowReaderImpl(ctx, conf)
	} else {
		if conf.ParallelReadConfig.DatabaseName == "" {
			return nil, errors.New("'DatabaseName' is a required configuration for parallel read")
		}

		if conf.ParallelReadConfig.ChannelSize == 0 {
			conf.ParallelReadConfig.ChannelSize = 10000
		}
		return NewS2DBArrowReaderParallelImpl(ctx, conf)
	}
}
