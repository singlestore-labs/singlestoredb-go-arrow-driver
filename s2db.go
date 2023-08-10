package s2db_arrow_driver

import (
	"context"
	"database/sql"
)

// S2DB is a utility wrapper around sql.DB, used only by the driver
type S2DB interface {
	Stats() sql.DBStats
	Close() error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}
