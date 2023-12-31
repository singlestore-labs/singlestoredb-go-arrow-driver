package s2db_arrow_driver

import (
	"context"
	"database/sql"
)

// S2DB is a utility wrapper around sql.DB, used only by the driver
type S2SqlDbWrapper interface {
	Stats() sql.DBStats
	Close() error
	Conn(ctx context.Context) (*sql.Conn, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}
