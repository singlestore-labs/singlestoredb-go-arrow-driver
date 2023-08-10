package s2db_arrow_driver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	_ "github.com/go-sql-driver/mysql"
)

func readMySQL(conn *sql.DB, query string) error {
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	start := time.Now()
	a := new(int64)
	for rows.Next() {
		err = rows.Scan(a)
		if err != nil {
			return err
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("Reading took %s\n", elapsed)

	return nil
}

func readArrow(conn *sql.DB, query string) error {
	arrowExecutor := S2DBArrow{Conn: conn}
	defer arrowExecutor.Close()

	err := arrowExecutor.Execute(context.Background(), 100000, "SELECT * FROM t")
	if err != nil {
		return err
	}

	start := time.Now()
	batches := make([]array.Record, 0)
	for batch, err := arrowExecutor.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowExecutor.GetNextArrowRecordBatch() {
		if err != nil {
			return err
		}

		batches = append(batches, batch)
	}
	elapsed := time.Since(start)
	fmt.Printf("Reading with parsing took %s\n", elapsed)

	return nil
}

func TestRead(t *testing.T) {
	db, err := sql.Open("mysql", "root:1@tcp(127.0.0.1:5506)/db")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	err = readMySQL(db, "SELECT * FROM t")
	if err != nil {
		t.Error(err)
	}

	err = readArrow(db, "SELECT * FROM t")
	if err != nil {
		t.Error(err)
	}
}
