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
	a := new(sql.NullInt32)
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

func readArrow(conn *sql.DB, query string, printRows bool) error {
	arrowExecutor, err := NewS2DBArrowReader(context.Background(), S2DbArrowReaderConfig{
		Conn:  conn,
		Query: query,
	})
	if err != nil {
		return err
	}
	defer arrowExecutor.Close()

	start := time.Now()
	batches := make([]array.Record, 0)
	for batch, err := arrowExecutor.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowExecutor.GetNextArrowRecordBatch() {
		if err != nil {
			return err
		}

		batches = append(batches, batch)
	}
	elapsed := time.Since(start)
	fmt.Printf("reading with parsing to arrow took %s\n", elapsed)

	if printRows {
		for _, batch := range batches {
			for i, col := range batch.Columns() {
				fmt.Printf("column[%d] %q: %v\n", i, batch.ColumnName(i), col)
			}
		}
	}
	return nil
}

func readArrowParallel(conn *sql.DB, query string, printRows bool) error {
	arrowExecutor, err := NewS2DBArrowReader(context.Background(), S2DbArrowReaderConfig{
		Conn:  conn,
		Query: query,
		ParallelReadConfig: &S2DBParallelReadConfig{
			DatabaseName: "db",
		},
	})
	if err != nil {
		return err
	}
	defer arrowExecutor.Close()

	start := time.Now()
	batches := make([]array.Record, 0)
	for batch, err := arrowExecutor.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowExecutor.GetNextArrowRecordBatch() {
		if err != nil {
			return err
		}

		batches = append(batches, batch)
	}
	elapsed := time.Since(start)
	fmt.Printf("Parallel reading with parsing to arrow took %s\n", elapsed)

	if printRows {
		for _, batch := range batches {
			for i, col := range batch.Columns() {
				fmt.Printf("column[%d] %q: %v\n", i, batch.ColumnName(i), col)
			}
		}
	}
	return nil
}

func TestRead(t *testing.T) {
	db, err := sql.Open("mysql", "root:1@tcp(127.0.0.1:5506)/db")
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	query := "SELECT * FROM t"

	err = readArrow(db, query, false)
	if err != nil {
		t.Error(err)
	}

	err = readArrowParallel(db, query, false)
	if err != nil {
		t.Error(err)
	}

	err = readMySQL(db, query)
	if err != nil {
		t.Error(err)
	}
}
