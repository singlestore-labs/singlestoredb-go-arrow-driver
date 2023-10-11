package s2db_arrow_driver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/Thor-x86/nullable"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

var USER = "root"
var PASSWORD = ""
var HOST = "127.0.0.1"
var DB = "testdb"
var PORT = 3306
var DSN_BASE = fmt.Sprintf("%s:%s@tcp(%s:%d)/", USER, PASSWORD, HOST, PORT)
var DSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", USER, PASSWORD, HOST, PORT, DB)

var CREATE_TEST_TABLE = true
var DROP_TEST_TABLE = false

type readFunction func(*sql.DB, string) error

func beforeAllCreateTable(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", DB)); err != nil {
		return err
	}

	if _, err := db.Exec(fmt.Sprintf("USE %s", DB)); err != nil {
		return err
	}

	if _, err := db.Exec("DROP TABLE IF EXISTS t"); err != nil {
		return err
	}

	if _, err := db.Exec("CREATE TABLE t(a BIGINT, b DOUBLE, t VARCHAR(256))"); err != nil {
		return err
	}

	query := "INSERT INTO t VALUES "
	i := 0
	for i = 0; i < 25 * 25 * 25 - 1; i++ {
		query += fmt.Sprintf("(%d,100.1001,'asdasdasdasd'),", i)
	}
	query += fmt.Sprintf("(%d,100.1001,'asdasdasdasd')", i)
	if _, err := db.Exec(query); err != nil {
		return err
	}
	for i := 0; i < 7; i++ {
		if _, err := db.Exec("INSERT INTO t SELECT * FROM t"); err != nil {
			return err
		}
	}
	return nil
}

func afterAllDropTable(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS t")
	return err
}

func readRegular(conn *sql.DB, query string) error {
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	a := new(int64)
	b := new(nullable.Float64)
	c := new(sql.NullString)
	for rows.Next() {
		err = rows.Scan(a, b, c)
		if err != nil {
			return err
		}
	}
	return nil
}

func readParallel(conn *sql.DB, query string) error {
	partitions, err := getPartitionsCount(context.Background(), conn, DB, false)
	if err != nil {
		return err
	}

	resultTableConn, err := conn.Conn(context.Background())
	if err != nil {
		return err
	}
	defer resultTableConn.Close()

	createResultTableQuery := fmt.Sprintf("CREATE RESULT TABLE tmp AS SELECT * FROM (%s)", query)
	_, err = resultTableConn.ExecContext(context.Background(), createResultTableQuery)
	if err != nil {
		return err
	}
	defer resultTableConn.ExecContext(context.Background(), "DROP RESULT TABLE tmp")

	errorGroup := new(errgroup.Group)
	for i := 0; i < int(partitions); i++ {
		partition := i
		errorGroup.Go(func() error {
			rows, err := conn.Query(fmt.Sprintf("SELECT * FROM ::tmp WHERE partition_id() = %d", partition))
			if err != nil {
				return err
			}
			defer rows.Close()

			a := new(int64)
			b := new(nullable.Float64)
			c := new(sql.NullString)
			for rows.Next() {
				err = rows.Scan(a, b, c)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}
	return errorGroup.Wait()
}

func readArrow(conn *sql.DB, query string) error {
	arrowReader, err := NewS2DBArrowReader(context.Background(), S2DBArrowReaderConfig{
		Conn:  conn,
		Query: query,
		UseClientConvesion: true,
	})
	if err != nil {
		return err
	}
	defer arrowReader.Close()

	for batch, err := arrowReader.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowReader.GetNextArrowRecordBatch() {
		if err != nil {
			return err
		}
	}
	return nil
}

func readArrowParallel(conn *sql.DB, query string) error {
	arrowReader, err := NewS2DBArrowReader(context.Background(), S2DBArrowReaderConfig{
		Conn:  conn,
		Query: query,
		ParallelReadConfig: &S2DBParallelReadConfig{
			DatabaseName: DB,
		},
		UseClientConvesion: true,
	})
	if err != nil {
		return err
	}
	defer arrowReader.Close()

	for batch, err := arrowReader.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowReader.GetNextArrowRecordBatch() {
		if err != nil {
			return err
		}
	}
	return nil
}

func readArrowServer(conn *sql.DB, query string) error {
	arrowReader, err := NewS2DBArrowReader(context.Background(), S2DBArrowReaderConfig{
		Conn:  conn,
		Query: query,
		RecordSize: 1000,
		UseClientConvesion: false,
		EnableQueryLogging: true,
	})
	if err != nil {
		return err
	}
	defer arrowReader.Close()

	for batch, err := arrowReader.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowReader.GetNextArrowRecordBatch() {
		if err != nil {
			return err
		}
	}
	return nil
}

func benchmark(b *testing.B, read readFunction) {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		b.Error(err)
	}
	defer db.Close()

	query := "SELECT * FROM t"

	for i := 0; i < b.N; i++ {
		err = read(db, query)
		if err != nil {
			b.Error(err)
		}
	}
}

func test(t *testing.T, read readFunction) {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	query := "SELECT * FROM t"

	err = read(db, query)
	if err != nil {
		t.Error(err)
	}
}

func TestMain(m *testing.M) {
	db, err := sql.Open("mysql", DSN_BASE)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()

	err = beforeAllCreateTable(db)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer afterAllDropTable(db)

	code := m.Run()
	os.Exit(code)
}

func BenchmarkReadRegular(b *testing.B) {
	benchmark(b, readRegular)
}

func BenchmarkReadParallel(b *testing.B) {
	benchmark(b, readParallel)
}

func BenchmarkReadArrow(b *testing.B) {
	benchmark(b, readArrow)
}

func BenchmarkReadArrowParallel(b *testing.B) {
	benchmark(b, readArrowParallel)
}

func BenchmarkReadArrowServer(b *testing.B) {
	benchmark(b, readArrowServer)
}

func TestReadArrow(t *testing.T) {
	test(t, readArrow)
}

func TestReadArrowParallel(t *testing.T) {
	test(t, readArrowParallel)
}

func TestReadArrowServer(t *testing.T) {
	test(t, readArrowServer)
}
