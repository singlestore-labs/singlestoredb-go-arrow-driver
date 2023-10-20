package s2db_arrow_driver

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/array"

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

var CREATE_TEST_TABLE = false
var DROP_TEST_TABLE = false
var PROCESS_READ_RESULTS = true
var TEST_PARALLEL = false

type readFunction func(*sql.DB, string) error

func beforeAllCreateTable(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", DB))
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("USE %s", DB))
	if err != nil {
		return err
	}
	_, err = db.Exec("DROP TABLE IF EXISTS t")
	if err != nil {
		return err
	}
	_, err = db.Exec("CREATE TABLE t(a BIGINT PRIMARY KEY, b DOUBLE, t TEXT)")
	if err != nil {
		return err
	}

	query := "INSERT INTO t VALUES "
	for i := 0; i < 1000000; i++ {
		if i%1000 == 999 {
			query += fmt.Sprintf("(%d, 100.1001, 'asdasdasdasd')", i)
			_, err = db.Exec(query)
			if err != nil {
				return err
			}

			query = "INSERT INTO t VALUES "
		} else {
			query += fmt.Sprintf("(%d, 100.1001, 'asdasdasdasd'), ", i)
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

	sumInt := int64(0)
	sumDouble := float64(0)

	for rows.Next() {
		if err = rows.Scan(a, b, c); err != nil {
			return err
		}
		if PROCESS_READ_RESULTS {
			sumInt += *a
			sumDouble += *b.Get()
		}
	}
	fmt.Printf("readRegular sumInt: %d, sumDouble: %f\n", sumInt, sumDouble)

	return nil
}

func readParallelRegular(conn *sql.DB, query string) error {
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
func readArrowGen(conn *sql.DB, query string, conf S2DBArrowReaderConfig, readType string) error {
	arrowReader, err := NewS2DBArrowReader(context.Background(), conf)
	if err != nil {
		return err
	}
	defer arrowReader.Close()

	sumInt := int64(0)
	sumFloat := float64(0)
	nBatches := 0

	for batch, err := arrowReader.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowReader.GetNextArrowRecordBatch() {
		nBatches += 1
		if err != nil {
			return err
		}
		if sumInt == 0 {
			fmt.Printf("%s schema: %v\n", readType, batch.Schema())
		}
		arrInt := batch.Column(0).(*array.Int64)
		arrDouble := batch.Column(1).(*array.Float64)

		for i := 0; i < arrInt.Len(); i++ {
			sumInt += arrInt.Value(i)
			sumFloat += arrDouble.Value(i)
		}
	}
	fmt.Printf("%s sum int: %d, sumFloat: %f, nBatches: %d\n", readType, sumInt, sumFloat, nBatches)

	return nil
}

func readArrow(conn *sql.DB, query string) error {
	return readArrowGen(
		conn,
		query,
		S2DBArrowReaderConfig{
			Conn:  conn,
			Query: query,
			RecordSize: 1000,
			UseClientConvesion: true,
		},
		"Read Arrow, convert on Client")
}

func readArrowParallel(conn *sql.DB, query string) error {
	return readArrowGen(
		conn,
		query,
		S2DBArrowReaderConfig{
			Conn:  conn,
			Query: query,
			RecordSize: 1000,
			ParallelReadConfig: &S2DBParallelReadConfig{
				DatabaseName: DB,
			},
			UseClientConvesion: true,
		},
		"Read Arrow in Parallel, convert on Client")
}

func readArrowServer(conn *sql.DB, query string) error {
	return readArrowGen(
		conn,
		query,
		S2DBArrowReaderConfig{
			Conn:  conn,
			Query: query,
			RecordSize: 1000,
			UseClientConvesion: false,
		},
		"Read Arrow, convert on Server")
}

func readArrowServerParallel(conn *sql.DB, query string) error {
	return readArrowGen(
		conn,
		query,
		S2DBArrowReaderConfig{
			Conn:  conn,
			Query: query,
			RecordSize: 1000,
			ParallelReadConfig: &S2DBParallelReadConfig{
				DatabaseName: DB,
			},
			UseClientConvesion: false,
		},
		"Read Arrow in Parallel, convert on Server")
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
	if CREATE_TEST_TABLE { 
		err = beforeAllCreateTable(db)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	if DROP_TEST_TABLE {
		defer afterAllDropTable(db)
	}
	code := m.Run()
	os.Exit(code)
}

func BenchmarkReadRegular(b *testing.B) {
	benchmark(b, readRegular)
}

func BenchmarkReadParallel(b *testing.B) {
	if TEST_PARALLEL {
		benchmark(b, readParallelRegular)
	}
}

func BenchmarkReadArrow(b *testing.B) {
	benchmark(b, readArrow)
}

func BenchmarkReadArrowParallel(b *testing.B) {
	if TEST_PARALLEL {
		benchmark(b, readArrowParallel)
	}
}

func BenchmarkReadArrowServer(b *testing.B) {
	benchmark(b, readArrowServer)
}

func BenchmarkReadArrowServerParallel(b *testing.B) {
	if TEST_PARALLEL {
		benchmark(b, readArrowServerParallel)
	}
}

func TestReadArrow(t *testing.T) {
	test(t, readArrow)
}

func TestReadArrowParallel(t *testing.T) {
	if TEST_PARALLEL {
		test(t, readArrowParallel)
	}
}

func TestReadArrowServer(t *testing.T) {
	test(t, readArrowServer)
}

func TestReadArrowServerParallel(t *testing.T) {
	if TEST_PARALLEL {
		test(t, readArrowServerParallel)
	}
}
