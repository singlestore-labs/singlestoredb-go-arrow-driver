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

func read(conn *sql.DB, query string) error {
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
	partitions, err := getPartitionsCount(context.Background(), conn, "db", false)
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
		Conn:               conn,
		Query:              query,
		EnableDebugLogging: true,
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
			DatabaseName: "db",
		},
		EnableDebugLogging: true,
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

type readFunction func(*sql.DB, string) error

func benchmark(b *testing.B, read readFunction) {
	db, err := sql.Open("mysql", "root:1@tcp(127.0.0.1:5506)/db")
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
	db, err := sql.Open("mysql", "root:1@tcp(127.0.0.1:5506)/db")
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

func createTable(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS t")
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

func dropTable(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS t")
	return err
}

func TestMain(m *testing.M) {
	db, err := sql.Open("mysql", "root:1@tcp(127.0.0.1:5506)/db")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()

	err = createTable(db)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer dropTable(db)

	code := m.Run()
	os.Exit(code)
}

func BenchmarkRead(b *testing.B) {
	benchmark(b, read)
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

func TestReadArrowParallel(t *testing.T) {
	test(t, readArrowParallel)
}
