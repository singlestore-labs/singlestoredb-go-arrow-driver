package s2db_arrow_driver

import (
	"context"
	"database/sql"
	"fmt"
)

type executable interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type queriable interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func execContext(ctx context.Context, conn executable, query string, loggingEnabled bool, args ...interface{}) (sql.Result, error) {
	if loggingEnabled {
		fmt.Printf("Executing query (query: '%s', args: '%v')\n", query, args)
	}
	return conn.ExecContext(ctx, query, args...)
}

func queryContext(ctx context.Context, conn queriable, query string, loggingEnabled bool, args ...interface{}) (*sql.Rows, error) {
	if loggingEnabled {
		fmt.Printf("Executing query (query: '%s', args: '%v')\n", query, args)
	}
	return conn.QueryContext(ctx, query, args...)
}

func profileQuery(loggingEnabled bool, ctx context.Context, conn *sql.Conn, query string, args ...interface{}) {
	if !loggingEnabled {
		return
	}

	execute := func(query string, args ...interface{}) error {
		_, err := execContext(ctx, conn, query, loggingEnabled)
		return err
	}

	err := execute(fmt.Sprintf("CREATE TEMPORARY TABLE temp AS SELECT * FROM (%s) LIMIT 0", query), args...)
	if err != nil {
		fmt.Printf("Failed to perform profiling (%s)\n", err)
		return
	}
	defer execute("DROP TEMPORARY TABLE temp")

	err = execute("SET SESSION profile_for_debug=1")
	if err != nil {
		fmt.Printf("Failed to perform profiling (%s)\n", err)
		return
	}
	defer execute("SET SESSION profile_for_debug=0")

	err = execute(fmt.Sprintf("PROFILE INSERT INTO temp SELECT * FROM (%s)", query), args...)
	if err != nil {
		fmt.Printf("Failed to perform profiling (%s)\n", err)
		return
	}

	rows, err := conn.QueryContext(ctx, "SHOW PROFILE JSON")
	if err != nil {
		fmt.Printf("Failed to perform profiling (%s)\n", err)
		return
	}
	defer rows.Close()

	if !rows.Next() {
		fmt.Println("Failed to perform profiling (SHOW PROFILE returned empty result)")
		return
	}

	var profile string
	err = rows.Scan(&profile)
	if err != nil {
		fmt.Printf("Failed to perform profiling (%s)\n", err)
		return
	}

	fmt.Printf("Profiling result:\n%s\n", profile)
}
