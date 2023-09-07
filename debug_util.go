package s2db_arrow_driver

import (
	"context"
	"database/sql"
	"fmt"
)

func logQueryExecution(loggingEnabled bool, query string, args ...interface{}) {
	if loggingEnabled {
		fmt.Printf("Executing query (query: '%s', args: '%v')\n", query, args)
	}
}

func profileQuery(loggingEnabled bool, ctx context.Context, conn *sql.Conn, query string) {
	if !loggingEnabled {
		return
	}

	execute := func(query string) error {
		logQueryExecution(loggingEnabled, query)
		_, err := conn.ExecContext(ctx, query)
		if err != nil {
			fmt.Printf("Failed to perform profiling (%s)\n", err)
			return err
		}

		return nil
	}

	err := execute(fmt.Sprintf("CREATE TEMPORARY TABLE temp AS SELECT * FROM (%s) LIMIT 0", query))
	if err != nil {
		return
	}
	defer execute("DROP TEMPORARY TABLE temp")
	execute("SET SESSION profile_for_debug=1")
	if err != nil {
		return
	}
	defer execute("SET SESSION profile_for_debug=0")
	execute(fmt.Sprintf("PROFILE INSERT INTO temp SELECT * FROM (%s)", query))
	if err != nil {
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
	rows.Scan(&profile)
	if err != nil {
		fmt.Printf("Failed to perform profiling (%s)\n", err)
		return
	}

	fmt.Printf("Profiling result:\n%s\n", profile)
}
