package integration_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/examples/01-getting-started/todo"
	"github.com/tmoeish/tsq/v5/examples/shop"
	"github.com/tmoeish/tsq/v5/internal/integration/academy"
)

// TestIntegrationGeneratedSQLFilesBuildTheRuntimesSchema runs the SQL file tsq
// gen wrote for each engine, as committed, and expects the runtime to find the
// schema as its tables declare it. The file's first section is the schema at the
// first generation and later sections are the migrations since, so a change in
// how TSQ spells a column (a UTC default, a range constraint) that no model change
// accompanied left the committed file building a schema the runtime no longer
// accepted, and gen --check could not see it: it compares the model, not the
// rendering. The fixture's state is reset when that happens (see the dev skill).
func TestIntegrationGeneratedSQLFilesBuildTheRuntimesSchema(t *testing.T) {
	ctx := context.Background()

	for _, pkg := range []struct {
		dir    string
		tables []tsq.Table
	}{
		{filepath.Join("..", "..", "examples", "01-getting-started", "todo"), todo.TSQTables()},
		{filepath.Join("..", "..", "examples", "shop"), shop.TSQTables()},
		{"academy", academy.TSQTables()},
	} {
		for _, target := range integrationTargets(t) {
			t.Run(filepath.Base(pkg.dir)+"/"+target.name, func(t *testing.T) {
				file := map[string]string{"sqlite": "sqlite.sql", "mysql": "mysql.sql", "postgres": "postgres.sql"}[target.name]

				ddl, err := os.ReadFile(filepath.Join(pkg.dir, file))
				if err != nil {
					t.Fatal(err)
				}

				names := make([]string, 0, len(pkg.tables))
				for _, table := range pkg.tables {
					names = append(names, table.TableName())
				}

				dropTables(t, target, names...)

				db, err := sql.Open(target.driver, target.dsn)
				if err != nil {
					t.Fatal(err)
				}

				defer func() { _ = db.Close() }()

				// One session, as psql or a migration tool runs a file: a migration
				// section's BEGIN and COMMIT must reach the same connection, and the
				// pool would hand out another, or drop one left inside a transaction.
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}

				defer func() { _ = conn.Close() }()

				for _, statement := range sqlStatements(string(ddl)) {
					if _, err := conn.ExecContext(ctx, statement); err != nil {
						t.Fatalf("a statement of %s failed on %s: %v\n%s", file, target.name, err, statement)
					}
				}

				for _, policy := range []tsq.SchemaPolicy{tsq.SchemaPolicyValidate, tsq.SchemaPolicyReconcile} {
					rt, ran, err := openQuietly(target, policy, pkg.tables...)
					if err != nil {
						t.Fatalf("%s over the schema %s built: %v", policy, file, err)
					}

					_ = rt.Close()

					if len(ran) != 0 {
						t.Fatalf("%s ran DDL over the schema %s built:\n  %s", policy, file, strings.Join(ran, "\n  "))
					}
				}

				dropTables(t, target, names...)
			})
		}
	}
}

// sqlStatements splits a generated DDL file into its statements, comments left out.
func sqlStatements(file string) []string {
	var statements []string

	for part := range strings.SplitSeq(file, ";\n") {
		var lines []string

		for line := range strings.SplitSeq(part, "\n") {
			if trimmed := strings.TrimSpace(line); trimmed != "" && !strings.HasPrefix(trimmed, "--") {
				lines = append(lines, line)
			}
		}

		if statement := strings.TrimSpace(strings.Join(lines, "\n")); statement != "" {
			statements = append(statements, statement)
		}
	}

	return statements
}
