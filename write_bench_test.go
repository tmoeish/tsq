package tsq

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// countingExecutor accepts statements without a database, so a benchmark measures
// what TSQ does rather than what SQLite does.
type countingExecutor struct {
	statements int
}

func (e *countingExecutor) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("no database")
}

func (e *countingExecutor) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }

func (e *countingExecutor) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	e.statements++

	return driverResult{}, nil
}

type driverResult struct{}

func (driverResult) LastInsertId() (int64, error) { return 0, nil }
func (driverResult) RowsAffected() (int64, error) { return 0, nil }

// BenchmarkBatchInsertStatement measures building one INSERT of 100 rows: the
// column values are read through the generated accessors, not reflection.
func BenchmarkBatchInsertStatement(b *testing.B) {
	ctx := context.Background()
	db := WrapExecutor(&countingExecutor{}, onSQLite)

	rows := make([]*order, 0, 100)
	for i := range 100 {
		rows = append(rows, &order{ID: int64(i + 1), UserID: 7, Amount: int64(i), Note: "note"})
	}

	for b.Loop() {
		if err := Orders.BatchInsert(ctx, db, rows); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBatchUpdateStatement measures the CASE-per-column form, which binds two
// values per column per row.
func BenchmarkBatchUpdateStatement(b *testing.B) {
	ctx := context.Background()
	db := WrapExecutor(&countingExecutor{}, onSQLite)

	rows := make([]*order, 0, 100)
	for i := range 100 {
		rows = append(rows, &order{ID: int64(i + 1), UserID: 7, Amount: int64(i), Note: "note"})
	}

	for b.Loop() {
		if err := Orders.BatchUpdate(ctx, db, rows); err != nil {
			b.Fatal(err)
		}
	}
}
