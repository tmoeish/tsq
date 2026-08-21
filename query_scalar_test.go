package tsq

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

type scalarName string

func TestQueryScalarInfersSelectedColumnType(t *testing.T) {
	db := newScanValidationEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), `INSERT INTO users (name) VALUES ('alice')`); err != nil {
		t.Fatalf("seed scalar row: %v", err)
	}

	users := newMockTable("users")
	name := newColForTable[scanDestUser, scalarName](users, "name", "name", nil)
	query := mustBuild(Select(name).From(users))

	got, err := query.Scalar(context.Background(), db, name)
	if err != nil {
		t.Fatalf("Scalar() error = %v", err)
	}
	if got != scalarName("alice") {
		t.Fatalf("Scalar() = %q, want alice", got)
	}
}

func TestQueryScalarMapsNullToZeroValue(t *testing.T) {
	db := newScanValidationEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), `INSERT INTO users (name) VALUES (NULL)`); err != nil {
		t.Fatalf("seed NULL scalar row: %v", err)
	}

	users := newMockTable("users")
	name := newColForTable[scanDestUser, string](users, "name", "name", nil)
	query := mustBuild(Select(name).From(users))

	got, err := query.Scalar(context.Background(), db, name)
	if err != nil {
		t.Fatalf("Scalar() error = %v", err)
	}
	if got != "" {
		t.Fatalf("Scalar() = %q, want the string zero value", got)
	}
}

func TestQueryScalarRejectsMismatchedSelection(t *testing.T) {
	db := newScanValidationEngine(t)
	users := newMockTable("users")
	name := newColForTable[scanDestUser, string](users, "name", "name", nil)
	alias := newColForTable[scanDestUser, string](users, "alias", "alias", nil)
	query := mustBuild(Select(name).From(users))

	_, err := query.Scalar(context.Background(), db, alias)
	if err == nil {
		t.Fatal("expected a mismatched selected column error")
	}
	if !strings.Contains(err.Error(), `scalar query selected "users"."name"`) {
		t.Fatalf("unexpected mismatch error: %v", err)
	}
}

func TestQueryScalarRejectsMultipleColumns(t *testing.T) {
	db := newScanValidationEngine(t)
	users := newMockTable("users")
	name := newColForTable[scanDestUser, string](users, "name", "name", nil)
	alias := newColForTable[scanDestUser, string](users, "alias", "alias", nil)
	query := mustBuild(Select(name, alias).From(users))

	_, err := query.Scalar(context.Background(), db, name)
	if err == nil {
		t.Fatal("expected a multiple-column scalar error")
	}
	if !strings.Contains(err.Error(), "scalar query must select exactly one column") {
		t.Fatalf("unexpected multiple-column error: %v", err)
	}
}

func TestQueryCountRejectsUnbuiltQuery(t *testing.T) {
	_, err := (&Query[queryOwner]{}).Count(context.Background(), nil)
	if err == nil {
		t.Fatal("expected unbuilt query to return an error")
	}
	if !strings.Contains(err.Error(), "query is not built") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryCountRejectsTypedNilExecutor(t *testing.T) {
	var db *sql.DB
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	query := mustBuild(Select(userID).From(userID.Table()))
	_, err := query.Count(context.Background(), db)
	if err == nil {
		t.Fatal("expected typed-nil executor to return an error")
	}
	if !strings.Contains(err.Error(), "sql executor cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryCountRejectsExecutorWithoutDialectForRenderedSQL(t *testing.T) {
	db := newEngineWithoutDialect(t)
	users := newMockTable("users")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	query := mustBuild(Select(userID).From(userID.Table()))
	_, err := query.Count(context.Background(), db)
	if err == nil {
		t.Fatal("expected executor without dialect to return an error")
	}
	if !strings.Contains(err.Error(), "dialect cannot be determined") {
		t.Fatalf("unexpected error: %v", err)
	}
}
