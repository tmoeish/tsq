package tsq

import (
	"context"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

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
	var db *Engine
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
