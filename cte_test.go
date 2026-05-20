package tsq

import (
	"strings"
	"testing"
)

func TestCTERejectsInvalidInputs(t *testing.T) {
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)

	tests := []struct {
		name string
		cte  Table
	}{
		{
			name: "empty name",
			cte:  CTE("", Select(id).From(id.Table())),
		},
		{
			name: "missing query core",
			cte:  CTE("users_cte", &QueryBuilder[Table]{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTableInput(tt.cte, "cte")
			if err == nil {
				t.Fatal("expected invalid cte input to return an error")
			}
		})
	}
}

func TestCTETableMetadataAndCols(t *testing.T) {
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	name := newColForTable[Table, string](users, "name", "name", nil)

	activeUsers := CTE(" active_users ", Select(id, name).From(id.Table()))

	if got := activeUsers.Table(); got != "active_users" {
		t.Fatalf("expected trimmed cte name active_users, got %q", got)
	}

	if got := activeUsers.(interface{ PhysicalTable() string }).PhysicalTable(); got != "active_users" {
		t.Fatalf("expected cte physical table active_users, got %q", got)
	}

	if got := activeUsers.PrimaryKeys(); got != nil {
		t.Fatalf("expected cte primary keys to be nil, got %#v", got)
	}

	if activeUsers.AutoIncrement() {
		t.Fatal("expected cte table to be non-autoincrement")
	}

	if got := activeUsers.VersionColumn(); got != "" {
		t.Fatalf("expected cte version column to be empty, got %q", got)
	}

	if got := activeUsers.SearchColumns(); got != nil {
		t.Fatalf("expected cte search columns to be nil, got %#v", got)
	}

	cols := activeUsers.Cols()
	if len(cols) != 2 {
		t.Fatalf("expected 2 rebound cte columns, got %d", len(cols))
	}

	if got := cols[0].QualifiedName(); got != `"active_users"."id"` {
		t.Fatalf("expected rebound id column, got %q", got)
	}

	if got := cols[1].QualifiedName(); got != `"active_users"."name"` {
		t.Fatalf("expected rebound name column, got %q", got)
	}
}

func TestCTEPropagatesDefinitionBuildErrors(t *testing.T) {
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	name := newColForTable[Table, string](users, "name", "name", nil)

	searchUsers := CTE("search_users", Select(id, name).From(id.Table()).Search(name))

	searchUserID := id.WithTable(searchUsers)

	_, err := Select(searchUserID).From(searchUsers).Build()
	if err == nil {
		t.Fatal("expected keyword-search cte definition to fail validation")
	}

	if !strings.Contains(err.Error(), "does not support keyword search") {
		t.Fatalf("unexpected error: %v", err)
	}
}
