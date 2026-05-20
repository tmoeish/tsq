package tsq

import "testing"

func TestQuerySpecTablesForColumnsIncludeReferencedTables(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")
	userName := newColForTable[Table, string](users, "name", "name", nil)
	orgName := newColForTable[Table, string](orgs, "name", "name", nil)
	expr := userName.Exprf("COALESCE(%s, %s)", orgName)
	tables := (querySpec[Table]{}).tablesForColumns([]SQLColumn{expr})
	if len(tables) != 2 {
		t.Fatalf("expected transformed column to surface two referenced tables, got %#v", tables)
	}
	if tables["users"] == nil || tables["orgs"] == nil {
		t.Fatalf("expected users and orgs tables in transformed column references, got %#v", tables)
	}
}
