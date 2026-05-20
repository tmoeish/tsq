package tsq

import "testing"

type aliasTestTable struct {
	name          string
	cols          []SQLColumn
	searchCols    []SearchColumn
	primaryKeys   []string
	autoIncrement bool
	versionColumn string
}

func (*aliasTestTable) TSQOwner() {}

func (t *aliasTestTable) Table() string { return t.name }

func (t *aliasTestTable) Cols() []SQLColumn {
	return append([]SQLColumn(nil), t.cols...)
}

func (t *aliasTestTable) SearchColumns() []SearchColumn {
	return append([]SearchColumn(nil), t.searchCols...)
}

func (t *aliasTestTable) PrimaryKeys() []string {
	return append([]string(nil), t.primaryKeys...)
}

func (t *aliasTestTable) AutoIncrement() bool { return t.autoIncrement }

func (t *aliasTestTable) VersionColumn() string { return t.versionColumn }

type fixedSQLColumn struct {
	table Table
}

func (*fixedSQLColumn) SQLExpr() string          { return "1" }
func (*fixedSQLColumn) OutputName() string       { return "literal" }
func (*fixedSQLColumn) JSONFieldName() string    { return "literal" }
func (c *fixedSQLColumn) Table() Table           { return c.table }
func (*fixedSQLColumn) Name() string             { return "" }
func (*fixedSQLColumn) QualifiedName() string    { return "1" }
func (*fixedSQLColumn) scanPointer() scanPointer { return nil }
func (c *fixedSQLColumn) referencedTables() map[string]Table {
	return map[string]Table{c.table.Table(): c.table}
}

func TestAliasTableReturnsOriginalForNilBlankOrSameAlias(t *testing.T) {
	base := newMockTable("users")

	if got := AliasTable(nil, "u"); got != nil {
		t.Fatalf("expected nil base table to stay nil, got %#v", got)
	}

	if got := AliasTable(base, "   "); got != base {
		t.Fatal("expected blank alias to return original table")
	}

	if got := AliasTable(base, "users"); got != base {
		t.Fatal("expected same logical alias to return original table")
	}
}

func TestAliasTableRebindsColumnsAndPreservesMetadata(t *testing.T) {
	base := &aliasTestTable{
		name:          "users",
		primaryKeys:   []string{"id"},
		autoIncrement: true,
		versionColumn: "version",
	}
	id := newColForTable[Table, int](base, "id", "id", nil)
	name := newColForTable[Table, string](base, "name", "name", nil)
	base.cols = []SQLColumn{id, name}
	base.searchCols = []SearchColumn{name}

	aliased := AliasTable(base, " manager ")

	if got := aliased.Table(); got != "manager" {
		t.Fatalf("expected alias table name manager, got %q", got)
	}

	if got := physicalTableName(aliased); got != "users" {
		t.Fatalf("expected aliased physical table users, got %q", got)
	}

	if got := tableAliasName(aliased); got != "manager" {
		t.Fatalf("expected alias name manager, got %q", got)
	}

	cols := aliased.Cols()
	if len(cols) != 2 {
		t.Fatalf("expected 2 aliased columns, got %d", len(cols))
	}

	if got := cols[0].QualifiedName(); got != `"manager"."id"` {
		t.Fatalf("expected aliased id qualified name, got %q", got)
	}

	if got := cols[1].QualifiedName(); got != `"manager"."name"` {
		t.Fatalf("expected aliased name qualified name, got %q", got)
	}

	searchCols := aliased.SearchColumns()
	if len(searchCols) != 1 {
		t.Fatalf("expected 1 aliased search column, got %d", len(searchCols))
	}

	if got := searchCols[0].QualifiedName(); got != `"manager"."name"` {
		t.Fatalf("expected aliased search column qualified name, got %q", got)
	}

	keys := aliased.PrimaryKeys()
	keys[0] = "mutated"
	if got := base.PrimaryKeys()[0]; got != "id" {
		t.Fatalf("expected primary keys to be copied defensively, got %q", got)
	}

	if !aliased.AutoIncrement() {
		t.Fatal("expected autoincrement flag to be preserved")
	}

	if got := aliased.VersionColumn(); got != "version" {
		t.Fatalf("expected version column to be preserved, got %q", got)
	}
}

func TestRebindColumnLeavesUnsupportedColumnsUntouched(t *testing.T) {
	base := newMockTable("users")
	aliased := AliasTable(base, "u")
	col := &fixedSQLColumn{table: base}

	got := RebindColumn(col, aliased)
	if got != col {
		t.Fatal("expected unsupported column rebinding to return original column")
	}
}
