package tsq

import (
	"errors"
	"testing"
)

type buildErrorTable struct {
	name string
	err  error
}

func (buildErrorTable) TSQOwner() {}

func (t buildErrorTable) Table() string               { return t.name }
func (buildErrorTable) Cols() []SQLColumn             { return nil }
func (buildErrorTable) SearchColumns() []SearchColumn { return nil }
func (buildErrorTable) PrimaryKeys() []string         { return nil }
func (buildErrorTable) AutoIncrement() bool           { return false }
func (buildErrorTable) VersionColumn() string         { return "" }
func (t buildErrorTable) buildError() error           { return t.err }

type referencedTablesOnlyColumn struct {
	refs map[string]Table
}

func (*referencedTablesOnlyColumn) SQLExpr() string          { return "expr" }
func (*referencedTablesOnlyColumn) OutputName() string       { return "expr" }
func (*referencedTablesOnlyColumn) JSONFieldName() string    { return "expr" }
func (*referencedTablesOnlyColumn) Table() Table             { return nil }
func (*referencedTablesOnlyColumn) Name() string             { return "expr" }
func (*referencedTablesOnlyColumn) QualifiedName() string    { return "expr" }
func (*referencedTablesOnlyColumn) scanPointer() scanPointer { return nil }
func (c *referencedTablesOnlyColumn) referencedTables() map[string]Table {
	return c.refs
}

func TestValidateTableInputRejectsNilEmptyAndBuildError(t *testing.T) {
	buildErr := errors.New("boom")

	tests := []struct {
		name  string
		table Table
	}{
		{name: "nil table", table: nil},
		{name: "empty name", table: buildErrorTable{}},
		{name: "build error", table: buildErrorTable{name: "users", err: buildErr}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTableInput(tt.table, "table")
			if err == nil {
				t.Fatal("expected validateTableInput to return an error")
			}
		})
	}
}

func TestColumnPrimaryTableFallsBackToSortedReferencedTables(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")
	col := &referencedTablesOnlyColumn{
		refs: map[string]Table{
			"users": users,
			"orgs":  orgs,
		},
	}

	got := columnPrimaryTable(col)
	if got == nil {
		t.Fatal("expected referenced tables fallback to return a table")
	}

	if got.Table() != "orgs" {
		t.Fatalf("expected alphabetical fallback table orgs, got %q", got.Table())
	}
}
