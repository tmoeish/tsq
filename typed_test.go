package tsq

import "testing"

type (
	typedUserOwner  struct{}
	typedOrderOwner struct{}
)

func (typedUserOwner) TSQOwner()              {}
func (typedUserOwner) Table() string          { return "users" }
func (typedUserOwner) Cols() []SQLColumn      { return nil }
func (typedUserOwner) KwList() []SearchColumn { return nil }
func (typedUserOwner) PrimaryKeys() []string  { return nil }
func (typedUserOwner) AutoIncrement() bool    { return false }
func (typedUserOwner) VersionColumn() string  { return "" }

func (typedOrderOwner) TSQOwner()              {}
func (typedOrderOwner) Table() string          { return "orders" }
func (typedOrderOwner) Cols() []SQLColumn      { return nil }
func (typedOrderOwner) KwList() []SearchColumn { return nil }
func (typedOrderOwner) PrimaryKeys() []string  { return nil }
func (typedOrderOwner) AutoIncrement() bool    { return false }
func (typedOrderOwner) VersionColumn() string  { return "" }

func TestTableColumnsConvertToErasedColumns(t *testing.T) {
	userID := NewCol[typedUserOwner, int]("id", "id", nil)
	userName := NewCol[typedUserOwner, string]("name", "name", nil)

	cols := []SQLColumn{userID, userName}
	if len(cols) != 2 {
		t.Fatalf("expected 2 table columns, got %d", len(cols))
	}
	if cols[0].QualifiedName() != `"users"."id"` {
		t.Fatalf("unexpected first column: %s", cols[0].QualifiedName())
	}
	if cols[1].QualifiedName() != `"users"."name"` {
		t.Fatalf("unexpected second column: %s", cols[1].QualifiedName())
	}
}

func TestSelectReturnsTypedQueryBuilder(t *testing.T) {
	userID := NewCol[typedUserOwner, int]("id", "id", nil)
	userName := NewCol[typedUserOwner, string]("name", "name", nil)

	var qb *QueryBuilder[typedUserOwner] = Select[typedUserOwner](userID, userName).From(typedUserOwner{})
	if qb == nil {
		t.Fatal("expected typed query builder")
	}
}
