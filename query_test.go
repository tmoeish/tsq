package tsq

import (
	"strings"
	"testing"
)

func TestErrUnknownSortField(t *testing.T) {
	field := "unknown_field"
	err := NewErrUnknownSortField(field)

	if err.field != field {
		t.Errorf("Expected field '%s', got '%s'", field, err.field)
	}

	expectedMsg := "unknown sort field: unknown_field"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestErrOrderCountMismatch(t *testing.T) {
	orderBys := 3
	orders := 2
	err := NewErrOrderCountMismatch(orderBys, orders)

	if err.orderBys != orderBys {
		t.Errorf("Expected orderBys %d, got %d", orderBys, err.orderBys)
	}

	if err.orders != orders {
		t.Errorf("Expected orders %d, got %d", orders, err.orders)
	}

	expectedMsg := "ORDER BY fields count(3) and ORDER directions count(2) mismatch"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestQuery_SQLAccessors(t *testing.T) {
	query := &Query{
		cntSQL:    "SELECT COUNT(*) FROM users",
		listSQL:   "SELECT * FROM users",
		kwCntSQL:  "SELECT COUNT(*) FROM users WHERE name LIKE ?",
		kwListSQL: "SELECT * FROM users WHERE name LIKE ?",
	}

	if query.CntSQL() != "SELECT COUNT(*) FROM users" {
		t.Errorf("Expected CntSQL 'SELECT COUNT(*) FROM users', got '%s'", query.CntSQL())
	}

	if query.ListSQL() != "SELECT * FROM users" {
		t.Errorf("Expected ListSQL 'SELECT * FROM users', got '%s'", query.ListSQL())
	}

	if query.KwCntSQL() != "SELECT COUNT(*) FROM users WHERE name LIKE ?" {
		t.Errorf("Expected KwCntSQL 'SELECT COUNT(*) FROM users WHERE name LIKE ?', got '%s'", query.KwCntSQL())
	}

	if query.KwListSQL() != "SELECT * FROM users WHERE name LIKE ?" {
		t.Errorf("Expected KwListSQL 'SELECT * FROM users WHERE name LIKE ?', got '%s'", query.KwListSQL())
	}
}

func TestQueryBuilder_Build_EmptySelectFields(t *testing.T) {
	qb := &QueryBuilder{
		selectTables: make(map[string]Table),
	}

	_, err := qb.Build()

	if err == nil {
		t.Error("Expected error for empty select fields")
	}

	expectedErrMsg := "empty select fields"
	if !strings.Contains(err.Error(), expectedErrMsg) {
		t.Errorf("Expected error message to contain '%s', got '%s'", expectedErrMsg, err.Error())
	}
}

func TestQueryBuilder_Build_Success(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "id")

	qb := &QueryBuilder{
		selectCols:   []Column{col},
		selectTables: map[string]Table{"users": table},
	}

	query, err := qb.Build()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if query == nil {
		t.Error("Expected non-nil query")
	}

	// Check that SQL statements are not empty
	if query.CntSQL() == "" {
		t.Error("Expected non-empty CntSQL")
	}

	if query.ListSQL() == "" {
		t.Error("Expected non-empty ListSQL")
	}

	if query.KwCntSQL() == "" {
		t.Error("Expected non-empty KwCntSQL")
	}

	if query.KwListSQL() == "" {
		t.Error("Expected non-empty KwListSQL")
	}
}

func TestQueryBuilder_MustBuild_Success(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "id")

	qb := &QueryBuilder{
		selectCols:   []Column{col},
		selectTables: map[string]Table{"users": table},
	}

	// Should not panic
	query := qb.MustBuild()

	if query == nil {
		t.Error("Expected non-nil query")
	}
}

func TestDefaultBatchInsertOptions(t *testing.T) {
	opts := DefaultBatchInsertOptions()

	if opts == nil {
		t.Fatal("Expected non-nil options")
	}

	if opts.BatchSize != 1000 {
		t.Errorf("Expected BatchSize 1000, got %d", opts.BatchSize)
	}

	if opts.IgnoreErrors != false {
		t.Errorf("Expected IgnoreErrors false, got %v", opts.IgnoreErrors)
	}

	if opts.OnDuplicateKey != false {
		t.Errorf("Expected OnDuplicateKey false, got %v", opts.OnDuplicateKey)
	}
}

func TestBatchInsertOptions_Modification(t *testing.T) {
	opts := DefaultBatchInsertOptions()

	// Modify options
	opts.BatchSize = 500
	opts.IgnoreErrors = true
	opts.OnDuplicateKey = true

	if opts.BatchSize != 500 {
		t.Errorf("Expected BatchSize 500, got %d", opts.BatchSize)
	}

	if opts.IgnoreErrors != true {
		t.Errorf("Expected IgnoreErrors true, got %v", opts.IgnoreErrors)
	}

	if opts.OnDuplicateKey != true {
		t.Errorf("Expected OnDuplicateKey true, got %v", opts.OnDuplicateKey)
	}
}

func TestQuery_MetadataAccess(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "id")

	query := &Query{
		selectCols:   []Column{col},
		selectTables: map[string]Table{"users": table},
		kwCols:       []Column{col},
		kwTables:     map[string]Table{"users": table},
	}

	// Test that metadata is accessible (though we can't test the actual values
	// without more complex setup, we can test that the fields exist)
	if len(query.selectCols) != 1 {
		t.Errorf("Expected 1 select column, got %d", len(query.selectCols))
	}

	if len(query.selectTables) != 1 {
		t.Errorf("Expected 1 select table, got %d", len(query.selectTables))
	}

	if len(query.kwCols) != 1 {
		t.Errorf("Expected 1 keyword column, got %d", len(query.kwCols))
	}

	if len(query.kwTables) != 1 {
		t.Errorf("Expected 1 keyword table, got %d", len(query.kwTables))
	}
}

func TestErrorTypes_Interfaces(t *testing.T) {
	// Test that error types implement error interface
	var _ error = &ErrUnknownSortField{}

	var _ error = &ErrOrderCountMismatch{}

	// Test that they can be created and used
	err1 := NewErrUnknownSortField("test")
	if err1 == nil {
		t.Error("Expected non-nil error")
	}

	err2 := NewErrOrderCountMismatch(1, 2)
	if err2 == nil {
		t.Error("Expected non-nil error")
	}
}

func TestQuery_EmptySQL(t *testing.T) {
	query := &Query{}

	// Test that empty SQL strings are returned correctly
	if query.CntSQL() != "" {
		t.Errorf("Expected empty CntSQL, got '%s'", query.CntSQL())
	}

	if query.ListSQL() != "" {
		t.Errorf("Expected empty ListSQL, got '%s'", query.ListSQL())
	}

	if query.KwCntSQL() != "" {
		t.Errorf("Expected empty KwCntSQL, got '%s'", query.KwCntSQL())
	}

	if query.KwListSQL() != "" {
		t.Errorf("Expected empty KwListSQL, got '%s'", query.KwListSQL())
	}
}
