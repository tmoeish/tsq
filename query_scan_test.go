package tsq

import (
	"context"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestListValidatesScanDestEvenWhenResultIsEmpty(t *testing.T) {
	db := newScanValidationEngine(t)
	col := newColForTable[scanDestUser, string](newMockTable("users"), "name", "name", nil)
	query := &Query[scanDestUser]{cntSQL: "SELECT COUNT(1) FROM users", listSQL: "SELECT name FROM users WHERE 1 = 0", selectCols: []BoundColumn[scanDestUser]{col}}
	_, err := List[scanDestUser](context.Background(), db, query)
	if err == nil {
		t.Fatal("expected invalid scan destination to fail before returning an empty list")
	}
	if !strings.Contains(err.Error(), "field pointer is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListSupportsInVarSlices(t *testing.T) {
	db := newInVarEngine(t)
	users := newMockTable("users")
	idCol := newColForTable[inVarUser, int64](users, "id", "id", toScanPointer(func(holder *inVarUser) *int64 {
		return &holder.ID
	}))
	nameCol := newColForTable[inVarUser, string](users, "name", "name", toScanPointer(func(holder *inVarUser) *string {
		return &holder.Name
	}))
	query := mustBuild(Select(idCol, nameCol).From(idCol.Table()).Where(idCol.InVar()))
	rows, err := List[inVarUser](context.Background(), db, query, []int64{1, 3})
	if err != nil {
		t.Fatalf("expected InVar query to execute, got %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].ID != 1 || rows[1].ID != 3 {
		t.Fatalf("unexpected rows returned: %#v", rows)
	}
	count, err := query.Count(context.Background(), db, []int64{1, 3})
	if err != nil {
		t.Fatalf("expected InVar count query to execute, got %v", err)
	}
	if count != 2 {
		t.Fatalf("expected InVar count query to return 2, got %d", count)
	}
}

func TestPageValidatesScanDestEvenWhenResultIsEmpty(t *testing.T) {
	db := newScanValidationEngine(t)
	col := newColForTable[scanDestUser, string](newMockTable("users"), "name", "name", nil)
	query := &Query[scanDestUser]{cntSQL: "SELECT COUNT(1) FROM users", listSQL: "SELECT name FROM users WHERE 1 = 0", selectCols: []BoundColumn[scanDestUser]{col}}
	_, err := Page[scanDestUser](context.Background(), db, nil, query)
	if err == nil {
		t.Fatal("expected invalid scan destination to fail before returning an empty page")
	}
	if !strings.Contains(err.Error(), "field pointer is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNilFieldPointer(t *testing.T) {
	col := newColForTable[scanDestUser, string](newMockTable("users"), "name", "name", nil)
	_, err := buildScanDest([]BoundColumn[scanDestUser]{col}, &scanDestUser{})
	if err == nil {
		t.Fatal("expected nil field pointer to return an error")
	}
	if !strings.Contains(err.Error(), "field pointer is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRecoversFieldPointerPanics(t *testing.T) {
	col := newColForTable[scanDestUser, string](newMockTable("users"), "name", "name", toScanPointer(func(holder *scanDestUser) *string {
		return &holder.Name
	}))
	_, err := invokeFieldPointer(col.FieldPointer(), &queryOwner{})
	if err == nil {
		t.Fatal("expected field pointer panic to return an error")
	}
	if !strings.Contains(err.Error(), "field pointer panicked") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNilScanTarget(t *testing.T) {
	col := newColForTable[scanDestUser, string](newMockTable("users"), "name", "name", toScanPointer(func(holder *scanDestUser) *string {
		return nil
	}))
	_, err := buildScanDest([]BoundColumn[scanDestUser]{col}, &scanDestUser{})
	if err == nil {
		t.Fatal("expected nil scan target to return an error")
	}
	if !strings.Contains(err.Error(), "returned nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNilHolder(t *testing.T) {
	col := newColForTable[scanDestUser, string](newMockTable("users"), "name", "name", toScanPointer(func(holder *scanDestUser) *string {
		return &holder.Name
	}))
	_, err := buildScanDest([]BoundColumn[scanDestUser]{col}, nil)
	if err == nil {
		t.Fatal("expected nil holder to return an error")
	}
	if !strings.Contains(err.Error(), "scan holder cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNonPointerHolder(t *testing.T) {
	err := validateScanHolder(scanDestUser{})
	if err == nil {
		t.Fatal("expected non-pointer holder to return an error")
	}
	if !strings.Contains(err.Error(), "scan holder must be a pointer") {
		t.Fatalf("unexpected error: %v", err)
	}
}
