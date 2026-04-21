package tsq

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gopkg.in/gorp.v2"
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

func TestErrAmbiguousSortField(t *testing.T) {
	field := "id"
	err := NewErrAmbiguousSortField(field)

	if err.field != field {
		t.Errorf("Expected field '%s', got '%s'", field, err.field)
	}

	expectedMsg := "ambiguous sort field: id"
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

func TestQueryBuilder_Build_EmptySelectFieldsWithWhereStillFails(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "id", "id", nil)

	_, err := Select().Where(col.EQVar()).Build()
	if err == nil {
		t.Fatal("expected empty select fields to fail even when conditions add tables")
	}

	if !strings.Contains(err.Error(), "empty select fields") {
		t.Fatalf("expected empty select fields error, got %v", err)
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

func TestQueryBuilder_Build_FullJoinReturnsError(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderUserID := newMockColumn(orders, "user_id")

	_, err := Select(userID).FullJoin(userID, orderUserID).Build()
	if err == nil {
		t.Fatal("expected FULL JOIN build to fail")
	}

	if !strings.Contains(err.Error(), "FULL JOIN") {
		t.Fatalf("expected FULL JOIN error, got %v", err)
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
}

func TestDefaultBatchOptions(t *testing.T) {
	opts := DefaultBatchOptions()

	if opts == nil {
		t.Fatal("expected non-nil options")
	}

	if opts.BatchSize != 1000 {
		t.Fatalf("expected batch size 1000, got %d", opts.BatchSize)
	}
}

func TestBatchInsertOptions_Modification(t *testing.T) {
	opts := DefaultBatchInsertOptions()

	// Modify options
	opts.BatchSize = 500
	opts.IgnoreErrors = true

	if opts.BatchSize != 500 {
		t.Errorf("Expected BatchSize 500, got %d", opts.BatchSize)
	}

	if opts.IgnoreErrors != true {
		t.Errorf("Expected IgnoreErrors true, got %v", opts.IgnoreErrors)
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

func TestNilQuery_SQLAccessorsReturnEmptyStrings(t *testing.T) {
	var query *Query

	if query.CntSQL() != "" {
		t.Errorf("Expected empty CntSQL for nil query, got %q", query.CntSQL())
	}

	if query.ListSQL() != "" {
		t.Errorf("Expected empty ListSQL for nil query, got %q", query.ListSQL())
	}

	if query.KwCntSQL() != "" {
		t.Errorf("Expected empty KwCntSQL for nil query, got %q", query.KwCntSQL())
	}

	if query.KwListSQL() != "" {
		t.Errorf("Expected empty KwListSQL for nil query, got %q", query.KwListSQL())
	}
}

func TestBuildDeleteByIDsSQL(t *testing.T) {
	sqlStr, err := buildDeleteByIDsSQL("users", "id", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := `DELETE FROM "users" WHERE "id" IN (?,?)`
	if got := renderCanonicalSQL(sqlStr); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestBuildDeleteByIDsSQLRejectsInvalidIdentifiers(t *testing.T) {
	_, err := buildDeleteByIDsSQL("users; DROP TABLE users", "id", 1)
	if err == nil {
		t.Fatal("expected invalid table name to return an error")
	}

	_, err = buildDeleteByIDsSQL("users", "id)` OR 1=1 --", 1)
	if err == nil {
		t.Fatal("expected invalid column name to return an error")
	}
}

func TestQuery_buildPageSQLsNormalizesNilRequest(t *testing.T) {
	query := &Query{
		cntSQL:    "SELECT COUNT(*) FROM users",
		listSQL:   "SELECT * FROM users",
		kwCntSQL:  "SELECT COUNT(*) FROM users WHERE name LIKE ?",
		kwListSQL: "SELECT * FROM users WHERE name LIKE ?",
	}

	cntSQL, listSQL, err := query.buildPageSQLs(nil)
	if err != nil {
		t.Fatalf("expected nil page request to be normalized, got error %v", err)
	}

	if cntSQL != "SELECT COUNT(*) FROM users" {
		t.Fatalf("unexpected count SQL: %q", cntSQL)
	}

	if listSQL != "SELECT * FROM users\nLIMIT ? OFFSET ?" {
		t.Fatalf("unexpected list SQL: %q", listSQL)
	}
}

func TestQuery_buildPageSQLsRejectsNilQuery(t *testing.T) {
	var query *Query

	_, _, err := query.buildPageSQLs(nil)
	if err == nil {
		t.Fatal("expected nil query to return an error")
	}

	if !strings.Contains(err.Error(), "query cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQuery_buildPageSQLsRejectsUnbuiltQuery(t *testing.T) {
	query := &Query{}

	_, _, err := query.buildPageSQLs(nil)
	if err == nil {
		t.Fatal("expected unbuilt query to return an error")
	}

	if !strings.Contains(err.Error(), "query is not built") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQuery_buildPageSQLsRejectsAmbiguousSortField(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderID := newMockColumn(orders, "id")

	query := Select(userID, orderID).MustBuild()

	_, _, err := query.buildPageSQLs(&PageReq{
		OrderBy: "id",
		Order:   "ASC",
	})
	if err == nil {
		t.Fatal("expected ambiguous sort field to return an error")
	}

	var ambiguousErr *ErrAmbiguousSortField
	if !errors.As(err, &ambiguousErr) {
		t.Fatalf("expected ErrAmbiguousSortField, got %v", err)
	}
}

func TestQuery_buildPageSQLsDefaultsMissingOrderToASC(t *testing.T) {
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	userName := newMockColumn(users, "name")

	query := Select(userID, userName).MustBuild()

	_, listSQL, err := query.buildPageSQLs(&PageReq{
		OrderBy: "name,id",
	})
	if err != nil {
		t.Fatalf("expected missing order to default to ASC, got %v", err)
	}

	want := `SELECT "users"."id", "users"."name" FROM "users"` +
		"\nORDER BY " + `"users"."name" ASC, "users"."id" ASC` +
		"\nLIMIT ? OFFSET ?"
	if got := renderCanonicalSQL(listSQL); got != want {
		t.Fatalf("expected list SQL %q, got %q", want, got)
	}
}

func TestQuery_buildPageSQLsRejectsExplicitOrderCountMismatch(t *testing.T) {
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	userName := newMockColumn(users, "name")

	query := Select(userID, userName).MustBuild()

	_, _, err := query.buildPageSQLs(&PageReq{
		OrderBy: "name,id",
		Order:   "DESC",
	})
	if err == nil {
		t.Fatal("expected explicit order count mismatch to return an error")
	}

	var mismatchErr *ErrOrderCountMismatch
	if !errors.As(err, &mismatchErr) {
		t.Fatalf("expected ErrOrderCountMismatch, got %v", err)
	}
}

func TestNormalizeBatchInsertOptionsValidatesInputs(t *testing.T) {
	if _, err := normalizeBatchInsertOptions(&BatchInsertOptions{BatchSize: 0}); err == nil {
		t.Fatal("expected zero batch size to return an error")
	}
}

func TestNormalizeBatchOptionsValidatesInputs(t *testing.T) {
	if _, err := normalizeBatchOptions(&BatchOptions{BatchSize: 0}); err == nil {
		t.Fatal("expected zero batch size to return an error")
	}
}

func TestQueryCountRejectsUnbuiltQuery(t *testing.T) {
	_, err := (&Query{}).Count(context.Background(), nil)
	if err == nil {
		t.Fatal("expected unbuilt query to return an error")
	}

	if !strings.Contains(err.Error(), "query is not built") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRenderSQLForDialectPostgres(t *testing.T) {
	users := newMockTable("users")
	userID := NewCol[int](users, "id", "id", nil)

	query := Select(userID).Where(userID.EQVar()).MustBuild()

	got := renderSQLForDialect(query.listSQL, gorp.PostgresDialect{})
	want := `SELECT "users"."id" FROM "users" WHERE "users"."id" = $1`
	if got != want {
		t.Fatalf("expected postgres SQL %q, got %q", want, got)
	}
}

func TestRenderDeleteByIDsSQLForPostgres(t *testing.T) {
	sqlStr, err := buildDeleteByIDsSQL("users", "id", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := renderSQLForDialect(sqlStr, gorp.PostgresDialect{})
	want := `DELETE FROM "users" WHERE "id" IN ($1,$2)`
	if got != want {
		t.Fatalf("expected postgres delete SQL %q, got %q", want, got)
	}
}

func TestBatchUpdateChunkRejectsNilItems(t *testing.T) {
	err := batchUpdateChunk[int](nil, nil, []*int{nil})
	if err == nil {
		t.Fatal("expected nil batch update item to return an error")
	}

	if !strings.Contains(err.Error(), "item at index 0 is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBatchDeleteChunkRejectsNilItems(t *testing.T) {
	err := batchDeleteChunk[int](nil, nil, []*int{nil})
	if err == nil {
		t.Fatal("expected nil batch delete item to return an error")
	}

	if !strings.Contains(err.Error(), "item at index 0 is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPageFnRejectsNilQuery(t *testing.T) {
	_, err := pageFn[int](context.Background(), nil, nil, nil)
	if err == nil {
		t.Fatal("expected nil query to return an error")
	}

	if !strings.Contains(err.Error(), "query cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryCountRejectsTypedNilExecutor(t *testing.T) {
	var db *gorp.DbMap
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	query := Select(userID).MustBuild()

	_, err := query.Count(context.Background(), db)
	if err == nil {
		t.Fatal("expected typed-nil executor to return an error")
	}

	if !strings.Contains(err.Error(), "sql executor cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertRejectsTypedNilExecutor(t *testing.T) {
	var db *gorp.DbMap
	value := 1

	err := Insert(context.Background(), db, &value)
	if err == nil {
		t.Fatal("expected typed-nil executor to return an error")
	}

	if !strings.Contains(err.Error(), "sql executor cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertRejectsNilItem(t *testing.T) {
	db := &gorp.DbMap{}
	var value *int

	err := Insert(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}

	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateRejectsNilItem(t *testing.T) {
	db := &gorp.DbMap{}
	var value *int

	err := Update(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}

	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteRejectsNilItem(t *testing.T) {
	db := &gorp.DbMap{}
	var value *int

	err := Delete(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}

	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBatchInsertRejectsTypedNilExecutor(t *testing.T) {
	var db *gorp.DbMap
	row := mockTable{tableName: "users"}

	err := BatchInsert(context.Background(), db, []*mockTable{&row})
	if err == nil {
		t.Fatal("expected typed-nil executor to return an error")
	}

	if !strings.Contains(err.Error(), "sql executor cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryCountRejectsExecutorWithoutDialectForRenderedSQL(t *testing.T) {
	db := &gorp.DbMap{}
	users := newMockTable("users")
	userID := NewCol[int](users, "id", "id", nil)
	query := Select(userID).MustBuild()

	_, err := query.Count(context.Background(), db)
	if err == nil {
		t.Fatal("expected executor without dialect to return an error")
	}

	if !strings.Contains(err.Error(), "dialect cannot be determined") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBatchDeleteByIDsRejectsExecutorWithoutDialectForRenderedSQL(t *testing.T) {
	db := &gorp.DbMap{}

	err := BatchDeleteByIDs(context.Background(), db, "users", "id", []any{1})
	if err == nil {
		t.Fatal("expected executor without dialect to return an error")
	}

	if !strings.Contains(err.Error(), "dialect cannot be determined") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type scanDestUser struct {
	Name string
}

func TestBuildScanDestRejectsNilFieldPointer(t *testing.T) {
	col := NewCol[string](newMockTable("users"), "name", "name", nil)

	_, err := buildScanDest([]Column{col}, &scanDestUser{})
	if err == nil {
		t.Fatal("expected nil field pointer to return an error")
	}

	if !strings.Contains(err.Error(), "field pointer is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRecoversFieldPointerPanics(t *testing.T) {
	col := NewCol[string](
		newMockTable("users"),
		"name",
		"name",
		func(holder any) any { return &holder.(*scanDestUser).Name },
	)

	_, err := buildScanDest([]Column{col}, &struct{}{})
	if err == nil {
		t.Fatal("expected field pointer panic to return an error")
	}

	if !strings.Contains(err.Error(), "field pointer panicked") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNilScanTarget(t *testing.T) {
	col := NewCol[string](
		newMockTable("users"),
		"name",
		"name",
		func(holder any) any { return nil },
	)

	_, err := buildScanDest([]Column{col}, &scanDestUser{})
	if err == nil {
		t.Fatal("expected nil scan target to return an error")
	}

	if !strings.Contains(err.Error(), "returned nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNilHolder(t *testing.T) {
	col := NewCol[string](
		newMockTable("users"),
		"name",
		"name",
		func(holder any) any { return &holder.(*scanDestUser).Name },
	)

	_, err := buildScanDest([]Column{col}, nil)
	if err == nil {
		t.Fatal("expected nil holder to return an error")
	}

	if !strings.Contains(err.Error(), "scan holder cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNonPointerHolder(t *testing.T) {
	col := NewCol[string](
		newMockTable("users"),
		"name",
		"name",
		func(holder any) any { return &holder.(*scanDestUser).Name },
	)

	_, err := buildScanDest([]Column{col}, scanDestUser{})
	if err == nil {
		t.Fatal("expected non-pointer holder to return an error")
	}

	if !strings.Contains(err.Error(), "scan holder must be a pointer") {
		t.Fatalf("unexpected error: %v", err)
	}
}
