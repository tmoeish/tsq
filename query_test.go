package tsq

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type queryOwner struct{}

func (queryOwner) TSQOwner() {}

// mustBuild is a test helper that builds a query and panics on error.
func mustBuild[O Owner](qb interface{ Build() (*Query[O], error) }) *Query[O] {
	q, err := qb.Build()
	if err != nil {
		panic(err)
	}
	return q
}

func newQueryBuilderForTest[O Owner](spec QuerySpec[O]) *QueryBuilder[O] {
	return &QueryBuilder[O]{
		queryBuilderCore: &queryBuilderCore[O]{
			spec:  spec,
			phase: builderPhaseBase,
		},
	}
}

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
	query := &Query[queryOwner]{
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
	qb := newQueryBuilderForTest(QuerySpec[Table]{})

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
	col := newColForTable[Table, string](table, "id", "id", nil)

	_, err := Select[Table]().From(table).Where(col.EQVar()).Build()
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

	qb := newQueryBuilderForTest(QuerySpec[Table]{
		From:    table,
		Selects: []BoundColumn[Table]{col},
	})

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

func TestQueryBuilder_Build_FullJoinDefersDialectValidationToExecution(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, string](users, "id", "id", nil)
	orderUserID := newColForTable[Table, string](orders, "user_id", "user_id", nil)

	query, err := Select(userID).
		From(userID.Table()).FullJoin(orders, userID.EQCol(orderUserID)).Build()
	if err != nil {
		t.Fatalf("expected FULL JOIN build to succeed, got %v", err)
	}

	db := newSQLiteIndexTestEngine(t)
	err = validateOperationalExecutorForSQL(db, query.listSQL)
	if err == nil {
		t.Fatal("expected sqlite dialect validation to reject FULL JOIN")
	}

	if !strings.Contains(err.Error(), "FULL JOIN") {
		t.Fatalf("expected FULL JOIN dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_SetOperationPaginationUsesOutputColumnNames(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderUserID := newMockColumn(orders, "user_id")

	query := mustBuild(Select(userID).
		From(userID.Table()).
		Union(Select(orderUserID).From(orderUserID.Table())))
	page := &PageReq{Page: 1, Size: 10, OrderBy: "id", Order: "asc"}

	_, listSQL, err := query.buildPageSQLs(page)
	if err != nil {
		t.Fatalf("expected page SQL build to succeed, got %v", err)
	}

	if !strings.Contains(listSQL, "ORDER BY "+rawIdentifier("id")+" ASC") {
		t.Fatalf("expected compound query to order by output column name, got %q", listSQL)
	}

	orderClause := listSQL[strings.Index(listSQL, "ORDER BY "):]
	if strings.Contains(orderClause, rawIdentifier("users")+"."+rawIdentifier("id")) {
		t.Fatalf("expected compound query ordering to avoid table-qualified columns, got %q", listSQL)
	}
}

func TestQueryBuilder_Build_CTEExecutionOnSQLite(t *testing.T) {
	db := newInVarEngine(t)
	users := newMockTable("users")
	idCol := newColForTable[inVarUser, int64](users, "id", "id", toScanPointer(func(holder *inVarUser) *int64 { return &holder.ID }))
	nameCol := newColForTable[inVarUser, string](users, "name", "name", toScanPointer(func(holder *inVarUser) *string { return &holder.Name }))

	selectedUsers := CTE("selected_users", Select(idCol, nameCol).
		From(idCol.Table()).Where(idCol.InVar()))
	selectedUserID := idCol.WithTable(selectedUsers)
	selectedUserName := nameCol.WithTable(selectedUsers)

	query := mustBuild(Select(selectedUserID, selectedUserName).
		From(selectedUserID.Table()).Where(selectedUserID.GT(1)))

	rows, err := List[inVarUser](context.Background(), db, query, []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("expected CTE query to execute, got %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 CTE rows, got %d", len(rows))
	}

	if rows[0].ID != 2 || rows[1].ID != 3 {
		t.Fatalf("unexpected CTE rows returned: %#v", rows)
	}

	count, err := query.Count(context.Background(), db, []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("expected CTE count query to execute, got %v", err)
	}

	if count != 2 {
		t.Fatalf("expected CTE count query to return 2, got %d", count)
	}
}

func TestQueryBuilder_Build_CTEDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.Dialect = MySQLDialect{}

	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	filteredUsers := CTE("filtered_users", Select(id).
		From(id.Table()).Where(id.GT(1)))
	filteredUserID := id.WithTable(filteredUsers)

	query := mustBuild(Select(filteredUserID).From(filteredUsers))
	err := validateOperationalExecutorForSQL(db, query.listSQL)
	if err == nil {
		t.Fatal("expected mysql dialect validation to reject CTE")
	}

	if !strings.Contains(err.Error(), "CTE") {
		t.Fatalf("expected CTE dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_IntersectDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.Dialect = MySQLDialect{}

	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)

	query := mustBuild(Select(id).
		From(id.Table()).
		Intersect(Select(id).From(id.Table())))
	err := validateOperationalExecutorForSQL(db, query.listSQL)
	if err == nil {
		t.Fatal("expected mysql dialect validation to reject INTERSECT")
	}

	if !strings.Contains(err.Error(), "INTERSECT") {
		t.Fatalf("expected INTERSECT dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_ExceptDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.Dialect = MySQLDialect{}

	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)

	query := mustBuild(Select(id).
		From(id.Table()).
		Except(Select(id).From(id.Table())))
	err := validateOperationalExecutorForSQL(db, query.listSQL)
	if err == nil {
		t.Fatal("expected mysql dialect validation to reject EXCEPT")
	}

	if !strings.Contains(err.Error(), "EXCEPT") {
		t.Fatalf("expected EXCEPT dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_MinusDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.Dialect = MySQLDialect{}

	err := validateOperationalExecutorForSQL(db, "SELECT 1 MINUS SELECT 1")
	if err == nil {
		t.Fatal("expected mysql dialect validation to reject MINUS")
	}

	if !strings.Contains(err.Error(), "EXCEPT") {
		t.Fatalf("expected EXCEPT dialect error for MINUS, got %v", err)
	}
}

type caseUser struct {
	ID    int64
	Label string
}

func (caseUser) TSQOwner() {}

func TestQueryBuilder_Build_CaseExecutionOnSQLite(t *testing.T) {
	db := newInVarEngine(t)
	users := newMockTable("users")
	idCol := newColForTable[caseUser, int64](users, "id", "id", toScanPointer(func(holder *caseUser) *int64 { return &holder.ID }))
	nameLabel := Into[caseUser](Case[string]().
		When(idCol.GT(1), "member").
		Else("owner").
		End(), func(holder *caseUser) *string { return &holder.Label }, "label")

	query := mustBuild(Select(idCol, nameLabel).
		From(idCol.Table()).Where(idCol.InVar()))

	rows, err := List[caseUser](context.Background(), db, query, []int64{1, 2})
	if err != nil {
		t.Fatalf("expected CASE query to execute, got %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 CASE rows, got %d", len(rows))
	}

	if rows[0].Label != "owner" || rows[1].Label != "member" {
		t.Fatalf("unexpected CASE labels: %#v", rows)
	}
}

func TestQueryBuilder_MustBuild_Success(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "id")

	qb := newQueryBuilderForTest(QuerySpec[Table]{
		From:    table,
		Selects: []BoundColumn[Table]{col},
	})

	// Should not panic
	query := mustBuild(qb)

	if query == nil {
		t.Error("Expected non-nil query")
	}
}

func TestDefaultChunkedInsertOptions(t *testing.T) {
	opts := DefaultChunkedInsertOptions()

	if opts == nil {
		t.Fatal("Expected non-nil options")
	}

	if opts.ChunkSize != 1000 {
		t.Errorf("Expected ChunkSize 1000, got %d", opts.ChunkSize)
	}

	if opts.IgnoreErrors != false {
		t.Errorf("Expected IgnoreErrors false, got %v", opts.IgnoreErrors)
	}
}

func TestDefaultChunkedOptions(t *testing.T) {
	opts := DefaultChunkedOptions()

	if opts == nil {
		t.Fatal("expected non-nil options")
	}

	if opts.ChunkSize != 1000 {
		t.Fatalf("expected chunk size 1000, got %d", opts.ChunkSize)
	}
}

func TestChunkedInsertOptions_Modification(t *testing.T) {
	opts := DefaultChunkedInsertOptions()

	// Modify options
	opts.ChunkSize = 500
	opts.IgnoreErrors = true

	if opts.ChunkSize != 500 {
		t.Errorf("Expected ChunkSize 500, got %d", opts.ChunkSize)
	}

	if opts.IgnoreErrors != true {
		t.Errorf("Expected IgnoreErrors true, got %v", opts.IgnoreErrors)
	}
}

func TestQuery_MetadataAccess(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "id")

	query := &Query[Table]{
		selectCols:   []BoundColumn[Table]{newColForTable[Table, string](table, "id", "id", nil)},
		selectTables: map[string]Table{"users": table},
		kwCols:       []SearchColumn{col},
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
	query := &Query[queryOwner]{}

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
	var query *Query[queryOwner]

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
	query := &Query[queryOwner]{
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
	var query *Query[queryOwner]

	_, _, err := query.buildPageSQLs(nil)
	if err == nil {
		t.Fatal("expected nil query to return an error")
	}

	if !strings.Contains(err.Error(), "query cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQuery_buildPageSQLsRejectsUnbuiltQuery(t *testing.T) {
	query := &Query[queryOwner]{}

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

	query := mustBuild(Select(userID, orderID).
		From(userID.Table()).
		CrossJoin(orderID.Table()))

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

func TestQuery_buildPageSQLsIgnoresHiddenJSONSortAlias(t *testing.T) {
	users := newMockTable("users")
	hidden := newColForTable[Table, string](users, "secret", "-", nil)

	query := mustBuild(Select(hidden).From(hidden.Table()))

	_, _, err := query.buildPageSQLs(&PageReq{
		OrderBy: "-",
		Order:   "ASC",
	})
	if err == nil {
		t.Fatal("expected json:- sort alias to be rejected")
	}

	var unknownErr *ErrUnknownSortField
	if !errors.As(err, &unknownErr) {
		t.Fatalf("expected ErrUnknownSortField, got %v", err)
	}
}

func TestQuery_buildPageSQLsDefaultsMissingOrderToASC(t *testing.T) {
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	userName := newMockColumn(users, "name")

	query := mustBuild(Select(userID, userName).From(userID.Table()))

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

	query := mustBuild(Select(userID, userName).From(userID.Table()))

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

func TestQuery_BuildKeywordQueriesTrackDedicatedMarkers(t *testing.T) {
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	userName := newMockColumn(users, "name")

	query := mustBuild(Select(userID, userName).
		From(userID.Table()).
		Search(userID, userName))

	if got := len(query.kwListArgs); got != 2 {
		t.Fatalf("expected 2 keyword list args, got %d", got)
	}

	if got := len(query.kwCntArgs); got != 2 {
		t.Fatalf("expected 2 keyword count args, got %d", got)
	}

	args, err := resolveQueryArgs(query.kwListArgs, nil, "demo")
	if err != nil {
		t.Fatalf("expected keyword markers to resolve, got %v", err)
	}

	if len(args) != 2 || args[0] != "%demo%" || args[1] != "%demo%" {
		t.Fatalf("unexpected resolved keyword args: %#v", args)
	}
}

func TestResolveQueryExpandsExternalSliceArgs(t *testing.T) {
	sqlText, args, err := resolveQuery(
		`SELECT * FROM "users" WHERE "users"."id" IN (?) AND "users"."name" = ?`,
		[]any{externalSliceArgMarker{}, externalArgMarker},
		[]any{[]int64{1, 3, 5}, "alice"},
		"",
	)
	if err != nil {
		t.Fatalf("expected resolveQuery to expand slice args, got %v", err)
	}

	wantSQL := `SELECT * FROM "users" WHERE "users"."id" IN (?, ?, ?) AND "users"."name" = ?`
	if sqlText != wantSQL {
		t.Fatalf("expected SQL %q, got %q", wantSQL, sqlText)
	}

	if want := []any{int64(1), int64(3), int64(5), "alice"}; len(args) != len(want) ||
		args[0] != want[0] || args[1] != want[1] || args[2] != want[2] || args[3] != want[3] {
		t.Fatalf("unexpected resolved args: %#v", args)
	}
}

func TestResolveQueryExpandsEmptyExternalSliceArgsToNull(t *testing.T) {
	sqlText, args, err := resolveQuery(
		`SELECT * FROM "users" WHERE "users"."id" IN (?)`,
		[]any{externalSliceArgMarker{}},
		[]any{[]int64{}},
		"",
	)
	if err != nil {
		t.Fatalf("expected empty slice to resolve, got %v", err)
	}

	if sqlText != `SELECT * FROM "users" WHERE "users"."id" IN (NULL)` {
		t.Fatalf("unexpected SQL for empty slice: %q", sqlText)
	}

	if len(args) != 0 {
		t.Fatalf("expected empty slice to contribute no args, got %#v", args)
	}
}

func TestNormalizeChunkedInsertOptionsValidatesInputs(t *testing.T) {
	if _, err := normalizeChunkedInsertOptions(&ChunkedInsertOptions{ChunkSize: 0}); err == nil {
		t.Fatal("expected zero chunk size to return an error")
	}
}

func TestNormalizeChunkedInsertOptionsRejectsMultipleValues(t *testing.T) {
	_, err := normalizeChunkedInsertOptions(&ChunkedInsertOptions{}, &ChunkedInsertOptions{})
	if err == nil {
		t.Fatal("expected multiple option values to return an error")
	}

	if !strings.Contains(err.Error(), "at most one") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeChunkedOptionsValidatesInputs(t *testing.T) {
	if _, err := normalizeChunkedOptions(&ChunkedOptions{ChunkSize: 0}); err == nil {
		t.Fatal("expected zero chunk size to return an error")
	}
}

func TestNormalizeChunkedOptionsRejectsMultipleValues(t *testing.T) {
	_, err := normalizeChunkedOptions(&ChunkedOptions{}, &ChunkedOptions{})
	if err == nil {
		t.Fatal("expected multiple option values to return an error")
	}

	if !strings.Contains(err.Error(), "at most one") {
		t.Fatalf("unexpected error: %v", err)
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

func TestRenderSQLForDialectPostgres(t *testing.T) {
	users := newMockTable("users")
	userID := newColForTable[Table, int](users, "id", "id", nil)

	query := mustBuild(Select(userID).
		From(userID.Table()).Where(userID.EQVar()))

	got := renderSQLForDialect(query.listSQL, PostgresDialect{})
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

	got := renderSQLForDialect(sqlStr, PostgresDialect{})
	want := `DELETE FROM "users" WHERE "id" IN ($1,$2)`

	if got != want {
		t.Fatalf("expected postgres delete SQL %q, got %q", want, got)
	}
}

func TestChunkedUpdateChunkRejectsNilItems(t *testing.T) {
	err := chunkedUpdateChunk[*mockTable](nil, nil, []*mockTable{nil})
	if err == nil {
		t.Fatal("expected nil batch update item to return an error")
	}

	if !strings.Contains(err.Error(), "item at index 0 is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkedDeleteChunkRejectsNilItems(t *testing.T) {
	err := chunkedDeleteChunk[*mockTable](nil, nil, []*mockTable{nil})
	if err == nil {
		t.Fatal("expected nil batch delete item to return an error")
	}

	if !strings.Contains(err.Error(), "item at index 0 is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPageFnRejectsNilQuery(t *testing.T) {
	_, err := pageFn[queryOwner](context.Background(), nil, nil, nil)
	if err == nil {
		t.Fatal("expected nil query to return an error")
	}

	if !strings.Contains(err.Error(), "query cannot be nil") {
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

func TestInsertRejectsTypedNilExecutor(t *testing.T) {
	var db *Engine

	row := mockTable{tableName: "users"}

	err := Insert(context.Background(), db, &row)
	if err == nil {
		t.Fatal("expected typed-nil executor to return an error")
	}

	if !strings.Contains(err.Error(), "sql executor cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertRejectsNilItem(t *testing.T) {
	db := &Engine{}

	var value *mockTable

	err := Insert(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}

	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateRejectsNilItem(t *testing.T) {
	db := &Engine{}

	var value *mockTable

	err := Update(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}

	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteRejectsNilItem(t *testing.T) {
	db := &Engine{}

	var value *mockTable

	err := Delete(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}

	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkedInsertRejectsTypedNilExecutor(t *testing.T) {
	var db *Engine

	row := mockTable{tableName: "users"}

	err := ChunkedInsert(context.Background(), db, []*mockTable{&row})
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

func TestChunkedDeleteByIDsRejectsExecutorWithoutDialectForRenderedSQL(t *testing.T) {
	db := newEngineWithoutDialect(t)

	err := ChunkedDeleteByIDs(context.Background(), db, "users", "id", []any{1})
	if err == nil {
		t.Fatal("expected executor without dialect to return an error")
	}

	if !strings.Contains(err.Error(), "dialect cannot be determined") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateExecutorForSQLIgnoresMarkersInsideStringsAndComments(t *testing.T) {
	db := &Engine{}
	rawSQL := "SELECT 1 /* " + identifierMarkerPrefix + "ignored_comment" + identifierMarkerSuffix + " */" +
		" WHERE note = '" + identifierMarkerPrefix + "ignored_string" + identifierMarkerSuffix + "'" +
		" -- " + identifierMarkerPrefix + "ignored_tail" + identifierMarkerSuffix + "\n"

	if err := validateExecutorForSQL(db, rawSQL); err != nil {
		t.Fatalf("expected markers inside strings/comments to be ignored, got %v", err)
	}
}

func TestValidateExecutorForSQLIgnoresMarkersInsideDollarQuotedStrings(t *testing.T) {
	db := &Engine{}
	rawSQL := "SELECT $$" + identifierMarkerPrefix + "ignored_marker" + identifierMarkerSuffix + "$$"

	if err := validateExecutorForSQL(db, rawSQL); err != nil {
		t.Fatalf("expected markers inside dollar-quoted strings to be ignored, got %v", err)
	}
}

func TestValidateExecutorForSQLRejectsBindVarsWithoutDialect(t *testing.T) {
	db := &Engine{}

	if err := validateExecutorForSQL(db, "SELECT ?"); err == nil {
		t.Fatal("expected bind vars without a known dialect to return an error")
	}
}

func TestValidateExecutorForSQLIgnoresBindVarsInsideStringsCommentsAndDollarQuotes(t *testing.T) {
	db := &Engine{}
	rawSQL := "SELECT '?'" +
		" /* ? */" +
		" WHERE note = $$?$$" +
		" -- ?\n"

	if err := validateExecutorForSQL(db, rawSQL); err != nil {
		t.Fatalf("expected bind vars inside strings/comments to be ignored, got %v", err)
	}
}

func TestChunkedDeleteByIDsRejectsNilIDs(t *testing.T) {
	db := &Engine{Dialect: SQLiteDialect{}}

	err := ChunkedDeleteByIDs(context.Background(), db, "users", "id", []any{1, nil})
	if err == nil {
		t.Fatal("expected nil ids to return an error")
	}

	if !strings.Contains(err.Error(), "cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type scanDestUser struct {
	Name string
}

func (scanDestUser) TSQOwner() {}

type inVarUser struct {
	ID   int64
	Name string
}

func (inVarUser) TSQOwner() {}

func newScanValidationEngine(t *testing.T) *Engine {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := db.Exec("CREATE TABLE users (name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	return &Engine{DB: db, Dialect: SQLiteDialect{}}
}

func newInVarEngine(t *testing.T) *Engine {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := db.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
	`); err != nil {
		t.Fatalf("failed to seed users table: %v", err)
	}

	return &Engine{DB: db, Dialect: SQLiteDialect{}}
}

func newEngineWithoutDialect(t *testing.T) *Engine {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return &Engine{DB: db}
}

func TestListValidatesScanDestEvenWhenResultIsEmpty(t *testing.T) {
	db := newScanValidationEngine(t)
	col := newColForTable[scanDestUser, string](newMockTable("users"), "name", "name", nil)
	query := &Query[scanDestUser]{
		cntSQL:     "SELECT COUNT(1) FROM users",
		listSQL:    "SELECT name FROM users WHERE 1 = 0",
		selectCols: []BoundColumn[scanDestUser]{col},
	}

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
	idCol := newColForTable[inVarUser, int64](users, "id", "id", toScanPointer(func(holder *inVarUser) *int64 { return &holder.ID }))
	nameCol := newColForTable[inVarUser, string](users, "name", "name", toScanPointer(func(holder *inVarUser) *string { return &holder.Name }))

	query := mustBuild(Select(idCol, nameCol).
		From(idCol.Table()).Where(idCol.InVar()))

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
	query := &Query[scanDestUser]{
		cntSQL:     "SELECT COUNT(1) FROM users",
		listSQL:    "SELECT name FROM users WHERE 1 = 0",
		selectCols: []BoundColumn[scanDestUser]{col},
	}

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
	col := newColForTable[scanDestUser, string](
		newMockTable("users"),
		"name",
		"name",
		toScanPointer(func(holder *scanDestUser) *string { return &holder.Name }),
	)

	_, err := invokeFieldPointer(col.FieldPointer(), &queryOwner{})
	if err == nil {
		t.Fatal("expected field pointer panic to return an error")
	}

	if !strings.Contains(err.Error(), "field pointer panicked") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNilScanTarget(t *testing.T) {
	col := newColForTable[scanDestUser, string](
		newMockTable("users"),
		"name",
		"name",
		toScanPointer(func(holder *scanDestUser) *string { return nil }),
	)

	_, err := buildScanDest([]BoundColumn[scanDestUser]{col}, &scanDestUser{})
	if err == nil {
		t.Fatal("expected nil scan target to return an error")
	}

	if !strings.Contains(err.Error(), "returned nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildScanDestRejectsNilHolder(t *testing.T) {
	col := newColForTable[scanDestUser, string](
		newMockTable("users"),
		"name",
		"name",
		toScanPointer(func(holder *scanDestUser) *string { return &holder.Name }),
	)

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

func TestEscapeKeywordSearch(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "no special chars",
			input:    "hello world",
			expected: "hello world",
		},
		{
			name:     "percent wildcard",
			input:    "100%",
			expected: "100\\%",
		},
		{
			name:     "underscore wildcard",
			input:    "user_name",
			expected: "user\\_name",
		},
		{
			name:     "both wildcards",
			input:    "%value_",
			expected: "\\%value\\_",
		},
		{
			name:     "backslash",
			input:    "path\\file",
			expected: "path\\\\file",
		},
		{
			name:     "backslash before percent",
			input:    "100\\% cotton",
			expected: "100\\\\\\% cotton",
		},
		{
			name:     "real world example",
			input:    "50% off_sale",
			expected: "50\\% off\\_sale",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EscapeKeywordSearch(tt.input)
			if result != tt.expected {
				t.Errorf("EscapeKeywordSearch(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValidateIdentifierLength(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		dialect    Dialect
		wantErr    bool
	}{
		{
			name:       "valid identifier - mysql",
			identifier: "users",
			dialect:    MySQLDialect{},
			wantErr:    false,
		},
		{
			name:       "valid identifier - postgres",
			identifier: "users",
			dialect:    PostgresDialect{},
			wantErr:    false,
		},
		{
			name:       "max length postgres (63)",
			identifier: "a" + strings.Repeat("b", 62),
			dialect:    PostgresDialect{},
			wantErr:    false,
		},
		{
			name:       "exceeds max postgres (64 > 63)",
			identifier: strings.Repeat("a", 64),
			dialect:    PostgresDialect{},
			wantErr:    true,
		},
		{
			name:       "max length mysql (64)",
			identifier: strings.Repeat("a", 64),
			dialect:    MySQLDialect{},
			wantErr:    false,
		},
		{
			name:       "exceeds max mysql (65 > 64)",
			identifier: strings.Repeat("a", 65),
			dialect:    MySQLDialect{},
			wantErr:    true,
		},
		{
			name:       "sqlite has no limit",
			identifier: strings.Repeat("a", 200),
			dialect:    SQLiteDialect{},
			wantErr:    false,
		},
		{
			name:       "empty identifier",
			identifier: "",
			dialect:    MySQLDialect{},
			wantErr:    true,
		},
		{
			name:       "nil dialect skips length validation",
			identifier: strings.Repeat("a", 100),
			dialect:    nil,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIdentifierLength(tt.identifier, tt.dialect)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateIdentifierLength(%q, %T) error = %v, wantErr %v", tt.identifier, tt.dialect, err, tt.wantErr)
			}
		})
	}
}

func TestValidateIdentifierForDialect(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		dialect    Dialect
		wantErr    bool
		errContent string
	}{
		{
			name:       "valid identifier - mysql",
			identifier: "users",
			dialect:    MySQLDialect{},
			wantErr:    false,
		},
		{
			name:       "valid identifier - postgres",
			identifier: "users_table",
			dialect:    PostgresDialect{},
			wantErr:    false,
		},
		{
			name:       "starts with underscore",
			identifier: "_internal",
			dialect:    MySQLDialect{},
			wantErr:    false,
		},
		{
			name:       "invalid - starts with number",
			identifier: "123users",
			dialect:    MySQLDialect{},
			wantErr:    true,
			errContent: "invalid SQL identifier",
		},
		{
			name:       "invalid - contains hyphen",
			identifier: "user-table",
			dialect:    MySQLDialect{},
			wantErr:    true,
			errContent: "invalid SQL identifier",
		},
		{
			name:       "exceeds postgres limit",
			identifier: strings.Repeat("a", 64),
			dialect:    PostgresDialect{},
			wantErr:    true,
			errContent: "exceeds",
		},
		{
			name:       "at mysql limit (64)",
			identifier: strings.Repeat("x", 64),
			dialect:    MySQLDialect{},
			wantErr:    false,
		},
		{
			name:       "empty identifier",
			identifier: "",
			dialect:    MySQLDialect{},
			wantErr:    true,
			errContent: "cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIdentifierForDialect(tt.identifier, tt.dialect)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateIdentifierForDialect(%q, %T) error = %v, wantErr %v", tt.identifier, tt.dialect, err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errContent != "" && !strings.Contains(err.Error(), tt.errContent) {
				t.Errorf("ValidateIdentifierForDialect(%q, %T) error message %q should contain %q", tt.identifier, tt.dialect, err.Error(), tt.errContent)
			}
		})
	}
}
