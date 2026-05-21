package tsq

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type queryOwner struct{}

func (queryOwner) TSQOwner() {
}

func mustBuild[O Owner](qb interface{ Build() (*Query[O], error) }) *Query[O] {
	q, err := qb.Build()
	if err != nil {
		panic(err)
	}
	return q
}

func newQueryBuilderForTest[O Owner](spec querySpec[O]) *queryBuilder[O] {
	return &queryBuilder[O]{queryBuilderCore: &queryBuilderCore[O]{spec: spec, phase: builderPhaseBase}}
}

func TestErrUnknownSortField(t *testing.T) {
	field := "unknown_field"
	err := newErrUnknownSortField(field)
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
	err := newErrAmbiguousSortField(field)
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
	err := newErrOrderCountMismatch(orderBys, orders)
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
	query := &Query[queryOwner]{cntSQL: "SELECT COUNT(*) FROM users", listSQL: "SELECT * FROM users", kwCntSQL: "SELECT COUNT(*) FROM users WHERE name LIKE ?", kwListSQL: "SELECT * FROM users WHERE name LIKE ?"}
	if query.CountSQL() != "SELECT COUNT(*) FROM users" {
		t.Errorf("Expected CountSQL 'SELECT COUNT(*) FROM users', got '%s'", query.CountSQL())
	}
	if query.ListSQL() != "SELECT * FROM users" {
		t.Errorf("Expected ListSQL 'SELECT * FROM users', got '%s'", query.ListSQL())
	}
	if query.KeywordCountSQL() != "SELECT COUNT(*) FROM users WHERE name LIKE ?" {
		t.Errorf("Expected KeywordCountSQL 'SELECT COUNT(*) FROM users WHERE name LIKE ?', got '%s'", query.KeywordCountSQL())
	}
	if query.KeywordListSQL() != "SELECT * FROM users WHERE name LIKE ?" {
		t.Errorf("Expected KeywordListSQL 'SELECT * FROM users WHERE name LIKE ?', got '%s'", query.KeywordListSQL())
	}
}

func TestQueryBuilder_Build_EmptySelectFields(t *testing.T) {
	qb := newQueryBuilderForTest(querySpec[Table]{})
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
	qb := newQueryBuilderForTest(querySpec[Table]{From: table, Selects: []BoundColumn[Table]{col}})
	query, err := qb.Build()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if query == nil {
		t.Error("Expected non-nil query")
	}
	if query.CountSQL() == "" {
		t.Error("Expected non-empty CntSQL")
	}
	if query.ListSQL() == "" {
		t.Error("Expected non-empty ListSQL")
	}
	if query.KeywordCountSQL() == "" {
		t.Error("Expected non-empty KwCntSQL")
	}
	if query.KeywordListSQL() == "" {
		t.Error("Expected non-empty KwListSQL")
	}
}

func TestQueryBuilder_Build_FullJoinDefersDialectValidationToExecution(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, string](users, "id", "id", nil)
	orderUserID := newColForTable[Table, string](orders, "user_id", "user_id", nil)
	query, err := Select(userID).From(userID.Table()).FullJoin(orders, userID.EQCol(orderUserID)).Build()
	if err != nil {
		t.Fatalf("expected FULL JOIN build to succeed, got %v", err)
	}
	db := newSQLiteIndexTestEngine(t)
	err = validateOperationalExecutorForSQL(db.Executor(), query.listSQL)
	if err == nil {
		t.Fatal("expected sqlite dialect validation to reject FULL JOIN")
	}
	if !strings.Contains(err.Error(), "FULL JOIN") {
		t.Fatalf("expected FULL JOIN dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_ForUpdateDefersDialectValidationToExecution(t *testing.T) {
	users := newMockTable("users")
	userID := newColForTable[Table, string](users, "id", "id", nil)
	query, err := Select(userID).From(userID.Table()).ForUpdate().Build()
	if err != nil {
		t.Fatalf("expected FOR UPDATE build to succeed, got %v", err)
	}
	db := newSQLiteIndexTestEngine(t)
	err = validateOperationalExecutorForSQL(db.Executor(), query.listSQL)
	if err == nil {
		t.Fatal("expected sqlite dialect validation to reject FOR UPDATE")
	}
	if !strings.Contains(err.Error(), "FOR UPDATE") {
		t.Fatalf("expected FOR UPDATE dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_ForShareNoWaitDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.engine.dialect = MySQLDialect{}
	users := newMockTable("users")
	userID := newColForTable[Table, string](users, "id", "id", nil)
	query := mustBuild(Select(userID).From(userID.Table()).ForShare().NoWait())
	if err := validateOperationalExecutorForSQL(db.Executor(), query.listSQL); err != nil {
		t.Fatalf("expected mysql dialect validation to allow FOR SHARE NOWAIT, got %v", err)
	}
}

func TestQueryBuilder_Build_SetOperationPaginationUsesOutputColumnNames(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderUserID := newMockColumn(orders, "user_id")
	query := mustBuild(Select(userID).From(userID.Table()).Union(Select(orderUserID).From(orderUserID.Table())))
	page := &PageRequest{Page: 1, Size: 10, OrderBy: "id", Order: "asc"}
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

func TestQueryBuilder_Build_PageSQLPlacesLockAfterLimit(t *testing.T) {
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	query := mustBuild(Select(userID).From(userID.Table()).ForUpdate().SkipLocked())
	page := &PageRequest{Page: 1, Size: 10}
	_, listSQL, err := query.buildPageSQLs(page)
	if err != nil {
		t.Fatalf("expected page SQL build to succeed, got %v", err)
	}
	if !strings.HasSuffix(listSQL, "LIMIT ? OFFSET ?\nFOR UPDATE SKIP LOCKED") {
		t.Fatalf("expected lock clause after LIMIT/OFFSET, got %q", listSQL)
	}
}

func TestQueryBuilder_Build_CTEExecutionOnSQLite(t *testing.T) {
	db := newInVarEngine(t)
	users := newMockTable("users")
	idCol := newColForTable[inVarUser, int64](users, "id", "id", toScanPointer(func(holder *inVarUser) *int64 {
		return &holder.ID
	}))
	nameCol := newColForTable[inVarUser, string](users, "name", "name", toScanPointer(func(holder *inVarUser) *string {
		return &holder.Name
	}))
	selectedUsers := CTE("selected_users", Select(idCol, nameCol).From(idCol.Table()).Where(idCol.InVar()))
	selectedUserID := idCol.WithTable(selectedUsers)
	selectedUserName := nameCol.WithTable(selectedUsers)
	query := mustBuild(Select(selectedUserID, selectedUserName).From(selectedUserID.Table()).Where(selectedUserID.GT(1)))
	rows, err := List[inVarUser](context.Background(), db.Executor(), query, []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("expected CTE query to execute, got %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 CTE rows, got %d", len(rows))
	}
	if rows[0].ID != 2 || rows[1].ID != 3 {
		t.Fatalf("unexpected CTE rows returned: %#v", rows)
	}
	count, err := query.Count(context.Background(), db.Executor(), []int64{1, 2, 3})
	if err != nil {
		t.Fatalf("expected CTE count query to execute, got %v", err)
	}
	if count != 2 {
		t.Fatalf("expected CTE count query to return 2, got %d", count)
	}
}

func TestQueryBuilder_Build_CTEDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.engine.dialect = MySQLDialect{}
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	filteredUsers := CTE("filtered_users", Select(id).From(id.Table()).Where(id.GT(1)))
	filteredUserID := id.WithTable(filteredUsers)
	query := mustBuild(Select(filteredUserID).From(filteredUsers))
	err := validateOperationalExecutorForSQL(db.Executor(), query.listSQL)
	if err == nil {
		t.Fatal("expected mysql dialect validation to reject CTE")
	}
	if !strings.Contains(err.Error(), "CTE") {
		t.Fatalf("expected CTE dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_IntersectDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.engine.dialect = MySQLDialect{}
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	query := mustBuild(Select(id).From(id.Table()).Intersect(Select(id).From(id.Table())))
	err := validateOperationalExecutorForSQL(db.Executor(), query.listSQL)
	if err == nil {
		t.Fatal("expected mysql dialect validation to reject INTERSECT")
	}
	if !strings.Contains(err.Error(), "INTERSECT") {
		t.Fatalf("expected INTERSECT dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_ExceptDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.engine.dialect = MySQLDialect{}
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	query := mustBuild(Select(id).From(id.Table()).Except(Select(id).From(id.Table())))
	err := validateOperationalExecutorForSQL(db.Executor(), query.listSQL)
	if err == nil {
		t.Fatal("expected mysql dialect validation to reject EXCEPT")
	}
	if !strings.Contains(err.Error(), "EXCEPT") {
		t.Fatalf("expected EXCEPT dialect error, got %v", err)
	}
}

func TestQueryBuilder_Build_MinusDefersDialectValidationToExecution(t *testing.T) {
	db := newInVarEngine(t)
	db.engine.dialect = MySQLDialect{}
	err := validateOperationalExecutorForSQL(db.Executor(), "SELECT 1 MINUS SELECT 1")
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

func (caseUser) TSQOwner() {
}

func TestQueryBuilder_Build_CaseExecutionOnSQLite(t *testing.T) {
	db := newInVarEngine(t)
	users := newMockTable("users")
	idCol := newColForTable[caseUser, int64](users, "id", "id", toScanPointer(func(holder *caseUser) *int64 {
		return &holder.ID
	}))
	nameLabel := MapInto[caseUser](Case[string]().When(idCol.GT(1), "member").Else("owner").End(), func(holder *caseUser) *string {
		return &holder.Label
	}, "label")
	query := mustBuild(Select(idCol, nameLabel).From(idCol.Table()).Where(idCol.InVar()))
	rows, err := List[caseUser](context.Background(), db.Executor(), query, []int64{1, 2})
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
	qb := newQueryBuilderForTest(querySpec[Table]{From: table, Selects: []BoundColumn[Table]{col}})
	query := mustBuild(qb)
	if query == nil {
		t.Error("Expected non-nil query")
	}
}

func TestQuery_MetadataAccess(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "id")
	query := &Query[Table]{selectCols: []BoundColumn[Table]{newColForTable[Table, string](table, "id", "id", nil)}, selectTables: map[string]Table{"users": table}, kwCols: []SearchColumn{col}, kwTables: map[string]Table{"users": table}}
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
	var _ error = &ErrUnknownSortField{}
	var _ error = &ErrOrderCountMismatch{}
	err1 := newErrUnknownSortField("test")
	if err1 == nil {
		t.Error("Expected non-nil error")
	}
	err2 := newErrOrderCountMismatch(1, 2)
	if err2 == nil {
		t.Error("Expected non-nil error")
	}
}

func TestQuery_EmptySQL(t *testing.T) {
	query := &Query[queryOwner]{}
	if query.CountSQL() != "" {
		t.Errorf("Expected empty CountSQL, got '%s'", query.CountSQL())
	}
	if query.ListSQL() != "" {
		t.Errorf("Expected empty ListSQL, got '%s'", query.ListSQL())
	}
	if query.KeywordCountSQL() != "" {
		t.Errorf("Expected empty KeywordCountSQL, got '%s'", query.KeywordCountSQL())
	}
	if query.KeywordListSQL() != "" {
		t.Errorf("Expected empty KeywordListSQL, got '%s'", query.KeywordListSQL())
	}
}

func TestNilQuery_SQLAccessorsReturnEmptyStrings(t *testing.T) {
	var query *Query[queryOwner]
	if query.CountSQL() != "" {
		t.Errorf("Expected empty CountSQL for nil query, got %q", query.CountSQL())
	}
	if query.ListSQL() != "" {
		t.Errorf("Expected empty ListSQL for nil query, got %q", query.ListSQL())
	}
	if query.KeywordCountSQL() != "" {
		t.Errorf("Expected empty KeywordCountSQL for nil query, got %q", query.KeywordCountSQL())
	}
	if query.KeywordListSQL() != "" {
		t.Errorf("Expected empty KeywordListSQL for nil query, got %q", query.KeywordListSQL())
	}
}

type scanDestUser struct{ Name string }

func (scanDestUser) TSQOwner() {
}

type inVarUser struct {
	ID   int64
	Name string
}

func (inVarUser) TSQOwner() {
}

func newScanValidationEngine(t *testing.T) *Runtime {
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
	return newRuntimeWithDB(db, SQLiteDialect{})
}

func newInVarEngine(t *testing.T) *Runtime {
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
	return newRuntimeWithDB(db, SQLiteDialect{})
}

func newEngineWithoutDialect(t *testing.T) *Runtime {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return newRuntimeWithDB(db, nil)
}
