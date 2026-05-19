package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
)

var (
	errSQLExecutorNil              = errors.New("sql executor cannot be nil")
	errEngineNil                   = errors.New("engine cannot be nil")
	errEngineDatabaseNil           = errors.New("engine database cannot be nil")
	errInsertRequiresColumn        = errors.New("insert requires at least one column")
	errInsertLayoutMismatch        = errors.New("batch insert requires matching column layouts")
	errUpdateRequiresMutableColumn = errors.New("update requires at least one mutable column")
	errUpdateRequiresPrimaryKey    = errors.New("update requires a non-zero primary key")
	errUpdateLayoutMismatch        = errors.New("batch update requires matching column layouts")
	errDeleteRequiresPrimaryKey    = errors.New("delete requires a non-zero primary key")
	errMutationItemNil             = errors.New("mutation item cannot be nil")
	errMutationItemPointer         = errors.New("mutation item must be a non-nil pointer")
	errMutationItemStructPointer   = errors.New("mutation item must point to a struct")
	errMutationItemNoTaggedFields  = errors.New("mutation item has no db-tagged fields")
)

// ErrOptimisticLockConflict reports that a version-guarded mutation matched fewer
// rows than expected.
type ErrOptimisticLockConflict struct {
	table    string
	expected int
	actual   int64
}

// Error implements error.
func (e *ErrOptimisticLockConflict) Error() string {
	if e == nil {
		return "optimistic lock conflict"
	}

	if e.table == "" {
		return fmt.Sprintf(
			"optimistic lock conflict: expected %d row(s) to match, updated %d",
			e.expected,
			e.actual,
		)
	}

	return fmt.Sprintf(
		"optimistic lock conflict on %s: expected %d row(s) to match, updated %d",
		e.table,
		e.expected,
		e.actual,
	)
}

// Is reports whether target is an optimistic lock conflict.
func (e *ErrOptimisticLockConflict) Is(target error) bool {
	_, ok := target.(*ErrOptimisticLockConflict)
	return ok
}

type errorRowContextKey struct{}

type errorRowDriver struct{}

type errorRowConn struct{}

var (
	errorRowDBOnce sync.Once
	errorRowDB     *sql.DB
)

func (errorRowDriver) Open(string) (driver.Conn, error) {
	return errorRowConn{}, nil
}

func (errorRowConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (errorRowConn) Close() error {
	return nil
}

func (errorRowConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (errorRowConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	if err, ok := ctx.Value(errorRowContextKey{}).(error); ok && err != nil {
		return nil, err
	}

	return nil, errors.New("missing query row error")
}

func (errorRowConn) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

func errorQueryRow(ctx context.Context, err error) *sql.Row {
	errorRowDBOnce.Do(func() {
		sql.Register("tsq-error-row", errorRowDriver{})

		db, openErr := sql.Open("tsq-error-row", "")
		if openErr != nil {
			panic(openErr)
		}
		errorRowDB = db
	})

	if ctx == nil {
		ctx = context.Background()
	}

	return errorRowDB.QueryRowContext(context.WithValue(ctx, errorRowContextKey{}, err), "SELECT 1")
}

func engineExecutionError(engine *Engine) error {
	if engine == nil {
		return errEngineNil
	}

	if engine.DB == nil {
		return errEngineDatabaseNil
	}

	return nil
}

type mutationExecutor interface {
	Insert(ctx context.Context, dst ...Table) error
	Update(ctx context.Context, dst ...Table) (int64, error)
	Delete(ctx context.Context, dst ...Table) (int64, error)
}

type sqlMutationExecutor struct {
	exec SQLExecutor
}

func (m sqlMutationExecutor) Insert(ctx context.Context, dst ...Table) error {
	return insertTables(ctx, m.exec, dst...)
}

func (m sqlMutationExecutor) Update(ctx context.Context, dst ...Table) (int64, error) {
	return updateTables(ctx, m.exec, dst...)
}

func (m sqlMutationExecutor) Delete(ctx context.Context, dst ...Table) (int64, error) {
	return deleteTables(ctx, m.exec, dst...)
}

// Engine couples a *sql.DB with the dialect rules tsq should use for it.
type Engine struct {
	DB             *sql.DB
	Dialect        Dialect
	schemaConfigMu sync.RWMutex
	schemaConfig   dbSchemaConfig
}
type dbSchemaConfig struct {
	indexInitMode      IndexInitMode
	schemaEventHandler func(SchemaEvent)
}

func defaultDBSchemaConfig() dbSchemaConfig {
	return dbSchemaConfig{indexInitMode: IndexInitUpsert}
}

func loadDBSchemaConfig(db *Engine) dbSchemaConfig {
	if db == nil {
		return defaultDBSchemaConfig()
	}

	db.schemaConfigMu.RLock()
	cfg := db.schemaConfig
	db.schemaConfigMu.RUnlock()

	if cfg.indexInitMode != "" || cfg.schemaEventHandler != nil {
		return cfg
	}

	return defaultDBSchemaConfig()
}

func storeDBSchemaConfig(db *Engine, cfg dbSchemaConfig) {
	if db == nil {
		return
	}

	if cfg.indexInitMode == "" {
		cfg.indexInitMode = IndexInitUpsert
	}

	if cfg.indexInitMode == IndexInitUpsert && cfg.schemaEventHandler == nil {
		db.schemaConfigMu.Lock()
		db.schemaConfig = dbSchemaConfig{}
		db.schemaConfigMu.Unlock()

		return
	}

	db.schemaConfigMu.Lock()
	db.schemaConfig = cfg
	db.schemaConfigMu.Unlock()
}

// TSQDialect exposes the Engine dialect for SQL rendering and validation.
func (e *Engine) TSQDialect() Dialect {
	if e == nil {
		return nil
	}

	return e.Dialect
}

// QueryContext executes a query and returns rows.
func (e *Engine) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := engineExecutionError(e); err != nil {
		return nil, err
	}

	rows, err := e.DB.QueryContext(ctx, query, args...)

	return rows, err
}

// QueryRowContext executes a query that returns a single row.
func (e *Engine) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if err := engineExecutionError(e); err != nil {
		return errorQueryRow(ctx, err)
	}

	return e.DB.QueryRowContext(ctx, query, args...)
}

// ExecContext executes a query without returning rows.
func (e *Engine) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := engineExecutionError(e); err != nil {
		return nil, err
	}

	res, err := e.DB.ExecContext(ctx, query, args...)

	return res, err
}

type mutationField struct {
	column string
	value  reflect.Value
}

type mutationRecord struct {
	tableName    string
	fields       []mutationField
	pkField      mutationField
	versionField mutationField
	autoIncr     bool
}

// Insert inserts objects into the database.
func (e *Engine) Insert(ctx context.Context, dst ...Table) error {
	return insertTables(ctx, e, dst...)
}

// Update updates objects in the database.
func (e *Engine) Update(ctx context.Context, dst ...Table) (int64, error) {
	return updateTables(ctx, e, dst...)
}

// Delete deletes objects from the database.
func (e *Engine) Delete(ctx context.Context, dst ...Table) (int64, error) {
	return deleteTables(ctx, e, dst...)
}

func insertTables(ctx context.Context, exec SQLExecutor, dst ...Table) error {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return err
	}

	for _, group := range groupInsertRecords(records) {
		if err := insertBatch(ctx, exec, group); err != nil {
			return err
		}
	}

	return nil
}

func updateTables(ctx context.Context, exec SQLExecutor, dst ...Table) (int64, error) {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return 0, err
	}

	var total int64

	for _, group := range groupUpdateRecords(records) {
		affected, err := updateBatch(ctx, exec, group)
		if err != nil {
			return total, err
		}

		total += affected
	}

	return total, nil
}

func deleteTables(ctx context.Context, exec SQLExecutor, dst ...Table) (int64, error) {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return 0, err
	}

	var total int64

	for _, group := range groupDeleteRecords(records) {
		affected, err := deleteBatch(ctx, exec, group)
		if err != nil {
			return total, err
		}

		total += affected
	}

	return total, nil
}

func collectMutationRecords(dst []Table) ([]mutationRecord, error) {
	records := make([]mutationRecord, 0, len(dst))

	for _, item := range dst {
		record, err := mutationMetadata(item)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

func groupInsertRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		fields := insertFieldsForRecord(record)
		return record.tableName + "|" + record.pkField.column + "|" + strings.Join(mutationFieldColumns(fields), ",")
	})
}

func groupUpdateRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		return record.tableName + "|" + record.pkField.column + "|" +
			strings.Join(mutationFieldColumns(updateFieldsForRecord(record)), ",")
	})
}

func groupDeleteRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		return record.tableName + "|" + record.pkField.column
	})
}

func groupMutationRecords(records []mutationRecord, keyFn func(mutationRecord) string) [][]mutationRecord {
	if len(records) == 0 {
		return nil
	}

	groups := make([][]mutationRecord, 0)
	indexByKey := make(map[string]int)

	for _, record := range records {
		key := keyFn(record)
		if idx, ok := indexByKey[key]; ok {
			groups[idx] = append(groups[idx], record)
			continue
		}

		indexByKey[key] = len(groups)
		groups = append(groups, []mutationRecord{record})
	}

	return groups
}

func insertBatch(ctx context.Context, exec SQLExecutor, records []mutationRecord) error {
	if len(records) == 0 {
		return nil
	}

	insertFields := insertFieldsForRecord(records[0])
	if len(insertFields) == 0 {
		return errInsertRequiresColumn
	}

	for _, record := range records[1:] {
		if !mutationFieldColumnsEqual(insertFields, insertFieldsForRecord(record)) {
			return errInsertLayoutMismatch
		}
	}

	tableSQL, err := quoteMutationIdentifier(exec, records[0].tableName)
	if err != nil {
		return err
	}

	quotedCols := make([]string, 0, len(insertFields))

	for _, field := range insertFields {
		col, err := quoteMutationIdentifier(exec, field.column)
		if err != nil {
			return err
		}

		quotedCols = append(quotedCols, col)
	}

	var (
		argIndex     int
		args         = make([]any, 0, len(insertFields)*len(records))
		valueClauses = make([]string, 0, len(records))
	)

	for _, record := range records {
		recordFields := insertFieldsForRecord(record)
		placeholders := make([]string, 0, len(recordFields))

		for _, field := range recordFields {
			placeholders = append(placeholders, nextBindVar(exec, &argIndex))
			args = append(args, field.value.Interface())
		}

		valueClauses = append(valueClauses, "("+strings.Join(placeholders, ", ")+")")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableSQL,
		strings.Join(quotedCols, ", "),
		strings.Join(valueClauses, ", "),
	)

	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	assignBatchInsertIDs(exec, records, result, len(insertFields) != len(records[0].fields))

	return nil
}

func updateBatch(ctx context.Context, exec SQLExecutor, records []mutationRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	updateFields := updateFieldsForRecord(records[0])
	if len(updateFields) == 0 {
		return 0, errUpdateRequiresMutableColumn
	}

	for _, record := range records {
		if isZeroMutationValue(record.pkField.value) {
			return 0, errUpdateRequiresPrimaryKey
		}

		if !mutationFieldColumnsEqual(updateFields, updateFieldsForRecord(record)) {
			return 0, errUpdateLayoutMismatch
		}
	}

	tableSQL, err := quoteMutationIdentifier(exec, records[0].tableName)
	if err != nil {
		return 0, err
	}

	pkSQL, err := quoteMutationIdentifier(exec, records[0].pkField.column)
	if err != nil {
		return 0, err
	}

	hasOptimisticLock := hasOptimisticMutation(records[0])

	versionSQL := ""
	if hasOptimisticLock {
		versionSQL, err = quoteMutationIdentifier(exec, records[0].versionField.column)
		if err != nil {
			return 0, err
		}
	}

	var (
		argIndex   int
		args       []any
		setClauses = make([]string, 0, len(updateFields))
	)

	for _, field := range updateFields {
		colSQL, err := quoteMutationIdentifier(exec, field.column)
		if err != nil {
			return 0, err
		}

		var clause strings.Builder
		clause.WriteString(colSQL)
		clause.WriteString(" = CASE ")
		clause.WriteString(pkSQL)

		for _, record := range records {
			recordField := mutationFieldByColumn(record.fields, field.column)

			clause.WriteString(" WHEN ")
			clause.WriteString(nextBindVar(exec, &argIndex))
			clause.WriteString(" THEN ")
			clause.WriteString(nextBindVar(exec, &argIndex))

			args = append(args, record.pkField.value.Interface(), recordField.value.Interface())
		}

		clause.WriteString(" ELSE ")
		clause.WriteString(colSQL)
		clause.WriteString(" END")
		setClauses = append(setClauses, clause.String())
	}

	if hasOptimisticLock {
		setClauses = append(setClauses, versionSQL+" = "+versionSQL+" + 1")
	}

	whereSQL, whereArgs, err := buildMutationWhereClause(exec, records, &argIndex)
	if err != nil {
		return 0, err
	}

	args = append(args, whereArgs...)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		tableSQL,
		strings.Join(setClauses, ", "),
		whereSQL,
	)

	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if hasOptimisticLock && rowsAffected != int64(len(records)) {
		return rowsAffected, &ErrOptimisticLockConflict{
			table:    records[0].tableName,
			expected: len(records),
			actual:   rowsAffected,
		}
	}

	if hasOptimisticLock {
		incrementMutationVersions(records)
	}

	return rowsAffected, nil
}

func deleteBatch(ctx context.Context, exec SQLExecutor, records []mutationRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	for _, record := range records {
		if isZeroMutationValue(record.pkField.value) {
			return 0, errDeleteRequiresPrimaryKey
		}
	}

	tableSQL, err := quoteMutationIdentifier(exec, records[0].tableName)
	if err != nil {
		return 0, err
	}

	var argIndex int

	whereSQL, args, err := buildMutationWhereClause(exec, records, &argIndex)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		tableSQL,
		whereSQL,
	)

	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if hasOptimisticMutation(records[0]) && rowsAffected != int64(len(records)) {
		return rowsAffected, &ErrOptimisticLockConflict{
			table:    records[0].tableName,
			expected: len(records),
			actual:   rowsAffected,
		}
	}

	return rowsAffected, nil
}

func mutationMetadata(dst Table) (mutationRecord, error) {
	if isNilValue(dst) {
		return mutationRecord{}, errMutationItemNil
	}

	value := reflect.ValueOf(dst)
	if value.Kind() != reflect.Ptr || value.IsNil() {
		return mutationRecord{}, errMutationItemPointer
	}

	if value.Elem().Kind() != reflect.Struct {
		return mutationRecord{}, errMutationItemStructPointer
	}

	fields, err := collectMutationFields(dst)
	if err != nil {
		return mutationRecord{}, err
	}

	if len(fields) == 0 {
		return mutationRecord{}, errMutationItemNoTaggedFields
	}

	pkField, err := primaryMutationField(dst.PrimaryKeys(), fields)
	if err != nil {
		return mutationRecord{}, err
	}

	versionField, err := optimisticLockMutationField(dst.VersionColumn(), fields)
	if err != nil {
		return mutationRecord{}, err
	}

	return mutationRecord{
		tableName:    dst.Table(),
		fields:       fields,
		pkField:      pkField,
		versionField: versionField,
		autoIncr:     dst.AutoIncrement(),
	}, nil
}

func collectMutationFields(dst Table) ([]mutationField, error) {
	fields := make([]mutationField, 0, len(dst.Cols()))
	for _, col := range dst.Cols() {
		if isNilValue(col) {
			continue
		}

		ptr, err := mutationFieldPointer(col, dst)
		if err != nil {
			return nil, err
		}

		fields = append(fields, mutationField{
			column: col.Name(),
			value:  reflect.ValueOf(ptr).Elem(),
		})
	}

	return fields, nil
}

func insertFieldsForRecord(record mutationRecord) []mutationField {
	fields := make([]mutationField, 0, len(record.fields))
	for _, field := range record.fields {
		if field.column == record.pkField.column && record.autoIncr && isZeroMutationValue(field.value) {
			continue
		}

		fields = append(fields, field)
	}

	return fields
}

func updateFieldsForRecord(record mutationRecord) []mutationField {
	fields := make([]mutationField, 0, len(record.fields)-1)
	for _, field := range record.fields {
		if field.column == record.pkField.column || field.column == record.versionField.column {
			continue
		}

		fields = append(fields, field)
	}

	return fields
}

func optimisticLockMutationField(column string, fields []mutationField) (mutationField, error) {
	column = strings.TrimSpace(column)
	if column == "" {
		return mutationField{}, nil
	}

	for _, field := range fields {
		if field.column == column {
			return field, nil
		}
	}

	return mutationField{}, fmt.Errorf("mutation item is missing optimistic lock column %s", column)
}

func mutationFieldColumns(fields []mutationField) []string {
	cols := make([]string, 0, len(fields))
	for _, field := range fields {
		cols = append(cols, field.column)
	}

	return cols
}

func mutationFieldColumnsEqual(expected, actual []mutationField) bool {
	if len(expected) != len(actual) {
		return false
	}

	for i := range expected {
		if expected[i].column != actual[i].column {
			return false
		}
	}

	return true
}

func mutationFieldByColumn(fields []mutationField, column string) mutationField {
	for _, field := range fields {
		if field.column == column {
			return field
		}
	}

	return mutationField{}
}

func hasOptimisticMutation(record mutationRecord) bool {
	return record.versionField.column != ""
}

func buildMutationWhereClause(exec SQLExecutor, records []mutationRecord, argIndex *int) (string, []any, error) {
	pkSQL, err := quoteMutationIdentifier(exec, records[0].pkField.column)
	if err != nil {
		return "", nil, err
	}

	if !hasOptimisticMutation(records[0]) {
		placeholders := make([]string, 0, len(records))

		args := make([]any, 0, len(records))
		for _, record := range records {
			placeholders = append(placeholders, nextBindVar(exec, argIndex))
			args = append(args, record.pkField.value.Interface())
		}

		return pkSQL + " IN (" + strings.Join(placeholders, ", ") + ")", args, nil
	}

	versionSQL, err := quoteMutationIdentifier(exec, records[0].versionField.column)
	if err != nil {
		return "", nil, err
	}

	clauses := make([]string, 0, len(records))

	args := make([]any, 0, len(records)*2)
	for _, record := range records {
		clauses = append(
			clauses,
			"("+pkSQL+" = "+nextBindVar(exec, argIndex)+" AND "+versionSQL+" = "+nextBindVar(exec, argIndex)+")",
		)
		args = append(args, record.pkField.value.Interface(), record.versionField.value.Interface())
	}

	if len(clauses) == 1 {
		return clauses[0], args, nil
	}

	return "(" + strings.Join(clauses, " OR ") + ")", args, nil
}

func incrementMutationVersions(records []mutationRecord) {
	for _, record := range records {
		if !hasOptimisticMutation(record) || !record.versionField.value.IsValid() {
			continue
		}

		switch record.versionField.value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			record.versionField.value.SetInt(record.versionField.value.Int() + 1)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			record.versionField.value.SetUint(record.versionField.value.Uint() + 1)
		}
	}
}

func primaryMutationField(pkColumns []string, fields []mutationField) (mutationField, error) {
	if len(pkColumns) != 1 {
		return mutationField{}, errors.New("mutation item must define exactly one primary key column")
	}

	for _, field := range fields {
		if field.column == pkColumns[0] {
			return field, nil
		}
	}

	return mutationField{}, fmt.Errorf("mutation item is missing primary key column %s", pkColumns[0])
}

func quoteMutationIdentifier(exec SQLExecutor, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("identifier cannot be empty")
	}

	if !builtInIdentifierPattern.MatchString(name) {
		return "", fmt.Errorf("invalid identifier: %s", name)
	}

	if dialect := dialectForExecutor(exec); dialect != nil {
		return dialect.QuoteField(name), nil
	}

	return name, nil
}

func bindVar(exec SQLExecutor, index int) string {
	if dialect := dialectForExecutor(exec); dialect != nil {
		return dialect.BindVar(index)
	}

	return "?"
}

func isZeroMutationValue(value reflect.Value) bool {
	return value.IsZero()
}

func nextBindVar(exec SQLExecutor, index *int) string {
	placeholder := bindVar(exec, *index)
	*index++

	return placeholder
}

func assignMutationID(field reflect.Value, id int64) {
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		field.SetInt(id)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		field.SetUint(uint64(id))
	}
}

func assignBatchInsertIDs(exec SQLExecutor, records []mutationRecord, result sql.Result, omittedPrimaryKey bool) {
	if !omittedPrimaryKey || len(records) == 0 {
		return
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return
	}

	if len(records) == 1 {
		assignMutationID(records[0].pkField.value, lastID)
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil || rowsAffected != int64(len(records)) {
		slog.Warn("batch insert ID assignment skipped: rows affected mismatch",
			"expected", len(records),
			"actual", rowsAffected,
			"error", err,
		)

		return
	}

	dialect := dialectForExecutor(exec)
	if dialect == nil {
		return
	}

	startID, ok := dialect.BatchInsertStartID(lastID, rowsAffected)
	if !ok {
		return
	}

	for i, record := range records {
		assignMutationID(record.pkField.value, startID+int64(i))
	}
}

func mutationFieldPointer(col SQLColumn, holder Table) (any, error) {
	pointerFunc := col.scanPointer()
	if pointerFunc == nil {
		return nil, fmt.Errorf("column %s field pointer is nil", col.Name())
	}

	ptr := pointerFunc(holder)
	if ptr == nil {
		return nil, fmt.Errorf("column %s field pointer returned nil", col.Name())
	}

	value := reflect.ValueOf(ptr)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, fmt.Errorf("column %s field pointer must return a non-nil pointer", col.Name())
	}

	return ptr, nil
}

// wrappedExecutor wraps a standard SQL executor with dialect information.
type wrappedExecutor struct {
	SQLExecutor
	dialect Dialect
}

func (w wrappedExecutor) TSQDialect() Dialect {
	return w.dialect
}

// WrapExecutor wraps a SQLExecutor with dialect information.
func WrapExecutor(exec SQLExecutor, dialect Dialect) SQLExecutor {
	if exec == nil {
		return nil
	}

	if _, ok := exec.(dialectProvider); ok {
		return exec
	}

	return wrappedExecutor{
		SQLExecutor: exec,
		dialect:     dialect,
	}
}
