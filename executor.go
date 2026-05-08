// Package tsq provides type-safe SQL query helpers and code generation utilities.
package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
)

var (
	errInsertRequiresColumn        = errors.New("insert requires at least one column")
	errInsertLayoutMismatch        = errors.New("batch insert requires matching column layouts")
	errUpdateRequiresMutableColumn = errors.New("update requires at least one mutable column")
	errUpdateRequiresPrimaryKey    = errors.New("update requires a non-zero primary key")
	errUpdateLayoutMismatch        = errors.New("batch update requires matching column layouts")
	errDeleteRequiresPrimaryKey    = errors.New("delete requires a non-zero primary key")
	errMutationItemNil             = errors.New("mutation item cannot be nil")
	errMutationItemTableMethod     = errors.New("mutation item must implement Table() string")
	errMutationItemPointer         = errors.New("mutation item must be a non-nil pointer")
	errMutationItemStructPointer   = errors.New("mutation item must point to a struct")
	errMutationItemNoTaggedFields  = errors.New("mutation item has no db-tagged fields")
)

// SqlExecutor defines tsq's low-level execution surface.
// It exists for handwritten SQL fallbacks and to centralize common table
// mutation helpers, not for gorp compatibility.
type SqlExecutor interface {
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)

	SelectInt(ctx context.Context, query string, args ...any) (int64, error)
	SelectNullInt(ctx context.Context, query string, args ...any) (sql.NullInt64, error)
	SelectFloat(ctx context.Context, query string, args ...any) (float64, error)
	SelectNullFloat(ctx context.Context, query string, args ...any) (sql.NullFloat64, error)
	SelectStr(ctx context.Context, query string, args ...any) (string, error)
	SelectNullStr(ctx context.Context, query string, args ...any) (sql.NullString, error)

	Insert(ctx context.Context, dst ...Table) error
	Update(ctx context.Context, dst ...Table) (int64, error)
	Delete(ctx context.Context, dst ...Table) (int64, error)
}

// Dialect defines the interface for database dialect-specific operations.
// It mirrors the Dialect interface but is owned by tsq.
type Dialect interface {
	// QuoteField returns the dialect-specific quoted identifier
	QuoteField(field string) string
	// BindVar returns the dialect-specific bind variable placeholder
	BindVar(i int) string
	// CreateTableSuffix returns dialect-specific create table suffix
	CreateTableSuffix() string
	// CreateIndexSuffix returns dialect-specific create index suffix
	CreateIndexSuffix() string
	// DropIndexSuffix returns dialect-specific drop index suffix
	DropIndexSuffix() string
	// TruncateClause returns the dialect-specific truncate clause
	TruncateClause() string
	// AutoIncrementClause returns the dialect-specific auto-increment clause
	AutoIncrementClause() string
	// AutoIncrementBindValue returns the dialect-specific auto-increment bind value
	AutoIncrementBindValue() string
	// LastInsertIdReturningSuffix returns the dialect-specific returning suffix for last insert id
	LastInsertIdReturningSuffix(table, col string) string
	// AllTablesQuery returns the dialect-specific query to list all tables
	AllTablesQuery() string
	// CreateTableIfNotExistsSuffix returns the dialect-specific create if not exists suffix
	CreateTableIfNotExistsSuffix() string
	// HasConstraintsQuery returns the dialect-specific query to check constraints
	HasConstraintsQuery(string, string) string
}

// DbMap represents a database map that holds database connection and dialect information.
// It mirrors the DbMap structure but is owned by tsq.
type DbMap struct {
	Db      *sql.DB
	Dialect Dialect
}

type dbSchemaConfig struct {
	indexInitMode      IndexInitMode
	schemaEventHandler func(SchemaEvent)
}

var dbSchemaConfigs sync.Map

func defaultDBSchemaConfig() dbSchemaConfig {
	return dbSchemaConfig{indexInitMode: IndexInitUpsert}
}

func loadDBSchemaConfig(db *DbMap) dbSchemaConfig {
	if db == nil {
		return defaultDBSchemaConfig()
	}

	if cfg, ok := dbSchemaConfigs.Load(db); ok {
		return cfg.(dbSchemaConfig)
	}

	return defaultDBSchemaConfig()
}

func storeDBSchemaConfig(db *DbMap, cfg dbSchemaConfig) {
	if db == nil {
		return
	}

	if cfg.indexInitMode == "" {
		cfg.indexInitMode = IndexInitUpsert
	}

	if cfg.indexInitMode == IndexInitUpsert && cfg.schemaEventHandler == nil {
		dbSchemaConfigs.Delete(db)
		return
	}

	dbSchemaConfigs.Store(db, cfg)
}

// Query executes a query and returns rows.
func (db *DbMap) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if db == nil || db.Db == nil {
		return nil, nil
	}

	rows, err := db.Db.QueryContext(ctx, query, args...)

	return rows, errors.Trace(err)
}

// QueryRow executes a query that returns a single row.
func (db *DbMap) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	if db == nil || db.Db == nil {
		return nil
	}

	return db.Db.QueryRowContext(ctx, query, args...)
}

// Exec executes a query without returning rows.
func (db *DbMap) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if db == nil || db.Db == nil {
		return nil, nil
	}

	res, err := db.Db.ExecContext(ctx, query, args...)

	return res, errors.Trace(err)
}

// SelectInt executes a query and returns a single integer result.
func (db *DbMap) SelectInt(ctx context.Context, query string, args ...any) (int64, error) {
	var result sql.NullInt64

	err := db.QueryRow(ctx, query, args...).Scan(&result)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return result.Int64, nil
}

// SelectNullInt executes a query and returns a nullable integer result.
func (db *DbMap) SelectNullInt(ctx context.Context, query string, args ...any) (sql.NullInt64, error) {
	var result sql.NullInt64
	err := db.QueryRow(ctx, query, args...).Scan(&result)

	return result, errors.Trace(err)
}

// SelectFloat executes a query and returns a single float result.
func (db *DbMap) SelectFloat(ctx context.Context, query string, args ...any) (float64, error) {
	var result sql.NullFloat64

	err := db.QueryRow(ctx, query, args...).Scan(&result)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return result.Float64, nil
}

// SelectNullFloat executes a query and returns a nullable float result.
func (db *DbMap) SelectNullFloat(ctx context.Context, query string, args ...any) (sql.NullFloat64, error) {
	var result sql.NullFloat64
	err := db.QueryRow(ctx, query, args...).Scan(&result)

	return result, errors.Trace(err)
}

// SelectStr executes a query and returns a single string result.
func (db *DbMap) SelectStr(ctx context.Context, query string, args ...any) (string, error) {
	var result sql.NullString

	err := db.QueryRow(ctx, query, args...).Scan(&result)
	if err != nil {
		return "", errors.Trace(err)
	}

	return result.String, nil
}

// SelectNullStr executes a query and returns a nullable string result.
func (db *DbMap) SelectNullStr(ctx context.Context, query string, args ...any) (sql.NullString, error) {
	var result sql.NullString
	err := db.QueryRow(ctx, query, args...).Scan(&result)

	return result, errors.Trace(err)
}

// CreateTablesIfNotExists creates all tables if they don't exist.
func (db *DbMap) CreateTablesIfNotExists() error {
	// This is typically used during initialization
	// Implementation depends on registered tables
	return nil
}

type mutationField struct {
	column string
	value  reflect.Value
}

type mutationRecord struct {
	tableName string
	fields    []mutationField
	pkField   mutationField
	autoIncr  bool
}

// Insert inserts objects into the database.
func (db *DbMap) Insert(ctx context.Context, dst ...Table) error {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return errors.Trace(err)
	}

	for _, group := range groupInsertRecords(records) {
		if err := db.insertBatch(ctx, group); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Update updates objects in the database.
func (db *DbMap) Update(ctx context.Context, dst ...Table) (int64, error) {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return 0, errors.Trace(err)
	}

	var total int64

	for _, group := range groupUpdateRecords(records) {
		affected, err := db.updateBatch(ctx, group)
		if err != nil {
			return total, errors.Trace(err)
		}

		total += affected
	}

	return total, nil
}

// Delete deletes objects from the database.
func (db *DbMap) Delete(ctx context.Context, dst ...Table) (int64, error) {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return 0, errors.Trace(err)
	}

	var total int64

	for _, group := range groupDeleteRecords(records) {
		affected, err := db.deleteBatch(ctx, group)
		if err != nil {
			return total, errors.Trace(err)
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
			return nil, errors.Trace(err)
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

func (db *DbMap) insertBatch(ctx context.Context, records []mutationRecord) error {
	if len(records) == 0 {
		return nil
	}

	insertFields := insertFieldsForRecord(records[0])
	if len(insertFields) == 0 {
		return errors.Trace(errInsertRequiresColumn)
	}

	for _, record := range records[1:] {
		if !mutationFieldColumnsEqual(insertFields, insertFieldsForRecord(record)) {
			return errors.Trace(errInsertLayoutMismatch)
		}
	}

	tableSQL, err := db.quoteMutationIdentifier(records[0].tableName)
	if err != nil {
		return errors.Trace(err)
	}

	quotedCols := make([]string, 0, len(insertFields))

	for _, field := range insertFields {
		col, err := db.quoteMutationIdentifier(field.column)
		if err != nil {
			return errors.Trace(err)
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
			placeholders = append(placeholders, db.nextBindVar(&argIndex))
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

	result, err := db.Exec(ctx, query, args...)
	if err != nil {
		return errors.Trace(err)
	}

	assignBatchInsertIDs(db, records, result, len(insertFields) != len(records[0].fields))

	return nil
}

func (db *DbMap) updateBatch(ctx context.Context, records []mutationRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	updateFields := updateFieldsForRecord(records[0])
	if len(updateFields) == 0 {
		return 0, errors.Trace(errUpdateRequiresMutableColumn)
	}

	for _, record := range records {
		if isZeroMutationValue(record.pkField.value) {
			return 0, errors.Trace(errUpdateRequiresPrimaryKey)
		}

		if !mutationFieldColumnsEqual(updateFields, updateFieldsForRecord(record)) {
			return 0, errors.Trace(errUpdateLayoutMismatch)
		}
	}

	tableSQL, err := db.quoteMutationIdentifier(records[0].tableName)
	if err != nil {
		return 0, errors.Trace(err)
	}

	pkSQL, err := db.quoteMutationIdentifier(records[0].pkField.column)
	if err != nil {
		return 0, errors.Trace(err)
	}

	var (
		argIndex   int
		args       []any
		setClauses = make([]string, 0, len(updateFields))
	)

	for _, field := range updateFields {
		colSQL, err := db.quoteMutationIdentifier(field.column)
		if err != nil {
			return 0, errors.Trace(err)
		}

		var clause strings.Builder
		clause.WriteString(colSQL)
		clause.WriteString(" = CASE ")
		clause.WriteString(pkSQL)

		for _, record := range records {
			recordField := mutationFieldByColumn(record.fields, field.column)

			clause.WriteString(" WHEN ")
			clause.WriteString(db.nextBindVar(&argIndex))
			clause.WriteString(" THEN ")
			clause.WriteString(db.nextBindVar(&argIndex))

			args = append(args, record.pkField.value.Interface(), recordField.value.Interface())
		}

		clause.WriteString(" ELSE ")
		clause.WriteString(colSQL)
		clause.WriteString(" END")
		setClauses = append(setClauses, clause.String())
	}

	wherePlaceholders := make([]string, 0, len(records))
	for _, record := range records {
		wherePlaceholders = append(wherePlaceholders, db.nextBindVar(&argIndex))
		args = append(args, record.pkField.value.Interface())
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s IN (%s)",
		tableSQL,
		strings.Join(setClauses, ", "),
		pkSQL,
		strings.Join(wherePlaceholders, ", "),
	)

	result, err := db.Exec(ctx, query, args...)
	if err != nil {
		return 0, errors.Trace(err)
	}

	rowsAffected, err := result.RowsAffected()

	return rowsAffected, errors.Trace(err)
}

func (db *DbMap) deleteBatch(ctx context.Context, records []mutationRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	for _, record := range records {
		if isZeroMutationValue(record.pkField.value) {
			return 0, errors.Trace(errDeleteRequiresPrimaryKey)
		}
	}

	tableSQL, err := db.quoteMutationIdentifier(records[0].tableName)
	if err != nil {
		return 0, errors.Trace(err)
	}

	pkSQL, err := db.quoteMutationIdentifier(records[0].pkField.column)
	if err != nil {
		return 0, errors.Trace(err)
	}

	var (
		argIndex     int
		args         = make([]any, 0, len(records))
		placeholders = make([]string, 0, len(records))
	)

	for _, record := range records {
		placeholders = append(placeholders, db.nextBindVar(&argIndex))
		args = append(args, record.pkField.value.Interface())
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		tableSQL,
		pkSQL,
		strings.Join(placeholders, ", "),
	)

	result, err := db.Exec(ctx, query, args...)
	if err != nil {
		return 0, errors.Trace(err)
	}

	rowsAffected, err := result.RowsAffected()

	return rowsAffected, errors.Trace(err)
}

func mutationMetadata(dst Table) (mutationRecord, error) {
	if isNilValue(dst) {
		return mutationRecord{}, errors.Trace(errMutationItemNil)
	}

	value := reflect.ValueOf(dst)
	if value.Kind() != reflect.Ptr || value.IsNil() {
		return mutationRecord{}, errors.Trace(errMutationItemPointer)
	}

	if value.Elem().Kind() != reflect.Struct {
		return mutationRecord{}, errors.Trace(errMutationItemStructPointer)
	}

	fields, err := collectMutationFields(dst)
	if err != nil {
		return mutationRecord{}, errors.Trace(err)
	}

	if len(fields) == 0 {
		return mutationRecord{}, errors.Trace(errMutationItemNoTaggedFields)
	}

	pkField, err := primaryMutationField(dst.PrimaryKeys(), fields)
	if err != nil {
		return mutationRecord{}, errors.Trace(err)
	}

	return mutationRecord{
		tableName: dst.Table(),
		fields:    fields,
		pkField:   pkField,
		autoIncr:  dst.AutoIncrement(),
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
			return nil, errors.Trace(err)
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
		if field.column == record.pkField.column {
			continue
		}

		fields = append(fields, field)
	}

	return fields
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

func primaryMutationField(pkColumns []string, fields []mutationField) (mutationField, error) {
	if len(pkColumns) != 1 {
		return mutationField{}, errors.New("mutation item must define exactly one primary key column")
	}

	for _, field := range fields {
		if field.column == pkColumns[0] {
			return field, nil
		}
	}

	return mutationField{}, errors.Errorf("mutation item is missing primary key column %s", pkColumns[0])
}

func (db *DbMap) quoteMutationIdentifier(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("identifier cannot be empty")
	}

	if !builtInIdentifierPattern.MatchString(name) {
		return "", errors.Errorf("invalid identifier: %s", name)
	}

	if db != nil && db.Dialect != nil {
		return db.Dialect.QuoteField(name), nil
	}

	return name, nil
}

func (db *DbMap) bindVar(index int) string {
	if db != nil && db.Dialect != nil {
		return db.Dialect.BindVar(index)
	}

	return "?"
}

func isZeroMutationValue(value reflect.Value) bool {
	return value.IsZero()
}

func (db *DbMap) nextBindVar(index *int) string {
	placeholder := db.bindVar(*index)
	(*index)++

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

func assignBatchInsertIDs(db *DbMap, records []mutationRecord, result sql.Result, omittedPrimaryKey bool) {
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
		return
	}

	var startID int64

	switch db.Dialect.(type) {
	case SqliteDialect, *SqliteDialect:
		startID = lastID - rowsAffected + 1
	case MySQLDialect, *MySQLDialect:
		startID = lastID
	default:
		return
	}

	for i, record := range records {
		assignMutationID(record.pkField.value, startID+int64(i))
	}
}

func mutationFieldPointer(col SQLColumn, holder Table) (any, error) {
	pointerFunc := col.scanPointer()
	if pointerFunc == nil {
		return nil, errors.Errorf("column %s field pointer is nil", col.Name())
	}

	ptr := pointerFunc(holder)
	if ptr == nil {
		return nil, errors.Errorf("column %s field pointer returned nil", col.Name())
	}

	value := reflect.ValueOf(ptr)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.Errorf("column %s field pointer must return a non-nil pointer", col.Name())
	}

	return ptr, nil
}

// AddTableWithName registers a table mapping (stub implementation for gorp compatibility)
func (db *DbMap) AddTableWithName(dst any, name string) *DbMapTable {
	// This is a stub implementation kept for gorp compatibility
	return &DbMapTable{}
}

// DbMapTable is a stub for method chaining compatibility
type DbMapTable struct{}

// SetKeys sets the primary keys (stub for gorp compatibility)
func (t *DbMapTable) SetKeys(autoincr bool, keynames ...string) *DbMapTable {
	return t
}

// SetVersionCol sets the version column (stub for gorp compatibility)
func (t *DbMapTable) SetVersionCol(name string) *DbMapTable {
	return t
}

// SqliteDialect is the SQLite dialect implementation.
type SqliteDialect struct{}

func (d SqliteDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

func (d SqliteDialect) BindVar(i int) string {
	return "?"
}

func (d SqliteDialect) CreateTableSuffix() string {
	return ";"
}

func (d SqliteDialect) CreateIndexSuffix() string {
	return ";"
}

func (d SqliteDialect) DropIndexSuffix() string {
	return ";"
}

func (d SqliteDialect) TruncateClause() string {
	return "DELETE FROM"
}

func (d SqliteDialect) AutoIncrementClause() string {
	return "AUTOINCREMENT"
}

func (d SqliteDialect) AutoIncrementBindValue() string {
	return ""
}

func (d SqliteDialect) LastInsertIdReturningSuffix(table, col string) string {
	return ""
}

func (d SqliteDialect) AllTablesQuery() string {
	return "SELECT name FROM sqlite_master WHERE type='table'"
}

func (d SqliteDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d SqliteDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

// MySQLDialect is the MySQL dialect implementation.
type MySQLDialect struct{}

func (d MySQLDialect) QuoteField(f string) string {
	return "`" + f + "`"
}

func (d MySQLDialect) BindVar(i int) string {
	return "?"
}

func (d MySQLDialect) CreateTableSuffix() string {
	return ";"
}

func (d MySQLDialect) CreateIndexSuffix() string {
	return ";"
}

func (d MySQLDialect) DropIndexSuffix() string {
	return ";"
}

func (d MySQLDialect) TruncateClause() string {
	return "TRUNCATE"
}

func (d MySQLDialect) AutoIncrementClause() string {
	return "AUTO_INCREMENT"
}

func (d MySQLDialect) AutoIncrementBindValue() string {
	return ""
}

func (d MySQLDialect) LastInsertIdReturningSuffix(table, col string) string {
	return ""
}

func (d MySQLDialect) AllTablesQuery() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
}

func (d MySQLDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d MySQLDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

// PostgresDialect is the PostgreSQL dialect implementation.
type PostgresDialect struct{}

func (d PostgresDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

func (d PostgresDialect) BindVar(i int) string {
	// PostgreSQL uses $1, $2, etc for bind variables (1-indexed)
	return "$" + strconv.Itoa(i+1)
}

func (d PostgresDialect) CreateTableSuffix() string {
	return ";"
}

func (d PostgresDialect) CreateIndexSuffix() string {
	return ";"
}

func (d PostgresDialect) DropIndexSuffix() string {
	return ";"
}

func (d PostgresDialect) TruncateClause() string {
	return "TRUNCATE"
}

func (d PostgresDialect) AutoIncrementClause() string {
	return ""
}

func (d PostgresDialect) AutoIncrementBindValue() string {
	return ""
}

func (d PostgresDialect) LastInsertIdReturningSuffix(table, col string) string {
	return " RETURNING " + d.QuoteField(col)
}

func (d PostgresDialect) AllTablesQuery() string {
	return "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
}

func (d PostgresDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d PostgresDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

// wrappedExecutor wraps a standard SQL executor with dialect information.
type wrappedExecutor struct {
	SqlExecutor
	dialect Dialect
}

func (w wrappedExecutor) TSQDialect() Dialect {
	return w.dialect
}

// WrapExecutor wraps a SqlExecutor with dialect information.
func WrapExecutor(exec SqlExecutor, dialect Dialect) SqlExecutor {
	if exec == nil {
		return nil
	}

	if _, ok := exec.(dialectProvider); ok {
		return exec
	}

	return wrappedExecutor{
		SqlExecutor: exec,
		dialect:     dialect,
	}
}

// WrapDBMapExecutor wraps a SqlExecutor with dialect from DbMap.
func WrapDBMapExecutor(exec SqlExecutor, db *DbMap) SqlExecutor {
	if db == nil {
		return exec
	}

	return WrapExecutor(exec, db.Dialect)
}
