// Package tsq provides type-safe SQL query helpers and code generation utilities.
package tsq

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"
)

// SqlExecutor defines the interface for executing SQL queries.
// It mirrors the gorp.SqlExecutor interface but is owned by tsq.
type SqlExecutor interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
	WithContext(ctx context.Context) SqlExecutor
	SelectOne(dst interface{}, query string, args ...interface{}) error
	SelectInt(query string, args ...interface{}) (int64, error)
	SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error)
	SelectFloat(query string, args ...interface{}) (float64, error)
	SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error)
	SelectStr(query string, args ...interface{}) (string, error)
	SelectNullStr(query string, args ...interface{}) (sql.NullString, error)
	Select(dst interface{}, query string, args ...interface{}) (int, error)
	Insert(dst ...interface{}) error
	Update(dst ...interface{}) (int64, error)
	Delete(dst ...interface{}) (int64, error)
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
	ctx     context.Context
}

// Query executes a query and returns rows.
func (db *DbMap) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if db == nil || db.Db == nil {
		return nil, nil
	}
	if db.ctx != nil {
		return db.Db.QueryContext(db.ctx, query, args...)
	}
	return db.Db.Query(query, args...)
}

// QueryRow executes a query that returns a single row.
func (db *DbMap) QueryRow(query string, args ...interface{}) *sql.Row {
	if db == nil || db.Db == nil {
		return nil
	}
	if db.ctx != nil {
		return db.Db.QueryRowContext(db.ctx, query, args...)
	}
	return db.Db.QueryRow(query, args...)
}

// Exec executes a query without returning rows.
func (db *DbMap) Exec(query string, args ...interface{}) (sql.Result, error) {
	if db == nil || db.Db == nil {
		return nil, nil
	}
	if db.ctx != nil {
		return db.Db.ExecContext(db.ctx, query, args...)
	}
	return db.Db.Exec(query, args...)
}

// WithContext returns a new DbMap with the specified context.
func (db *DbMap) WithContext(ctx context.Context) SqlExecutor {
	if db == nil {
		return nil
	}
	return &DbMap{
		Db:      db.Db,
		Dialect: db.Dialect,
		ctx:     ctx,
	}
}

// SelectOne executes a query and scans a single row into dst.
func (db *DbMap) SelectOne(dst interface{}, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	// Use scanRow for proper struct tag handling
	return scanRow(rows, dst)
}

// scanRow handles scanning with struct tag support
func scanRow(rows *sql.Rows, dst interface{}) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	// Try direct Scan first (works for simple types)
	switch reflect.TypeOf(dst).Kind() {
	case reflect.Ptr:
		elem := reflect.ValueOf(dst).Elem()
		switch elem.Kind() {
		case reflect.Struct:
			// Use scanStruct for struct types with tag support
			return scanStruct(rows, cols, dst)
		default:
			// Simple types like *int, *string - use direct Scan
			return rows.Scan(dst)
		}
	}

	return rows.Scan(dst)
}

// scanStruct handles scanning into structs with db tag support
func scanStruct(rows *sql.Rows, cols []string, dst interface{}) error {
	values := make([]interface{}, len(cols))
	for i := range cols {
		values[i] = new(interface{})
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	// Map columns to struct fields
	elem := reflect.ValueOf(dst).Elem()
	for i, col := range cols {
		val := reflect.ValueOf(values[i]).Elem().Elem()
		
		// Find field with matching db tag
		found := false
		for j := 0; j < elem.NumField(); j++ {
			field := elem.Type().Field(j)
			tag := field.Tag.Get("db")
			if tag == col || (tag == "" && strings.EqualFold(field.Name, col)) {
				if elem.Field(j).CanSet() && val.IsValid() {
					// Handle type conversion
					setField(elem.Field(j), val)
				}
				found = true
				break
			}
		}

		if !found && !isNilInterface(val) {
			// Try case-insensitive match
			for j := 0; j < elem.NumField(); j++ {
				field := elem.Type().Field(j)
				if strings.EqualFold(field.Name, col) {
					if elem.Field(j).CanSet() && val.IsValid() {
						setField(elem.Field(j), val)
					}
					break
				}
			}
		}
	}

	return nil
}

// setField sets a field value with type conversion
func setField(field reflect.Value, val reflect.Value) {
	if field.Type() == val.Type() {
		field.Set(val)
		return
	}

	// Handle type conversion
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field.SetInt(val.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field.SetInt(int64(val.Uint()))
		case reflect.Float32, reflect.Float64:
			field.SetInt(int64(val.Float()))
		}
	case reflect.String:
		if val.Kind() == reflect.String {
			field.SetString(val.String())
		}
	case reflect.Bool:
		if val.Kind() == reflect.Bool {
			field.SetBool(val.Bool())
		}
	case reflect.Float32, reflect.Float64:
		switch val.Kind() {
		case reflect.Float32, reflect.Float64:
			field.SetFloat(val.Float())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field.SetFloat(float64(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field.SetFloat(float64(val.Uint()))
		}
	default:
		if field.CanSet() {
			field.Set(val)
		}
	}
}

func isNilInterface(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	if v.Kind() == reflect.Interface && v.IsNil() {
		return true
	}
	return false
}

// SelectInt executes a query and returns a single integer result.
func (db *DbMap) SelectInt(query string, args ...interface{}) (int64, error) {
	var result sql.NullInt64
	err := db.SelectOne(&result, query, args...)
	if err != nil {
		return 0, err
	}
	return result.Int64, nil
}

// SelectNullInt executes a query and returns a nullable integer result.
func (db *DbMap) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	var result sql.NullInt64
	err := db.SelectOne(&result, query, args...)
	return result, err
}

// SelectFloat executes a query and returns a single float result.
func (db *DbMap) SelectFloat(query string, args ...interface{}) (float64, error) {
	var result sql.NullFloat64
	err := db.SelectOne(&result, query, args...)
	if err != nil {
		return 0, err
	}
	return result.Float64, nil
}

// SelectNullFloat executes a query and returns a nullable float result.
func (db *DbMap) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	var result sql.NullFloat64
	err := db.SelectOne(&result, query, args...)
	return result, err
}

// SelectStr executes a query and returns a single string result.
func (db *DbMap) SelectStr(query string, args ...interface{}) (string, error) {
	var result sql.NullString
	err := db.SelectOne(&result, query, args...)
	if err != nil {
		return "", err
	}
	return result.String, nil
}

// SelectNullStr executes a query and returns a nullable string result.
func (db *DbMap) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	var result sql.NullString
	err := db.SelectOne(&result, query, args...)
	return result, err
}

// Select executes a query and scans multiple rows into dst.
func (db *DbMap) Select(dst interface{}, query string, args ...interface{}) (int, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return 0, errors.New("dst must be a pointer")
	}

	sliceVal := dstVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return 0, errors.New("dst must be a pointer to a slice")
	}

	count := 0
	elemType := sliceVal.Type().Elem()

	for rows.Next() {
		// Create a new element
		elem := reflect.New(elemType)

		// Scan the row into the element
		if elemType.Kind() == reflect.Struct {
			if err := scanStruct(rows, cols, elem.Interface()); err != nil {
				return 0, err
			}
		} else {
			// Simple type
			if err := rows.Scan(elem.Interface()); err != nil {
				return 0, err
			}
		}

		// Append to the slice
		sliceVal.Set(reflect.Append(sliceVal, elem.Elem()))
		count++
	}

	return count, rows.Err()
}

// CreateTablesIfNotExists creates all tables if they don't exist.
func (db *DbMap) CreateTablesIfNotExists() error {
	// This is typically used during initialization
	// Implementation depends on registered tables
	return nil
}

// Insert inserts objects into the database (stub implementation - not used by tsq)
func (db *DbMap) Insert(dst ...interface{}) error {
	// This is a stub implementation kept for gorp compatibility
	// TSQ does not use ORM-style inserts
	return nil
}

// Update updates objects in the database (stub implementation - not used by tsq)
func (db *DbMap) Update(dst ...interface{}) (int64, error) {
	// This is a stub implementation kept for gorp compatibility
	// TSQ does not use ORM-style updates
	return 0, nil
}

// Delete deletes objects from the database (stub implementation - not used by tsq)
func (db *DbMap) Delete(dst ...interface{}) (int64, error) {
	// This is a stub implementation kept for gorp compatibility
	// TSQ does not use ORM-style deletes
	return 0, nil
}

// AddTableWithName registers a table mapping (stub implementation for gorp compatibility)
func (db *DbMap) AddTableWithName(dst interface{}, name string) *DbMapTable {
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
