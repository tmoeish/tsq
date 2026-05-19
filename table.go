package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ================================================
// 表注册和管理
// ================================================

// RegistrationErrorType identifies a table-registration failure category.
type RegistrationErrorType string

const (
	// RegistrationErrorNilTable means RegisterTable received a nil table.
	RegistrationErrorNilTable RegistrationErrorType = "nil_table"
	// RegistrationErrorNilInitFunc means RegisterTable received a nil init hook.
	RegistrationErrorNilInitFunc RegistrationErrorType = "nil_init_func"
	// RegistrationErrorDuplicate means the same table key was registered twice.
	RegistrationErrorDuplicate RegistrationErrorType = "duplicate"
	// RegistrationErrorNilRuntime means a method was called on a nil runtime.
	RegistrationErrorNilRuntime RegistrationErrorType = "nil_runtime"
)

// IndexInitMode controls how tsq handles declared indexes during Init.
type IndexInitMode string

const (
	// IndexInitSkip leaves declared indexes untouched.
	IndexInitSkip IndexInitMode = "skip"
	// IndexInitUpsert creates missing declared indexes when possible.
	IndexInitUpsert IndexInitMode = "upsert"
	// IndexInitValidate fails when a declared index is missing or mismatched.
	IndexInitValidate IndexInitMode = "validate"
)

// SchemaEventKind classifies emitted schema events.
type SchemaEventKind string

const (
	// SchemaEventCreateTable reports table creation.
	SchemaEventCreateTable SchemaEventKind = "create_table"
	// SchemaEventCreateIndex reports index creation.
	SchemaEventCreateIndex SchemaEventKind = "create_index"
	// SchemaEventValidateIndex reports successful index validation.
	SchemaEventValidateIndex SchemaEventKind = "validate_index"
	// SchemaEventSkipIndex reports that index work was skipped.
	SchemaEventSkipIndex SchemaEventKind = "skip_index"
)

// SchemaEvent reports a schema action performed or skipped during Init.
type SchemaEvent struct {
	Kind  SchemaEventKind
	Table string
	Name  string
	SQL   string
}

// ErrIndexMissing reports that an expected index was not found.
type ErrIndexMissing struct {
	Table  string
	Name   string
	Fields []string
	Unique bool
}

// Error implements error.
func (e *ErrIndexMissing) Error() string {
	if e == nil {
		return ""
	}

	return fmt.Sprintf(
		"index %s on table %s is missing; expected fields %v; enable IndexInitUpsert or create the index in your migration",
		e.Name,
		e.Table,
		e.Fields,
	)
}

// RegistrationError reports a table-registration failure.
type RegistrationError struct {
	Type      RegistrationErrorType
	TableName string
	Message   string
}

// Error implements error.
func (e *RegistrationError) Error() string {
	return e.Message
}

type registry struct {
	mu     sync.RWMutex
	tables map[string]*registeredTable
}

func newRegistry() *registry {
	return &registry{
		tables: make(map[string]*registeredTable),
	}
}

// InitOptions controls runtime initialization behavior.
type InitOptions struct {
	UpsertIndexes      bool
	IndexMode          IndexInitMode
	Tracers            []Tracer
	SchemaEventHandler func(SchemaEvent)
	// IdentifierValidationMode controls how to handle identifier length violations:
	// "strict" = fail if any identifier exceeds dialect limits (default for most dialects)
	// "warn"   = log warnings but allow (for permissive databases)
	// "skip"   = no validation (useful for dynamic schemas)
	IdentifierValidationMode string
}

// RegisterTable registers a table in the global registry.
// Returns an error if registration fails.
func RegisterTable(
	table Table,
	initFunc func(db *Engine) error,
) error {
	return defaultRuntime.RegisterTable(table, initFunc)
}

func (r *registry) Register(
	table Table,
	initFunc func(db *Engine) error,
) error {
	if isNilValue(table) {
		return &RegistrationError{
			Type:    RegistrationErrorNilTable,
			Message: "registered table cannot be nil",
		}
	}

	if initFunc == nil {
		return &RegistrationError{
			Type:      RegistrationErrorNilInitFunc,
			TableName: fmt.Sprintf("%v", table),
			Message:   "init function cannot be nil",
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := registeredTableKey(table)
	if _, exists := r.tables[key]; exists {
		return &RegistrationError{
			Type:      RegistrationErrorDuplicate,
			TableName: key,
			Message:   fmt.Sprintf("table %s is already registered", key),
		}
	}

	r.tables[key] = &registeredTable{
		Table:    table,
		InitFunc: initFunc,
	}

	return nil
}

type registeredTable struct {
	Table
	InitFunc func(db *Engine) error
}

// ================================================
// 表接口定义
// ================================================

// Table defines a physical SQL table source.
// Unlike Result, a Table is both a scan owner and a mutation target, and it
// exposes stable column and primary-key metadata for metadata-driven execution.
type Table interface {
	Owner
	Cols() []SQLColumn
	Table() string
	SearchColumns() []SearchColumn
	PrimaryKeys() []string
	AutoIncrement() bool
	VersionColumn() string
}

// ================================================
// 数据库初始化
// ================================================

// Init initializes indexes and tracers for the default runtime with default options.
func Init(db *sql.DB, dialect Dialect) error {
	return defaultRuntime.Init(db, dialect)
}

// InitWithOpts initializes indexes and tracers for the default runtime with explicit options.
func InitWithOpts(db *sql.DB, dialect Dialect, options *InitOptions) error {
	return defaultRuntime.InitWithOpts(db, dialect, options)
}

// CurrentEngine returns the Engine of the default runtime.
func CurrentEngine() *Engine {
	return defaultRuntime.Engine()
}

func registeredTableKey(table Table) string {
	if table == nil {
		return ""
	}

	if schemaTable, ok := table.(schemaTabler); ok && strings.TrimSpace(schemaTable.Schema()) != "" {
		return schemaTable.Schema() + "." + table.Table()
	}

	return table.Table()
}

func snapshotRegisteredTables() []*registeredTable {
	return defaultRuntime.snapshotRegisteredTables()
}

func (r *registry) Snapshot() []*registeredTable {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.tables))
	for name := range r.tables {
		names = append(names, name)
	}

	sort.Strings(names)

	result := make([]*registeredTable, 0, len(names))
	for _, name := range names {
		result = append(result, r.tables[name])
	}

	return result
}

// ================================================
// 索引管理 - 数据库方言检测
// ================================================

// UpsertIndex ensures an index exists, with database dialect-specific implementation
func UpsertIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	if db == nil {
		return errors.New("engine cannot be nil")
	}

	if db.Dialect == nil {
		return errors.New("database dialect is required")
	}

	if err := validateIndexIdentifiers(table, idx, fields); err != nil {
		return err
	}

	mode := db.effectiveIndexInitMode()
	if mode == IndexInitSkip {
		return db.emitSchemaEvent(SchemaEvent{
			Kind:  SchemaEventSkipIndex,
			Table: table,
			Name:  idx,
		})
	}

	definition, found, err := inspectIndexDefinition(db, table, idx)
	if err != nil {
		return err
	}

	if found {
		if err := validateIndexDefinition(table, unique, idx, fields, definition); err != nil {
			return err
		}

		if err := db.emitSchemaEvent(SchemaEvent{
			Kind:  SchemaEventValidateIndex,
			Table: table,
			Name:  idx,
		}); err != nil {
			return err
		}

		return nil
	}

	if mode == IndexInitValidate {
		return &ErrIndexMissing{
			Table:  table,
			Name:   idx,
			Fields: append([]string(nil), fields...),
			Unique: unique,
		}
	}

	return db.Dialect.EnsureIndex(db, table, unique, idx, fields)
}

func (e *Engine) effectiveIndexInitMode() IndexInitMode {
	return loadDBSchemaConfig(e).indexInitMode
}

func (e *Engine) emitSchemaEvent(event SchemaEvent) (err error) {
	handler := loadDBSchemaConfig(e).schemaEventHandler
	if e == nil || handler == nil {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf(
				"schema event handler panicked for %s on %s: %v",
				event.Kind,
				event.Table,
				r,
			)
		}
	}()

	handler(event)

	return nil
}

func resolveIndexInitMode(options *InitOptions) IndexInitMode {
	if options == nil {
		return IndexInitSkip
	}

	if options.IndexMode != "" {
		return options.IndexMode
	}

	if options.UpsertIndexes {
		return IndexInitUpsert
	}

	return IndexInitSkip
}

func validateIndexInitMode(mode IndexInitMode) error {
	switch mode {
	case IndexInitSkip, IndexInitUpsert, IndexInitValidate:
		return nil
	default:
		return fmt.Errorf("invalid index init mode %q", mode)
	}
}

func inspectIndexDefinition(
	db *Engine,
	table string,
	idx string,
) (IndexDefinition, bool, error) {
	return db.Dialect.InspectIndexDefinition(db, table, idx)
}

func validateIndexDefinition(
	table string,
	unique bool,
	idx string,
	fields []string,
	existing IndexDefinition,
) error {
	if existing.Table != table {
		return fmt.Errorf(
			"index %s already exists on table %s, expected table %s",
			idx,
			existing.Table,
			table,
		)
	}

	if existing.Unique != unique || !sameOrderedFields(existing.Fields, fields) {
		return fmt.Errorf(
			"index %s on table %s has definition unique=%t fields=%v, expected unique=%t fields=%v",
			idx,
			table,
			existing.Unique,
			existing.Fields,
			unique,
			fields,
		)
	}

	return nil
}

func sameOrderedFields(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}

func columnsCSV(fields []string) string {
	return strings.Join(fields, ",")
}

func parseColumnsCSV(csv string) []string {
	if csv == "" {
		return nil
	}

	return strings.Split(csv, ",")
}

func finishCreateIndex(
	db *Engine,
	table string,
	unique bool,
	idx string,
	fields []string,
	createErr error,
) error {
	if createErr == nil {
		return nil
	}

	definition, found, err := inspectIndexDefinition(db, table, idx)
	if err == nil && found && validateIndexDefinition(table, unique, idx, fields, definition) == nil {
		return nil
	}

	return createErr
}

func validateIndexIdentifiers(table, idx string, fields []string) error {
	if err := validateBuiltInIdentifier(table); err != nil {
		return fmt.Errorf("%s: %w", "invalid table name", err)
	}

	if err := validateBuiltInIdentifier(idx); err != nil {
		return fmt.Errorf("%s: %w", "invalid index name", err)
	}

	if len(fields) == 0 {
		return errors.New("index fields cannot be empty")
	}

	for _, field := range fields {
		if err := validateBuiltInIdentifier(field); err != nil {
			return fmt.Errorf("invalid index field %s"+": %w", field, err)
		}
	}

	return nil
}

func validateBuiltInIdentifier(name string) error {
	if !builtInIdentifierPattern.MatchString(name) {
		return fmt.Errorf("invalid SQL identifier: %s", name)
	}

	return nil
}

func quoteDialectIdentifier(dialect Dialect, name string) (string, error) {
	if err := validateBuiltInIdentifier(name); err != nil {
		return "", err
	}

	if dialect == nil {
		return canonicalQuoteIdentifier(name), nil
	}

	if err := dialect.ValidateIdentifier(name); err != nil {
		return "", err
	}

	return dialect.QuoteField(name), nil
}

func quoteDialectIdentifiers(dialect Dialect, names []string) ([]string, error) {
	quoted := make([]string, len(names))

	for i, name := range names {
		value, err := quoteDialectIdentifier(dialect, name)
		if err != nil {
			return nil, err
		}

		quoted[i] = value
	}

	return quoted, nil
}

// ================================================
// MySQL 索引管理
// ================================================

// ensureMySQLIndex ensures an index exists in MySQL
func ensureMySQLIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return createMySQLIndex(db, table, unique, idx, fields)
}

func inspectMySQLIndexDefinition(
	engine *Engine,
	table string,
	idx string,
) (IndexDefinition, bool, error) {
	type row struct {
		Table   string         `db:"table_name"`
		Unique  int            `db:"is_unique"`
		Columns sql.NullString `db:"columns_csv"`
	}

	var existing row

	err := engine.QueryRowContext(context.Background(), `
		SELECT
			table_name,
			CASE WHEN MIN(non_unique) = 0 THEN 1 ELSE 0 END AS is_unique,
			GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS columns_csv
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE
			table_schema = DATABASE()
			AND table_name = ?
			AND index_name = ?
		GROUP BY table_name`,
		table, idx,
	).Scan(&existing.Table, &existing.Unique, &existing.Columns)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IndexDefinition{}, false, nil
		}

		return IndexDefinition{}, false, err
	}

	return IndexDefinition{
		Table:  existing.Table,
		Unique: existing.Unique == 1,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

// createMySQLIndex creates an index in MySQL
func createMySQLIndex(engine *Engine, table string, unique bool, idx string, fields []string) error {
	quotedFields, err := quoteDialectIdentifiers(engine.Dialect, fields)
	if err != nil {
		return err
	}

	quotedTable, err := quoteDialectIdentifier(engine.Dialect, table)
	if err != nil {
		return err
	}

	quotedIndex, err := quoteDialectIdentifier(engine.Dialect, idx)
	if err != nil {
		return err
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"ALTER TABLE %s ADD %sINDEX %s(%s)",
		quotedTable, uniqueClause, quotedIndex, strings.Join(quotedFields, ", "),
	)

	_, err = engine.ExecContext(context.Background(), query)
	if err := finishCreateIndex(engine, table, unique, idx, fields, err); err != nil {
		return err
	}

	return engine.emitSchemaEvent(SchemaEvent{
		Kind:  SchemaEventCreateIndex,
		Table: table,
		Name:  idx,
		SQL:   query,
	})
}

// ================================================
// SQLite 索引管理
// ================================================

// ensureSQLiteIndex ensures an index exists in SQLite
func ensureSQLiteIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return createSQLiteIndex(db, table, unique, idx, fields)
}

func inspectSQLiteIndexDefinition(db *Engine, idx string) (IndexDefinition, bool, error) {
	type sqliteMasterRow struct {
		Table string `db:"tbl_name"`
	}

	type sqliteIndexListRow struct {
		Seq     int    `db:"seq"`
		Name    string `db:"name"`
		Unique  int    `db:"unique"`
		Origin  string `db:"origin"`
		Partial int    `db:"partial"`
	}

	var master sqliteMasterRow

	err := db.QueryRowContext(
		context.Background(),
		"SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?",
		idx,
	).Scan(&master.Table)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IndexDefinition{}, false, nil
		}

		return IndexDefinition{}, false, err
	}

	quotedTable, err := quoteDialectIdentifier(db.Dialect, master.Table)
	if err != nil {
		return IndexDefinition{}, false, err
	}

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("PRAGMA index_list(%s)", quotedTable))
	if err != nil {
		return IndexDefinition{}, false, err
	}

	defer func() {
		_ = rows.Close()
	}()

	definition := IndexDefinition{Table: master.Table}

	for rows.Next() {
		var row sqliteIndexListRow
		if err := rows.Scan(&row.Seq, &row.Name, &row.Unique, &row.Origin, &row.Partial); err != nil {
			return IndexDefinition{}, false, err
		}

		if row.Name == idx {
			definition.Unique = row.Unique == 1
		}
	}

	if err := rows.Err(); err != nil {
		return IndexDefinition{}, false, err
	}

	fields, err := inspectSQLiteIndexFields(db, idx)
	if err != nil {
		return IndexDefinition{}, false, err
	}

	definition.Fields = fields

	return definition, true, nil
}

func inspectSQLiteIndexFields(db *Engine, idx string) ([]string, error) {
	type sqliteIndexInfoRow struct {
		SeqNo int    `db:"seqno"`
		CID   int    `db:"cid"`
		Name  string `db:"name"`
	}

	quotedIndex, err := quoteDialectIdentifier(db.Dialect, idx)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("PRAGMA index_info(%s)", quotedIndex))
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	fields := make([]string, 0)

	for rows.Next() {
		var row sqliteIndexInfoRow
		if err := rows.Scan(&row.SeqNo, &row.CID, &row.Name); err != nil {
			return nil, err
		}
		fields = append(fields, row.Name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fields, nil
}

// createSQLiteIndex creates an index in SQLite
func createSQLiteIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	quotedFields, err := quoteDialectIdentifiers(db.Dialect, fields)
	if err != nil {
		return err
	}

	quotedTable, err := quoteDialectIdentifier(db.Dialect, table)
	if err != nil {
		return err
	}

	quotedIndex, err := quoteDialectIdentifier(db.Dialect, idx)
	if err != nil {
		return err
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)",
		uniqueClause, quotedIndex, quotedTable, strings.Join(quotedFields, ", "),
	)

	_, err = db.ExecContext(context.Background(), query)
	if err := finishCreateIndex(db, table, unique, idx, fields, err); err != nil {
		return err
	}

	return db.emitSchemaEvent(SchemaEvent{
		Kind:  SchemaEventCreateIndex,
		Table: table,
		Name:  idx,
		SQL:   query,
	})
}

// ================================================
// PostgreSQL 索引管理
// ================================================

// ensurePostgresIndex ensures an index exists in PostgreSQL
func ensurePostgresIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return createPostgresIndex(db, table, unique, idx, fields)
}

func inspectPostgresIndexDefinition(db *Engine, idx string) (IndexDefinition, bool, error) {
	type row struct {
		Table   string         `db:"table_name"`
		Unique  bool           `db:"is_unique"`
		Columns sql.NullString `db:"columns_csv"`
	}

	var existing row

	err := db.QueryRowContext(context.Background(), `
		SELECT
			t.relname AS table_name,
			i.indisunique AS is_unique,
			STRING_AGG(a.attname, ',' ORDER BY ord.ord) AS columns_csv
		FROM pg_class idx
		JOIN pg_namespace ns ON ns.oid = idx.relnamespace
		JOIN pg_index i ON i.indexrelid = idx.oid
		JOIN pg_class t ON t.oid = i.indrelid
		JOIN UNNEST(i.indkey) WITH ORDINALITY AS ord(attnum, ord) ON TRUE
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum
		WHERE ns.nspname = current_schema()
			AND idx.relname = $1
		GROUP BY t.relname, i.indisunique`,
		idx,
	).Scan(&existing.Table, &existing.Unique, &existing.Columns)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IndexDefinition{}, false, nil
		}

		return IndexDefinition{}, false, err
	}

	return IndexDefinition{
		Table:  existing.Table,
		Unique: existing.Unique,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

// createPostgresIndex creates an index in PostgreSQL
func createPostgresIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	quotedFields, err := quoteDialectIdentifiers(db.Dialect, fields)
	if err != nil {
		return err
	}

	quotedTable, err := quoteDialectIdentifier(db.Dialect, table)
	if err != nil {
		return err
	}

	quotedIndex, err := quoteDialectIdentifier(db.Dialect, idx)
	if err != nil {
		return err
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)",
		uniqueClause, quotedIndex, quotedTable, strings.Join(quotedFields, ", "),
	)

	_, err = db.ExecContext(context.Background(), query)
	if err := finishCreateIndex(db, table, unique, idx, fields, err); err != nil {
		return err
	}

	return db.emitSchemaEvent(SchemaEvent{
		Kind:  SchemaEventCreateIndex,
		Table: table,
		Name:  idx,
		SQL:   query,
	})
}
