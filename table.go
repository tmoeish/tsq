package tsq

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/juju/errors"
	"gopkg.in/gorp.v2"
)

// ================================================
// 表注册和管理
// ================================================

type Registry struct {
	mu     sync.RWMutex
	tables map[string]*RegisteredTable
}

func NewRegistry() *Registry {
	return &Registry{
		tables: make(map[string]*RegisteredTable),
	}
}

type InitOptions struct {
	AutoCreateTables bool
	UpsertIndexes    bool
	Tracers          []Tracer
}

// RegisterTable registers a table in the global registry
func RegisterTable(
	table Table,
	addTableFunc func(db *gorp.DbMap),
	initFunc func(db *gorp.DbMap) error,
) {
	defaultRuntime.RegisterTable(table, addTableFunc, initFunc)
}

func (r *Registry) Register(
	table Table,
	addTableFunc func(db *gorp.DbMap),
	initFunc func(db *gorp.DbMap) error,
) {
	if isNilValue(table) {
		panic("registered table cannot be nil")
	}

	if addTableFunc == nil {
		panic("add table function cannot be nil")
	}

	if initFunc == nil {
		panic("init function cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := registeredTableKey(table)
	if _, exists := r.tables[key]; exists {
		panic(fmt.Sprintf("table %s is already registered", key))
	}

	r.tables[key] = &RegisteredTable{
		Table:        table,
		AddTableFunc: addTableFunc,
		InitFunc:     initFunc,
	}
}

type RegisteredTable struct {
	Table
	AddTableFunc func(db *gorp.DbMap)
	InitFunc     func(db *gorp.DbMap) error
}

// ================================================
// 表接口定义
// ================================================

// Table interface defines a database table (minimized for gorp compatibility)
type Table interface {
	Table() string    // Table name
	KwList() []Column // Keyword search columns
}

// ================================================
// 数据库初始化
// ================================================

// Init initializes the database with all registered tables and creates tables/indexes if needed
func Init(
	db *gorp.DbMap,
	autoCreateTable bool,
	upsertIndexies bool,
	tracer ...Tracer,
) error {
	return defaultRuntime.Init(db, autoCreateTable, upsertIndexies, tracer...)
}

func InitWithOptions(db *gorp.DbMap, options *InitOptions) error {
	return defaultRuntime.InitWithOptions(db, options)
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

func snapshotRegisteredTables() []*RegisteredTable {
	return defaultRuntime.snapshotRegisteredTables()
}

func (r *Registry) Snapshot() []*RegisteredTable {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.tables))
	for name := range r.tables {
		names = append(names, name)
	}

	sort.Strings(names)

	result := make([]*RegisteredTable, 0, len(names))
	for _, name := range names {
		result = append(result, r.tables[name])
	}

	return result
}

// ================================================
// 索引管理 - 数据库方言检测
// ================================================

// UpsertIndex ensures an index exists, with database dialect-specific implementation
func UpsertIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	if db == nil {
		return errors.New("db map cannot be nil")
	}

	if db.Dialect == nil {
		return errors.New("database dialect is required")
	}

	if err := validateIndexIdentifiers(table, idx, fields); err != nil {
		return errors.Trace(err)
	}

	definition, found, err := inspectIndexDefinition(db, table, idx)
	if err != nil {
		return errors.Trace(err)
	}

	if found {
		if err := validateIndexDefinition(table, unique, idx, fields, definition); err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	switch db.Dialect.(type) {
	case gorp.MySQLDialect:
		return ensureMySQLIndex(db, table, unique, idx, fields)
	case gorp.SqliteDialect:
		return ensureSQLiteIndex(db, table, unique, idx, fields)
	case gorp.PostgresDialect:
		return ensurePostgresIndex(db, table, unique, idx, fields)
	default:
		return errors.Errorf("unsupported database dialect: %T", db.Dialect)
	}
}

type indexDefinition struct {
	Table  string
	Unique bool
	Fields []string
}

func inspectIndexDefinition(
	db *gorp.DbMap,
	table string,
	idx string,
) (indexDefinition, bool, error) {
	switch db.Dialect.(type) {
	case gorp.MySQLDialect:
		return inspectMySQLIndexDefinition(db, table, idx)
	case gorp.SqliteDialect:
		return inspectSQLiteIndexDefinition(db, idx)
	case gorp.PostgresDialect:
		return inspectPostgresIndexDefinition(db, idx)
	default:
		return indexDefinition{}, false, errors.Errorf("unsupported database dialect: %T", db.Dialect)
	}
}

func validateIndexDefinition(
	table string,
	unique bool,
	idx string,
	fields []string,
	existing indexDefinition,
) error {
	if existing.Table != table {
		return errors.Errorf(
			"index %s already exists on table %s, expected table %s",
			idx,
			existing.Table,
			table,
		)
	}

	if existing.Unique != unique || !sameOrderedFields(existing.Fields, fields) {
		return errors.Errorf(
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

func sameOrderedFields(left []string, right []string) bool {
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
	db *gorp.DbMap,
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

	return errors.Trace(createErr)
}

func validateIndexIdentifiers(table string, idx string, fields []string) error {
	if err := validateBuiltInIdentifier(table); err != nil {
		return errors.Annotate(err, "invalid table name")
	}

	if err := validateBuiltInIdentifier(idx); err != nil {
		return errors.Annotate(err, "invalid index name")
	}

	if len(fields) == 0 {
		return errors.New("index fields cannot be empty")
	}

	for _, field := range fields {
		if err := validateBuiltInIdentifier(field); err != nil {
			return errors.Annotatef(err, "invalid index field %s", field)
		}
	}

	return nil
}

func validateBuiltInIdentifier(name string) error {
	if !builtInIdentifierPattern.MatchString(name) {
		return errors.Errorf("invalid SQL identifier: %s", name)
	}

	return nil
}

func quoteDialectIdentifier(dialect gorp.Dialect, name string) (string, error) {
	if err := validateBuiltInIdentifier(name); err != nil {
		return "", errors.Trace(err)
	}

	if dialect == nil {
		return canonicalQuoteIdentifier(name), nil
	}

	return dialect.QuoteField(name), nil
}

func quoteDialectIdentifiers(dialect gorp.Dialect, names []string) ([]string, error) {
	quoted := make([]string, len(names))

	for i, name := range names {
		value, err := quoteDialectIdentifier(dialect, name)
		if err != nil {
			return nil, errors.Trace(err)
		}

		quoted[i] = value
	}

	return quoted, nil
}

// ================================================
// MySQL 索引管理
// ================================================

// ensureMySQLIndex ensures an index exists in MySQL
func ensureMySQLIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	return createMySQLIndex(db, table, unique, idx, fields)
}

func inspectMySQLIndexDefinition(
	dbMap *gorp.DbMap,
	table string,
	idx string,
) (indexDefinition, bool, error) {
	type row struct {
		Table   string         `db:"table_name"`
		Unique  int            `db:"is_unique"`
		Columns sql.NullString `db:"columns_csv"`
	}

	var existing row

	err := dbMap.SelectOne(&existing, `
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
	)
	if err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return indexDefinition{}, false, nil
		}

		return indexDefinition{}, false, errors.Trace(err)
	}

	return indexDefinition{
		Table:  existing.Table,
		Unique: existing.Unique == 1,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

// createMySQLIndex creates an index in MySQL
func createMySQLIndex(dbMap *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	quotedFields, err := quoteDialectIdentifiers(dbMap.Dialect, fields)
	if err != nil {
		return errors.Trace(err)
	}

	quotedTable, err := quoteDialectIdentifier(dbMap.Dialect, table)
	if err != nil {
		return errors.Trace(err)
	}

	quotedIndex, err := quoteDialectIdentifier(dbMap.Dialect, idx)
	if err != nil {
		return errors.Trace(err)
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"ALTER TABLE %s ADD %sINDEX %s(%s)",
		quotedTable, uniqueClause, quotedIndex, strings.Join(quotedFields, ", "),
	)

	_, err = dbMap.Exec(query)

	return finishCreateIndex(dbMap, table, unique, idx, fields, err)
}

// ================================================
// SQLite 索引管理
// ================================================

// ensureSQLiteIndex ensures an index exists in SQLite
func ensureSQLiteIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	return createSQLiteIndex(db, table, unique, idx, fields)
}

func inspectSQLiteIndexDefinition(db *gorp.DbMap, idx string) (indexDefinition, bool, error) {
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

	err := db.SelectOne(
		&master,
		"SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?",
		idx,
	)
	if err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return indexDefinition{}, false, nil
		}

		return indexDefinition{}, false, errors.Trace(err)
	}

	quotedTable, err := quoteDialectIdentifier(db.Dialect, master.Table)
	if err != nil {
		return indexDefinition{}, false, errors.Trace(err)
	}

	var rows []sqliteIndexListRow
	if _, err := db.Select(&rows, fmt.Sprintf("PRAGMA index_list(%s)", quotedTable)); err != nil {
		return indexDefinition{}, false, errors.Trace(err)
	}

	definition := indexDefinition{Table: master.Table}

	for _, row := range rows {
		if row.Name == idx {
			definition.Unique = row.Unique == 1
			break
		}
	}

	fields, err := inspectSQLiteIndexFields(db, idx)
	if err != nil {
		return indexDefinition{}, false, errors.Trace(err)
	}

	definition.Fields = fields

	return definition, true, nil
}

func inspectSQLiteIndexFields(db *gorp.DbMap, idx string) ([]string, error) {
	type sqliteIndexInfoRow struct {
		SeqNo int    `db:"seqno"`
		CID   int    `db:"cid"`
		Name  string `db:"name"`
	}

	quotedIndex, err := quoteDialectIdentifier(db.Dialect, idx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rows []sqliteIndexInfoRow
	if _, err := db.Select(&rows, fmt.Sprintf("PRAGMA index_info(%s)", quotedIndex)); err != nil {
		return nil, errors.Trace(err)
	}

	fields := make([]string, 0, len(rows))
	for _, row := range rows {
		fields = append(fields, row.Name)
	}

	return fields, nil
}

// createSQLiteIndex creates an index in SQLite
func createSQLiteIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	quotedFields, err := quoteDialectIdentifiers(db.Dialect, fields)
	if err != nil {
		return errors.Trace(err)
	}

	quotedTable, err := quoteDialectIdentifier(db.Dialect, table)
	if err != nil {
		return errors.Trace(err)
	}

	quotedIndex, err := quoteDialectIdentifier(db.Dialect, idx)
	if err != nil {
		return errors.Trace(err)
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)",
		uniqueClause, quotedIndex, quotedTable, strings.Join(quotedFields, ", "),
	)

	_, err = db.Exec(query)

	return finishCreateIndex(db, table, unique, idx, fields, err)
}

// ================================================
// PostgreSQL 索引管理
// ================================================

// ensurePostgresIndex ensures an index exists in PostgreSQL
func ensurePostgresIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	return createPostgresIndex(db, table, unique, idx, fields)
}

func inspectPostgresIndexDefinition(db *gorp.DbMap, idx string) (indexDefinition, bool, error) {
	type row struct {
		Table   string         `db:"table_name"`
		Unique  bool           `db:"is_unique"`
		Columns sql.NullString `db:"columns_csv"`
	}

	var existing row

	err := db.SelectOne(&existing, `
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
	)
	if err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return indexDefinition{}, false, nil
		}

		return indexDefinition{}, false, errors.Trace(err)
	}

	return indexDefinition{
		Table:  existing.Table,
		Unique: existing.Unique,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

// createPostgresIndex creates an index in PostgreSQL
func createPostgresIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	quotedFields, err := quoteDialectIdentifiers(db.Dialect, fields)
	if err != nil {
		return errors.Trace(err)
	}

	quotedTable, err := quoteDialectIdentifier(db.Dialect, table)
	if err != nil {
		return errors.Trace(err)
	}

	quotedIndex, err := quoteDialectIdentifier(db.Dialect, idx)
	if err != nil {
		return errors.Trace(err)
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)",
		uniqueClause, quotedIndex, quotedTable, strings.Join(quotedFields, ", "),
	)

	_, err = db.Exec(query)

	return finishCreateIndex(db, table, unique, idx, fields, err)
}
