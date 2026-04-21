package tsq

import (
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

var (
	tablesMu sync.RWMutex
	tables   = make(map[string]*RegisteredTable)
)

// RegisterTable registers a table in the global registry
func RegisterTable(
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

	tablesMu.Lock()
	defer tablesMu.Unlock()

	if _, exists := tables[table.Table()]; exists {
		panic(fmt.Sprintf("table %s is already registered", table.Table()))
	}

	tables[table.Table()] = &RegisteredTable{
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
	if db == nil {
		return errors.New("db map cannot be nil")
	}

	// Add tracers
	appendUniqueGlobalTracers(tracer...)

	registeredTables := snapshotRegisteredTables()

	// Configure tables in gorp
	for _, table := range registeredTables {
		table.AddTableFunc(db)
	}

	if autoCreateTable {
		if err := db.CreateTablesIfNotExists(); err != nil {
			return errors.Annotate(err, "failed to create tables")
		}
	}

	if upsertIndexies {
		for _, table := range registeredTables {
			if err := table.InitFunc(db); err != nil {
				return errors.Annotatef(err,
					"failed to initialize table %s", table.Table.Table(),
				)
			}
		}
	}

	return nil
}

func snapshotRegisteredTables() []*RegisteredTable {
	tablesMu.RLock()
	defer tablesMu.RUnlock()

	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}

	sort.Strings(names)

	result := make([]*RegisteredTable, 0, len(names))
	for _, name := range names {
		result = append(result, tables[name])
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
	exists, err := isIndexExist(db, table, idx)
	if err != nil {
		return errors.Trace(err)
	}

	if exists {
		return nil
	}

	return createMySQLIndex(db, table, unique, idx, fields)
}

// isIndexExist checks if an index exists in MySQL
func isIndexExist(dbMap *gorp.DbMap, table string, idx string) (bool, error) {
	count, err := dbMap.SelectInt(`
		SELECT COUNT(1)
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE
			table_schema = DATABASE()
			AND table_name = ?
			AND index_name = ?`,
		table, idx,
	)
	if err != nil {
		return false, errors.Trace(err)
	}

	return count > 0, nil
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

	return errors.Trace(err)
}

// ================================================
// SQLite 索引管理
// ================================================

// ensureSQLiteIndex ensures an index exists in SQLite
func ensureSQLiteIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	var exists int

	err := db.SelectOne(&exists,
		"SELECT COUNT(1) FROM sqlite_master WHERE type='index' AND name=?", idx)
	if err != nil {
		return errors.Trace(err)
	}

	if exists > 0 {
		return nil
	}

	return createSQLiteIndex(db, table, unique, idx, fields)
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

	return errors.Trace(err)
}

// ================================================
// PostgreSQL 索引管理
// ================================================

// ensurePostgresIndex ensures an index exists in PostgreSQL
func ensurePostgresIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
	var exists int

	err := db.SelectOne(&exists,
		"SELECT COUNT(1) FROM pg_indexes WHERE tablename=$1 AND indexname=$2", table, idx)
	if err != nil {
		return errors.Trace(err)
	}

	if exists > 0 {
		return nil
	}

	return createPostgresIndex(db, table, unique, idx, fields)
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

	return errors.Trace(err)
}
