package tsq

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"gopkg.in/gorp.v2"
)

// ================================================
// 表注册和管理
// ================================================

var tables = make(map[string]*RegisteredTable)

// RegisterTable registers a table in the global registry
func RegisterTable(
	table Table,
	addTableFunc func(db *gorp.DbMap),
	initFunc func(db *gorp.DbMap) error,
) {
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
	// Add tracers
	tracers = append(tracers, tracer...)

	// Configure tables in gorp
	for _, table := range tables {
		table.AddTableFunc(db)
	}

	if autoCreateTable {
		if err := db.CreateTablesIfNotExists(); err != nil {
			return errors.Annotate(err, "failed to create tables")
		}
	}

	if upsertIndexies {
		for _, table := range tables {
			if err := table.InitFunc(db); err != nil {
				return errors.Annotatef(err,
					"failed to initialize table %s", table.Table.Table(),
				)
			}
		}
	}

	return nil
}

// ================================================
// 索引管理 - 数据库方言检测
// ================================================

// UpsertIndex ensures an index exists, with database dialect-specific implementation
func UpsertIndex(db *gorp.DbMap, table string, unique bool, idx string, fields []string) error {
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
	quotedFields := make([]string, len(fields))
	for i, field := range fields {
		quotedFields[i] = fmt.Sprintf("`%s`", field)
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"ALTER TABLE `%s` ADD %sINDEX `%s`(%s)",
		table, uniqueClause, idx, strings.Join(quotedFields, ", "),
	)

	_, err := dbMap.Exec(query)

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
	quotedFields := make([]string, len(fields))
	for i, field := range fields {
		quotedFields[i] = fmt.Sprintf("`%s`", field)
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX `%s` ON `%s`(%s)",
		uniqueClause, idx, table, strings.Join(quotedFields, ", "),
	)

	_, err := db.Exec(query)

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
	quotedFields := make([]string, len(fields))
	for i, field := range fields {
		quotedFields[i] = fmt.Sprintf(`"%s"`, field)
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		`CREATE %sINDEX "%s" ON "%s"(%s)`,
		uniqueClause, idx, table, strings.Join(quotedFields, ", "),
	)

	_, err := db.Exec(query)

	return errors.Trace(err)
}
