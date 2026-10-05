package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// Runtime owns the initialized TSQ process state used for execution, index setup,
// identifier validation, and tracing.
type Runtime struct {
	tables      []*registeredTable
	tracers     []Tracer
	db          *sql.DB
	dialect     sqld.Dialect
	tablePolicy SchemaPolicy
	indexPolicy SchemaPolicy
	logger      Logger
	maxPageSize int
	logSQL      bool
	ownsDB      bool
	// schema is the connection the schema policies run on while they hold the
	// schema lock; nil outside of that, and under a policy that changes nothing.
	schema *sql.Conn
}

// Open opens a database connection, resolves the SQL dialect from
// driverName, and constructs an initialized runtime for the provided tables.
//
// ctx bounds the connection ping and the schema policy application, which may
// execute DDL. The runtime owns the pool it opened, so Close closes it.
//
// A MySQL DSN must set parseTime=true: without it the driver reads DATETIME as
// bytes, and no managed timestamp can be read back.
func Open(
	ctx context.Context,
	driverName string,
	dsn string,
	tables []Table,
	options ...RuntimeOption,
) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}

	if driverName == "" {
		return nil, errors.New("driver name cannot be empty")
	}

	if dsn == "" {
		return nil, errors.New("dsn cannot be empty")
	}

	sqlDialect, err := resolveRuntimeDialect(driverName)
	if err != nil {
		return nil, err
	}

	if sqlDialect.Name() == tsqdialect.MySQL {
		if !mysqlParsesTime(dsn) {
			return nil, errors.New("the MySQL DSN must set parseTime=true, or times are read as bytes and no timestamp can be read back")
		}

		// The driver writes a time in loc and reads a DATETIME as one in loc. TSQ
		// binds every time in UTC and has the database fill times in UTC
		// (default:CURRENT_TIMESTAMP is UTC_TIMESTAMP): under another loc the stamps
		// are stored in that zone after all, and a time the database filled is read
		// back shifted by its offset, without an error.
		if loc, set := mysqlDSNParam(dsn, "loc"); set && loc != "UTC" {
			return nil, fmt.Errorf("the MySQL DSN sets loc=%s; TSQ stores every time in UTC, and with another loc the driver stores times in that zone "+
				"and reads the times the database fills as if they were in it: drop loc (UTC is the driver's default) and convert for display with time.Time.In", loc)
		}
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	runtime, err := newRuntime(ctx, db, sqlDialect, tables, true, options)
	if err != nil {
		_ = db.Close()

		return nil, err
	}

	return runtime, nil
}

// NewRuntime constructs a runtime over a pool the caller already owns,
// which is the way to keep an instrumented or specially configured connection
// while still getting SQL logging, tracers and the page-size cap.
//
// The pool is not closed by Close: whoever opened it decides when it goes away.
// A MySQL pool has to be opened with parseTime=true, as Open requires.
func NewRuntime(
	ctx context.Context,
	db *sql.DB,
	dialect tsqdialect.Name,
	tables []Table,
	options ...RuntimeOption,
) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}

	if db == nil {
		return nil, errors.New("db cannot be nil")
	}

	sqlDialect, err := sqld.For(dialect)
	if err != nil {
		return nil, err
	}

	return newRuntime(ctx, db, sqlDialect, tables, false, options)
}

func newRuntime(
	ctx context.Context,
	db *sql.DB,
	sqlDialect sqld.Dialect,
	tables []Table,
	ownsDB bool,
	options []RuntimeOption,
) (*Runtime, error) {
	registeredTables, err := registerTables(tables)
	if err != nil {
		return nil, err
	}

	cfg, err := newRuntimeConfig(options)
	if err != nil {
		return nil, err
	}

	// The runtimes of one process change a SQLite schema one at a time (see
	// lockSchema), and that starts here: a connection that only reads the file
	// while another commits a schema change is refused too ("database is locked").
	if sqlDialect.Name() == tsqdialect.SQLite && (changesSchema(cfg.tablePolicy) || changesSchema(cfg.indexPolicy)) {
		sqliteSchema.Lock()
		defer sqliteSchema.Unlock()
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	switch sqlDialect.Name() {
	case tsqdialect.MySQL:
		if err := checkMySQLTimes(ctx, db); err != nil {
			return nil, err
		}

		if err := checkMySQLText(ctx, db); err != nil {
			return nil, err
		}
	case tsqdialect.Postgres:
		if err := checkPostgresText(ctx, db); err != nil {
			return nil, err
		}
	}

	runtime := &Runtime{
		tables:      registeredTables,
		tracers:     cfg.tracers,
		db:          db,
		dialect:     sqlDialect,
		tablePolicy: cfg.tablePolicy,
		indexPolicy: cfg.indexPolicy,
		logger:      cfg.logger,
		maxPageSize: cfg.maxPageSize,
		logSQL:      cfg.logSQL,
		ownsDB:      ownsDB,
	}

	if sqlDialect.Name() == tsqdialect.MySQL {
		runtime.warnMySQLMode(ctx)
	}

	// Identifiers are checked before any DDL runs: a name the dialect will
	// truncate produces objects that do not match what the queries reference.
	if err := runtime.validateRegisteredTableIdentifiers(); err != nil {
		return nil, err
	}

	if err := runtime.applySchemaPolicies(ctx); err != nil {
		return nil, err
	}

	return runtime, nil
}

var _ Executor = (*Runtime)(nil)

// Close releases the connection pool, but only the one Open opened: a pool
// handed to NewRuntime belongs to its caller. It is safe to call on a nil runtime.
func (r *Runtime) Close() error {
	if r == nil || r.db == nil || !r.ownsDB {
		return nil
	}

	return r.db.Close()
}

// maxPage is the page-size cap applied to paged queries on this runtime.
func (r *Runtime) maxPage() int {
	if r == nil || r.maxPageSize <= 0 {
		return DefaultMaxPageSize
	}

	return r.maxPageSize
}

func (*Runtime) needsRuntimeOrWrapExecutor() {}

func (r *Runtime) scope() execScope {
	if r == nil {
		return execScope{}
	}

	return execScope{dialect: r.dialect, runtime: r}
}

// DB returns the current *sql.DB.
func (r *Runtime) DB() *sql.DB {
	if r == nil {
		return nil
	}

	return r.db
}

// Dialect returns the dialect this runtime renders for.
func (r *Runtime) Dialect() tsqdialect.Name {
	if r == nil || r.dialect == nil {
		return ""
	}

	return r.dialect.Name()
}

// QueryContext executes a query against the runtime database; like ExecContext it
// is not traced or logged.
func (r *Runtime) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db, err := r.sqlDB()
	if err != nil {
		return nil, err
	}

	return db.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query expected to return at most one row; like
// ExecContext it is not traced or logged.
func (r *Runtime) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	db, err := r.sqlDB()
	if err != nil {
		return queryRowWithError(ctx, err, query, args...)
	}

	return db.QueryRowContext(ctx, query, args...)
}

// ExecContext executes a statement against the runtime database, as
// database/sql does: it is not traced or logged, and has no dialect checks.
func (r *Runtime) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	db, err := r.sqlDB()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}

// WithTx runs fn in a transaction and passes it the executor to use inside it:
// the transaction commits when fn returns nil and rolls back otherwise. Options
// set the isolation level, read-only mode and retries.
//
// A rollback undoes the database, not memory: a row an Insert or Update inside fn
// stamped (key, created_at, updated_at, version) keeps those values after the
// rollback, and a retry runs fn again with them. Load or build the rows fn writes
// inside fn, so every attempt starts from what the database holds.
func (r *Runtime) WithTx(
	ctx context.Context,
	fn func(context.Context, Executor) error,
	options ...TxOption,
) error {
	if fn == nil {
		return errors.New("transaction function cannot be nil")
	}

	_, err := r.withTxResult(ctx, func(ctx context.Context, txExec Executor) (struct{}, error) {
		return struct{}{}, fn(ctx, txExec)
	}, options)

	return err
}

func (r *Runtime) sqlDB() (*sql.DB, error) {
	if err := validateTxRuntime(r); err != nil {
		return nil, err
	}

	return r.db, nil
}

// runtimeErrorKey carries the error that made a runtime unusable into the shared
// error-only *sql.DB below.
type runtimeErrorKey struct{}

// errRuntimeUnusable is the fallback for connection attempts that reach the error-only
// pool without a caller error attached, which database/sql can do from its background
// connection opener.
var errRuntimeUnusable = errors.New("tsq runtime is not usable")

// errorDB is a single process-wide *sql.DB on which every connection attempt fails
// with the error the caller placed in the context.
//
// QueryRowContext must return a *sql.Row and has no other channel for reporting that
// the runtime is unusable, and a *sql.Row carrying an error cannot be constructed from
// outside database/sql. Opening a throwaway pool per call used to be the answer, but
// sql.OpenDB starts a connection-opener goroutine that only Close stops, so every call
// against a nil or half-built runtime leaked a *sql.DB and a goroutine. One shared pool
// costs one goroutine for the life of the process no matter how often callers hit it.
var errorDB = sync.OnceValue(func() *sql.DB {
	return sql.OpenDB(runtimeErrorConnector{})
})

// queryRowWithError returns a *sql.Row whose Scan reports err.
func queryRowWithError(ctx context.Context, err error, query string, args ...any) *sql.Row {
	return errorDB().QueryRowContext(context.WithValue(ctx, runtimeErrorKey{}, err), query, args...)
}

type runtimeErrorConnector struct{}

func (runtimeErrorConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err, ok := ctx.Value(runtimeErrorKey{}).(error); ok && err != nil {
		return nil, err
	}

	return nil, errRuntimeUnusable
}

func (c runtimeErrorConnector) Driver() driver.Driver {
	return runtimeErrorDriver{}
}

type runtimeErrorDriver struct{}

func (runtimeErrorDriver) Open(string) (driver.Conn, error) {
	return nil, errRuntimeUnusable
}

func (r *Runtime) validateRegisteredTableIdentifiers() error {
	for _, table := range r.tables {
		if err := validateIdentifierForDialect(table.name, r.dialect); err != nil {
			return fmt.Errorf("table %s: %w", table.name, err)
		}

		for _, col := range table.columns {
			if err := validateIdentifierForDialect(col.name, r.dialect); err != nil {
				return fmt.Errorf("column %s.%s: %w", table.name, col.name, err)
			}
		}

		for _, index := range table.Indexes {
			if err := validateIdentifierForDialect(index.Name, r.dialect); err != nil {
				return fmt.Errorf("index %s on %s: %w", index.Name, table.name, err)
			}
		}
	}

	return nil
}

func resolveRuntimeDialect(driverName string) (sqld.Dialect, error) {
	// The names are the ones drivers register: modernc.org/sqlite is "sqlite" and
	// mattn/go-sqlite3 is "sqlite3", and both speak the same SQL.
	switch strings.ToLower(strings.TrimSpace(driverName)) {
	case "sqlite", "sqlite3":
		return sqld.SQLiteDialect{}, nil
	case "mysql":
		return sqld.MySQLDialect{}, nil
	case "postgres", "postgresql", "pgx", "pq":
		return sqld.PostgresDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported sql driver %q; expected sqlite, sqlite3, mysql, postgres, postgresql, pgx, or pq", driverName)
	}
}

// checkMySQLTimes asks the pool how its driver reads a DATETIME. Open reads
// parseTime and loc from the DSN; a pool handed to NewRuntime has no DSN to read,
// and the same two settings went unchecked there: without parseTime a time comes
// back as bytes and no timestamp can be read, and under a loc other than UTC the
// driver stores TSQ's UTC times in that zone and reads the times the database
// fills as if they were in it, hours off and without an error.
func checkMySQLTimes(ctx context.Context, db *sql.DB) error {
	var probe any
	if err := db.QueryRowContext(ctx, "SELECT CAST('2001-02-03 04:05:06' AS DATETIME)").Scan(&probe); err != nil {
		return fmt.Errorf("check how the MySQL driver reads a time: %w", err)
	}

	at, parsed := probe.(time.Time)
	if !parsed {
		return errors.New("the MySQL pool must be opened with parseTime=true, or times are read as bytes and no timestamp can be read back")
	}

	if at.Location() != time.UTC {
		return fmt.Errorf("the MySQL pool is opened with loc=%s; TSQ stores every time in UTC, and with another loc the driver stores times in that zone "+
			"and reads the times the database fills as if they were in it: drop loc (UTC is the driver's default) and convert for display with time.Time.In", at.Location())
	}

	return nil
}

// checkPostgresText refuses a session that exchanges text in another encoding
// than UTF-8. pgx asks for none, so the session takes the database's: over a
// LATIN1 database the bytes of a Go string, which are UTF-8, are read as LATIN1
// characters, two or three for one. What is stored is not what was written (any
// other client sees it), LENGTH counts the wrong characters and SUBSTRING cuts
// between the bytes of one, and nothing fails, because the same misreading turns
// the text back on the way out. A SQL_ASCII database converts nothing whatever
// the client asks for, so there is nothing to ask of it.
func checkPostgresText(ctx context.Context, db *sql.DB) error {
	var client, server string

	// A setting that cannot be read is not a reason to refuse to start.
	if err := db.QueryRowContext(ctx, "SELECT current_setting('client_encoding'), current_setting('server_encoding')").Scan(&client, &server); err != nil {
		return nil //nolint:nilerr // see above
	}

	if strings.EqualFold(client, "UTF8") || strings.EqualFold(server, "SQL_ASCII") {
		return nil
	}

	return fmt.Errorf("the PostgreSQL session exchanges text as %s (the encoding of the database is %s, and the driver asks for none of its own); "+
		"a Go string is UTF-8, and its bytes would be stored as other characters than the ones written: add client_encoding=UTF8 to the DSN", client, server)
}

// checkMySQLText is checkPostgresText for MySQL, where the DSN's charset or
// collation decides: with charset=latin1 the server reads the UTF-8 bytes of a Go
// string as latin1 characters and stores those. The driver's default is utf8mb4.
func checkMySQLText(ctx context.Context, db *sql.DB) error {
	var client sql.NullString

	if err := db.QueryRowContext(ctx, "SELECT @@SESSION.character_set_client").Scan(&client); err != nil || !client.Valid {
		return nil //nolint:nilerr // a setting that cannot be read is not a reason to refuse to start
	}

	switch strings.ToLower(client.String) {
	case "utf8mb4", "utf8mb3", "utf8", "binary":
		return nil
	}

	return fmt.Errorf("the MySQL connection exchanges text as %s; a Go string is UTF-8, and its bytes would be stored as other characters than the ones written: "+
		"drop charset and collation from the DSN (utf8mb4 is the driver's default), or set them to utf8mb4", client.String)
}

// warnMySQLMode says so where the sessions of the pool are not strict. Outside
// strict mode MySQL cuts a string to the length of its column and clamps a number
// to the range of its type, with a warning the driver does not pass on: a write
// reported as done stored another value. The mode is the deployment's to choose,
// and schemas from before strict mode was the default depend on it, so this is a
// warning and not a refusal. One session answers for the pool.
func (r *Runtime) warnMySQLMode(ctx context.Context) {
	var mode string

	// A mode that cannot be read is not a reason to refuse to start.
	if err := r.db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&mode); err != nil || sqld.MySQLStrict(mode) {
		return
	}

	r.warn("the MySQL session is not in strict mode: a value that does not fit its column is cut or clamped, not refused; "+
		"add STRICT_TRANS_TABLES to sql_mode", "sql_mode", mode)
}

// mysqlParsesTime reports whether a go-sql-driver DSN sets parseTime, read as the
// driver reads it: the last parseTime parameter, true for 1, true, TRUE or True. A
// substring check refused parseTime=1, which the driver takes.
func mysqlParsesTime(dsn string) bool {
	setting, _ := mysqlDSNParam(dsn, "parseTime")

	switch setting {
	case "1", "true", "TRUE", "True":
		return true
	default:
		return false
	}
}

// mysqlDSNParam reads a parameter of a go-sql-driver DSN as the driver reads it:
// the last one of that name, unescaped. The root package imports no driver, so the
// query string is parsed here.
func mysqlDSNParam(dsn, name string) (string, bool) {
	_, query, ok := strings.Cut(dsn[strings.LastIndex(dsn, "/")+1:], "?")
	if !ok {
		return "", false
	}

	values, err := url.ParseQuery(query)
	if err != nil {
		return "", false
	}

	settings := values[name]
	if len(settings) == 0 {
		return "", false
	}

	return settings[len(settings)-1], true
}
