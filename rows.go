package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// Row writes live on the table descriptor: TableCourse.Insert(ctx, db, &course).
// Generated code adds the same operations as methods on the row type.
//
// Managed columns are maintained here, not in generated code: Insert fills
// created_at and updated_at when they are unset, Update refreshes updated_at and
// never writes created_at or deleted_at, Delete on a table with deleted_at stamps
// the tombstone and Restore clears it, and a version column is checked and
// incremented by every row write.
//
// Batch writes do not open a transaction. Run them inside Runtime.WithTx when the
// batch has to succeed or fail as a whole.

// stampTime is the time TSQ writes into managed timestamps. It is UTC, so rows
// stamped by processes in different zones, or on both sides of a daylight-saving
// change, still sort by time where the database keeps the time as text (SQLite).
func stampTime() time.Time { return time.Now().UTC() }

// updatedAtValue returns now as the table's updated_at field type holds it.
func (t *TableOf[R, K]) updatedAtValue(now time.Time) (any, error) {
	col := t.def.column(t.def.managed.UpdatedAt)
	v := reflect.New(field(new(R), col).Type()).Elem()

	if err := applyTimestamp(v, now); err != nil {
		return nil, fmt.Errorf("table %s: %w", t.def.name, err)
	}

	return v.Interface(), nil
}

// BatchOption configures the Batch* operations.
type BatchOption func(*batchConfig)

type batchConfig struct {
	size           int
	skipDuplicates bool
	err            error
	// only restricts an Update to these columns.
	only []SQLColumn
}

const defaultBatchSize = 1000

// WithBatchSize sets the number of rows per statement; the default is 1000. It is an
// upper bound: wide tables are split further so that one statement stays within
// the dialect's bind parameter limit (dialect.MaxBindParams).
func WithBatchSize(size int) BatchOption {
	return func(c *batchConfig) {
		if size <= 0 {
			c.err = fmt.Errorf("invalid batch size: %d", size)
			return
		}

		c.size = size
	}
}

// WithSkipDuplicates makes BatchInsert skip rows that fail with a duplicate-key
// error and continue with the rest. Only BatchInsert accepts it.
func WithSkipDuplicates() BatchOption {
	return func(c *batchConfig) { c.skipDuplicates = true }
}

func newBatchConfig(options []BatchOption, insert bool) (batchConfig, error) {
	config := batchConfig{size: defaultBatchSize}

	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}

	if config.err != nil {
		return batchConfig{}, config.err
	}

	if config.skipDuplicates && !insert {
		return batchConfig{}, errors.New("WithSkipDuplicates applies only to BatchInsert")
	}

	return config, nil
}

// effectiveChunkSize lowers a row count so one statement binds at most
// maxBindParams placeholders, never going below one row.
func effectiveChunkSize(chunkSize, bindParamsPerRow, maxBindParams int) int {
	if bindParamsPerRow <= 0 || maxBindParams <= 0 {
		return chunkSize
	}

	return max(1, min(chunkSize, maxBindParams/bindParamsPerRow))
}

func chunks[T any](items []T, size int) [][]T {
	var result [][]T

	for start := 0; start < len(items); start += size {
		result = append(result, items[start:min(start+size, len(items))])
	}

	return result
}

// Insert inserts row. A zero auto-increment primary key is left to the database
// and written back to row.
func (t *TableOf[R, K]) Insert(ctx context.Context, db Executor, row *R) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpInsert), func(ctx context.Context) error {
		return t.insert(ctx, db, []*R{row}, batchConfig{size: 1})
	})
}

// BatchInsert inserts rows in as few statements as the batch size allows.
func (t *TableOf[R, K]) BatchInsert(ctx context.Context, db Executor, rows []*R, options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpInsert), func(ctx context.Context) error {
		config, err := newBatchConfig(options, true)
		if err != nil {
			return err
		}

		return t.insert(ctx, db, rows, config)
	})
}

// Update writes row, matching on the primary key and, when the table has one,
// the version. With no cols it writes every column TSQ may write: all but the
// primary key, created_at, deleted_at and generated columns. With cols it writes
// only those, which is how a row read with a partial Select is saved without
// zeroing the columns it did not read. updated_at and version are maintained
// either way.
func (t *TableOf[R, K]) Update(ctx context.Context, db Executor, row *R, cols ...BoundColumn[R]) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpdate), func(ctx context.Context) error {
		config := batchConfig{size: 1}
		if len(cols) > 0 {
			config.only = sqlColumns(cols...)
		}

		return t.update(ctx, db, []*R{row}, config, stampTime())
	})
}

// BatchUpdate updates rows in as few statements as the batch size allows.
func (t *TableOf[R, K]) BatchUpdate(ctx context.Context, db Executor, rows []*R, options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpdate), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		return t.update(ctx, db, rows, config, stampTime())
	})
}

// Delete deletes row. On a table with a deleted_at column it is a soft delete: an
// update that stamps the tombstone, so the version check applies. Otherwise it is
// HardDelete.
func (t *TableOf[R, K]) Delete(ctx context.Context, db Executor, row *R) error {
	return t.BatchDelete(ctx, db, []*R{row}, WithBatchSize(1))
}

// BatchDelete deletes rows as Delete does.
func (t *TableOf[R, K]) BatchDelete(ctx context.Context, db Executor, rows []*R, options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpDelete), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		if !t.softDeleted() {
			return t.hardDelete(ctx, db, rows, config)
		}

		return t.setTombstone(ctx, db, rows, config, true)
	})
}

// Restore clears the tombstone of a soft-deleted row, refreshing updated_at and
// incrementing version. Only a deleted row matches; on a table with a version
// column a row that is not deleted, or changed since it was loaded, fails with
// OptimisticLockError.
func (t *TableOf[R, K]) Restore(ctx context.Context, db Executor, row *R) error {
	return t.BatchRestore(ctx, db, []*R{row}, WithBatchSize(1))
}

// BatchRestore restores rows in as few statements as the batch size allows.
func (t *TableOf[R, K]) BatchRestore(ctx context.Context, db Executor, rows []*R, options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpdate), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		if t.def.managed.DeletedAt == "" {
			return fmt.Errorf("restore %s: the table has no deleted_at column", t.TableName())
		}

		return t.setTombstone(ctx, db, rows, config, false)
	})
}

// setTombstone soft-deletes live rows (deleted is true) or restores deleted ones.
// It writes only the managed columns: a delete is not a way to save other changes.
func (t *TableOf[R, K]) setTombstone(ctx context.Context, db Executor, rows []*R, config batchConfig, deleted bool) error {
	if len(rows) == 0 {
		return nil
	}

	def, scope, err := t.prepareWrite(db, rows)
	if err != nil {
		return err
	}

	op := "delete"
	if !deleted {
		op = "restore"
	}

	if err := checkKeys(def, rows, op); err != nil {
		return err
	}

	now := stampTime()

	stamp := map[string]any{}
	if deleted {
		if stamp, err = t.tombstoneValues(now); err != nil {
			return err
		}
	} else if col := def.column(def.managed.UpdatedAt); col != nil {
		v := reflect.New(field(new(R), col).Type()).Elem()
		if err := applyTimestamp(v, now); err != nil {
			return fmt.Errorf("table %s: %w", def.name, err)
		}

		stamp[def.managed.UpdatedAt] = v.Interface()
	}

	version := def.column(def.managed.Version)
	size := effectiveChunkSize(config.size, 2, sqld.MaxBindParams(scope.dialect))

	for _, chunk := range chunks(rows, size) {
		w := &writeStmt{d: scope.dialect}
		w.text("UPDATE ").ident(def.name).text(" SET ").ident(def.managed.DeletedAt).text(" = ")

		switch {
		case deleted:
			w.arg(stamp[def.managed.DeletedAt])
		case def.tombstoneIsZero:
			w.text("0")
		default:
			w.text("NULL")
		}

		if name := def.managed.UpdatedAt; name != "" {
			w.text(", ").ident(name).text(" = ").arg(stamp[name])
		}

		if version != nil {
			w.text(", ").ident(version.name).text(" = ").ident(version.name).text(" + 1")
		}

		w.text(" WHERE ")
		writeKeyMatch(w, def, chunk)
		writeTombstoneFilter(w, def, !deleted)

		need := "a live row"
		if !deleted {
			need = "a deleted row"
		}

		if err := t.execCounted(ctx, db, w, def, op, chunk, wrongRowState(def.name, op, need)); err != nil {
			return err
		}

		for _, row := range chunk {
			tombstone := field(row, def.column(def.managed.DeletedAt))
			if deleted {
				if err := applyTombstone(tombstone, now); err != nil {
					return fmt.Errorf("table %s: %w", def.name, err)
				}
			} else {
				tombstone.SetZero()
			}

			if col := def.column(def.managed.UpdatedAt); col != nil {
				field(row, col).Set(reflect.ValueOf(stamp[col.name]))
			}

			if version != nil {
				incrementVersion(field(row, version))
			}
		}
	}

	return nil
}

// writeTombstoneFilter appends the condition that a row is deleted (true) or live.
func writeTombstoneFilter(w *writeStmt, def *tableDef, deleted bool) {
	w.text(" AND ").ident(def.managed.DeletedAt)

	switch {
	case def.tombstoneIsZero && deleted:
		w.text(" <> 0")
	case def.tombstoneIsZero:
		w.text(" = 0")
	case deleted:
		w.text(" IS NOT NULL")
	default:
		w.text(" IS NULL")
	}
}

func incrementVersion(v reflect.Value) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(v.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	}
}

// HardDelete removes row from the table, ignoring any deleted_at column.
func (t *TableOf[R, K]) HardDelete(ctx context.Context, db Executor, row *R) error {
	return t.BatchHardDelete(ctx, db, []*R{row}, WithBatchSize(1))
}

// BatchHardDelete removes rows from the table, ignoring any deleted_at column.
func (t *TableOf[R, K]) BatchHardDelete(ctx context.Context, db Executor, rows []*R, options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpDelete), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		return t.hardDelete(ctx, db, rows, config)
	})
}

// value reads what row holds in col, through the column's typed accessor rather
// than reflection: a batch write binds one of these per column per row.
func value[R any](row *R, col *columnCore) any {
	if col == nil || col.get == nil {
		return field(row, col).Interface()
	}

	return col.get(row)
}

// field returns the addressable field of row behind column.
func field[R any](row *R, col *columnCore) reflect.Value {
	return reflect.ValueOf(col.scan(row)).Elem()
}

// writeStmt builds one statement for a known dialect.
type writeStmt struct {
	d    sqld.Dialect
	sql  strings.Builder
	args []any
	err  error
}

func (w *writeStmt) text(s string) *writeStmt {
	w.sql.WriteString(s)
	return w
}

func (w *writeStmt) ident(name string) *writeStmt {
	if err := validateIdentifierForDialect(name, w.d); err != nil && w.err == nil {
		w.err = err
	}

	w.sql.WriteString(w.d.QuoteIdent(name))

	return w
}

func (w *writeStmt) arg(v any) *writeStmt {
	w.args = append(w.args, bindValue(v))
	w.sql.WriteString(w.d.Placeholder(len(w.args) - 1))

	return w
}

func (t *TableOf[R, K]) prepareWrite(db Executor, rows []*R) (*tableDef, execScope, error) {
	def, err := t.ready()
	if err != nil {
		return nil, execScope{}, err
	}

	scope, err := executorScope(db)
	if err != nil {
		return nil, execScope{}, err
	}

	for i, row := range rows {
		if row == nil {
			return nil, execScope{}, fmt.Errorf("row %d is nil", i)
		}
	}

	if def.primaryKey == nil {
		return nil, execScope{}, fmt.Errorf("table %s has no primary key", def.name)
	}

	return def, scope, nil
}

// isUnset reports whether a managed timestamp field holds no value yet.
func isUnset(v reflect.Value) bool {
	if v.IsZero() {
		return true
	}

	if valuer, ok := reflect.TypeAssert[driver.Valuer](v); ok {
		value, err := valuer.Value()
		return err == nil && value == nil
	}

	return false
}

func (t *TableOf[R, K]) insert(ctx context.Context, db Executor, rows []*R, config batchConfig) error {
	if len(rows) == 0 {
		return nil
	}

	def, scope, err := t.prepareWrite(db, rows)
	if err != nil {
		return err
	}

	now := stampTime()

	for _, row := range rows {
		for _, name := range []string{def.managed.CreatedAt, def.managed.UpdatedAt} {
			if col := def.column(name); col != nil && isUnset(field(row, col)) {
				if err := applyTimestamp(field(row, col), now); err != nil {
					return fmt.Errorf("table %s: %w", def.name, err)
				}
			}
		}
	}

	// Rows that leave a column to the database (a generated key, an unset column
	// with a DEFAULT) omit it from the statement, so rows are grouped by what they
	// write and each group gets its own INSERT.
	groups := map[string][]*R{}
	order := []string{}

	for _, row := range rows {
		key := t.insertColumnKey(def, row)
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}

		groups[key] = append(groups[key], row)
	}

	for _, key := range order {
		group := groups[key]
		omitKey := def.autoIncrement && field(group[0], def.primaryKey).IsZero()
		cols := t.insertColumns(def, group[0])

		if len(cols) == 0 {
			return fmt.Errorf("insert into %s: every column is left to the database", def.name)
		}

		if config.skipDuplicates {
			if err := t.insertSkippingDuplicates(ctx, db, scope, def, cols, group, omitKey); err != nil {
				return err
			}

			continue
		}

		size := effectiveChunkSize(config.size, len(cols), sqld.MaxBindParams(scope.dialect))
		for _, chunk := range chunks(group, size) {
			if err := t.insertChunk(ctx, db, scope, def, cols, chunk, omitKey); err != nil {
				return fmt.Errorf("insert into %s: %w", def.name, err)
			}
		}
	}

	// One row reads back what the database filled in. A batch does not: that would
	// be one query per row, and the caller asked for as few statements as possible.
	if len(rows) == 1 {
		if filled := t.databaseFilled(def, rows[0]); len(filled) > 0 {
			return t.reloadColumns(ctx, db, scope, def, rows[0], filled)
		}
	}

	return nil
}

// insertColumns are the columns an INSERT of row writes: not a zero generated key,
// not a generated column, and not an unset column the database defaults.
func (t *TableOf[R, K]) insertColumns(def *tableDef, row *R) []*columnCore {
	cols := make([]*columnCore, 0, len(def.columns))

	for _, col := range def.columns {
		switch {
		case col == def.primaryKey && def.autoIncrement && field(row, col).IsZero():
		case col.fill == tsqdialect.FillGenerated:
		case col.fill == tsqdialect.FillDefault && isUnset(field(row, col)):
		default:
			cols = append(cols, col)
		}
	}

	return cols
}

// insertColumnKey groups rows that write the same columns.
func (t *TableOf[R, K]) insertColumnKey(def *tableDef, row *R) string {
	var key strings.Builder

	for _, col := range t.insertColumns(def, row) {
		key.WriteString(col.name)
		key.WriteByte(0)
	}

	return key.String()
}

// databaseFilled are the columns of row the database provided, which an insert of
// one row reads back.
func (t *TableOf[R, K]) databaseFilled(def *tableDef, row *R) []*columnCore {
	var cols []*columnCore

	for _, col := range def.columns {
		if col == def.primaryKey {
			continue
		}

		if col.fill == tsqdialect.FillGenerated || (col.fill == tsqdialect.FillDefault && isUnset(field(row, col))) {
			cols = append(cols, col)
		}
	}

	return cols
}

func (t *TableOf[R, K]) insertChunk(ctx context.Context, db Executor, scope execScope, def *tableDef, cols []*columnCore, rows []*R, omitKey bool) error {
	w := &writeStmt{d: scope.dialect}
	w.text("INSERT INTO ").ident(def.name).text(" (")

	for i, col := range cols {
		if i > 0 {
			w.text(", ")
		}

		w.ident(col.name)
	}

	w.text(") VALUES ")

	for i, row := range rows {
		if i > 0 {
			w.text(", ")
		}

		w.text("(")

		for j, col := range cols {
			if j > 0 {
				w.text(", ")
			}

			w.arg(value(row, col))
		}

		w.text(")")
	}

	returning := ""
	if omitKey {
		returning = scope.dialect.ReturningClause(def.primaryKey.name)
	}

	w.text(returning)

	if w.err != nil {
		return w.err
	}

	logSQLForExecutor(ctx, db, "insert", w.sql.String(), w.args)

	if returning != "" {
		return t.insertReturning(ctx, db, def, w, rows)
	}

	result, err := db.ExecContext(ctx, w.sql.String(), w.args...)
	if err != nil {
		return err
	}

	if omitKey {
		assignInsertIDs(ctx, db, scope.dialect, def, rows, result)
	}

	return nil
}

func (t *TableOf[R, K]) insertReturning(ctx context.Context, db Executor, def *tableDef, w *writeStmt, rows []*R) error {
	result, err := db.QueryContext(ctx, w.sql.String(), w.args...)
	if err != nil {
		return err
	}

	defer func() { _ = result.Close() }()

	i := 0

	for result.Next() {
		var id int64
		if err := result.Scan(&id); err != nil {
			return fmt.Errorf("scan generated key: %w", err)
		}

		if i < len(rows) {
			setID(field(rows[i], def.primaryKey), id)
		}

		i++
	}

	if err := result.Err(); err != nil {
		return err
	}

	if i != len(rows) {
		logForExecutor(ctx, db, slog.LevelWarn, "insert returned an unexpected number of keys",
			"table", def.name, "expected", len(rows), "actual", i)
	}

	return nil
}

func assignInsertIDs[R any](ctx context.Context, db Executor, d sqld.Dialect, def *tableDef, rows []*R, result sql.Result) {
	lastID, err := result.LastInsertId()
	if err != nil {
		return
	}

	if len(rows) == 1 {
		setID(field(rows[0], def.primaryKey), lastID)
		return
	}

	affected, err := result.RowsAffected()
	if err != nil || affected != int64(len(rows)) {
		logForExecutor(ctx, db, slog.LevelWarn, "generated keys not assigned: rows affected mismatch",
			"table", def.name, "expected", len(rows), "actual", affected, "error", err)

		return
	}

	start, ok := d.BatchInsertStartID(lastID, affected)
	if !ok {
		return
	}

	for i, row := range rows {
		setID(field(row, def.primaryKey), start+int64(i))
	}
}

func setID(v reflect.Value, id int64) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(id)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// A negative id means the driver could not report one; zero says "unknown".
		if id >= 0 {
			v.SetUint(uint64(id))
		}
	}
}

// Every attempt reuses one savepoint, released or rolled back before the next row.
const (
	insertSavepoint         = "tsq_batch_insert"
	insertSavepointCreate   = "SAVEPOINT " + insertSavepoint
	insertSavepointRelease  = "RELEASE SAVEPOINT " + insertSavepoint
	insertSavepointRollback = "ROLLBACK TO SAVEPOINT " + insertSavepoint
)

// insertSkippingDuplicates inserts one row at a time and skips duplicate-key
// failures.
//
// Inside a transaction each row is bracketed by a savepoint: PostgreSQL aborts
// the whole transaction on any failed statement, so catching the error and moving
// on does not work there. Outside a transaction every insert is its own
// transaction and PostgreSQL rejects SAVEPOINT, so none is used.
func (t *TableOf[R, K]) insertSkippingDuplicates(ctx context.Context, db Executor, scope execScope, def *tableDef, cols []*columnCore, rows []*R, omitKey bool) error {
	for i, row := range rows {
		if scope.tx {
			if _, err := db.ExecContext(ctx, insertSavepointCreate); err != nil {
				return fmt.Errorf("insert into %s, row %d: %w", def.name, i, err)
			}
		}

		err := t.insertChunk(ctx, db, scope, def, cols, []*R{row}, omitKey)
		if err == nil {
			if scope.tx {
				if _, err := db.ExecContext(ctx, insertSavepointRelease); err != nil {
					return fmt.Errorf("insert into %s, row %d: %w", def.name, i, err)
				}
			}

			continue
		}

		if !isDuplicateKeyError(err) {
			return fmt.Errorf("insert into %s, row %d: %w", def.name, i, err)
		}

		if scope.tx {
			if _, rollbackErr := db.ExecContext(ctx, insertSavepointRollback); rollbackErr != nil {
				return fmt.Errorf("insert into %s, row %d: %w", def.name, i, errors.Join(err, rollbackErr))
			}
		}

		logForExecutor(ctx, db, slog.LevelDebug, "skipped duplicate row", "table", def.name, "error", err)
	}

	return nil
}

// writeKeyMatch writes the WHERE clause selecting rows by key, and by version when
// the table has one.
func writeKeyMatch[R any](w *writeStmt, def *tableDef, rows []*R) {
	version := def.column(def.managed.Version)
	pk := def.primaryKey

	if version == nil {
		w.ident(pk.name).text(" IN (")

		for i, row := range rows {
			if i > 0 {
				w.text(", ")
			}

			w.arg(value(row, pk))
		}

		w.text(")")

		return
	}

	if len(rows) > 1 {
		w.text("(")
	}

	for i, row := range rows {
		if i > 0 {
			w.text(" OR ")
		}

		w.text("(").ident(pk.name).text(" = ").arg(value(row, pk))
		w.text(" AND ").ident(version.name).text(" = ").arg(value(row, version)).text(")")
	}

	if len(rows) > 1 {
		w.text(")")
	}
}

func checkKeys[R any](def *tableDef, rows []*R, op string) error {
	for i, row := range rows {
		if field(row, def.primaryKey).IsZero() {
			return fmt.Errorf("%s %s: row %d has a zero primary key", op, def.name, i)
		}
	}

	return nil
}

func (t *TableOf[R, K]) update(ctx context.Context, db Executor, rows []*R, config batchConfig, now time.Time) error {
	if len(rows) == 0 {
		return nil
	}

	def, scope, err := t.prepareWrite(db, rows)
	if err != nil {
		return err
	}

	if err := checkKeys(def, rows, "update"); err != nil {
		return err
	}

	if col := def.column(def.managed.UpdatedAt); col != nil {
		for _, row := range rows {
			if err := applyTimestamp(field(row, col), now); err != nil {
				return fmt.Errorf("table %s: %w", def.name, err)
			}
		}
	}

	version := def.column(def.managed.Version)

	// created_at is written once, and deleted_at only by Delete and Restore: a row
	// built by hand, or loaded before a concurrent delete, must not overwrite them.
	writable := func(col *columnCore) bool {
		switch col.name {
		case def.primaryKey.name, def.managed.CreatedAt, def.managed.DeletedAt:
			return false
		}

		// A generated column is the database's to compute, never ours to write.
		return col != version && col.fill != tsqdialect.FillGenerated
	}

	cols := make([]*columnCore, 0, len(def.columns))

	if config.only == nil {
		for _, row := range rows {
			if err := checkFullRow("update", def.name, row); err != nil {
				return err
			}
		}

		for _, col := range def.columns {
			if writable(col) {
				cols = append(cols, col)
			}
		}
	} else {
		for _, only := range config.only {
			col, err := updateColumn(def, only, writable)
			if err != nil {
				return err
			}

			if !slices.Contains(cols, col) {
				cols = append(cols, col)
			}
		}

		// updated_at is refreshed by every update, whichever columns it names.
		if at := def.column(def.managed.UpdatedAt); at != nil && !slices.Contains(cols, at) {
			cols = append(cols, at)
		}
	}

	if len(cols) == 0 && version == nil {
		return fmt.Errorf("update %s: the table has no column to update", def.name)
	}

	// Each column binds a key and a value per row, and the WHERE clause one or two
	// more per row.
	size := effectiveChunkSize(config.size, 2*len(cols)+2, sqld.MaxBindParams(scope.dialect))

	for _, chunk := range chunks(rows, size) {
		if err := t.updateChunk(ctx, db, scope, def, cols, version, chunk); err != nil {
			return err
		}
	}

	return nil
}

func (t *TableOf[R, K]) updateChunk(ctx context.Context, db Executor, scope execScope, def *tableDef, cols []*columnCore, version *columnCore, rows []*R) error {
	w := &writeStmt{d: scope.dialect}
	w.text("UPDATE ").ident(def.name).text(" SET ")

	for i, col := range cols {
		if i > 0 {
			w.text(", ")
		}

		w.ident(col.name).text(" = ")

		if len(rows) == 1 {
			w.arg(value(rows[0], col))
			continue
		}

		w.text("CASE ").ident(def.primaryKey.name)

		for _, row := range rows {
			w.text(" WHEN ").arg(value(row, def.primaryKey))
			w.text(" THEN ").arg(value(row, col))
		}

		w.text(" ELSE ").ident(col.name).text(" END")
	}

	if version != nil {
		if len(cols) > 0 {
			w.text(", ")
		}

		w.ident(version.name).text(" = ").ident(version.name).text(" + 1")
	}

	w.text(" WHERE ")
	writeKeyMatch(w, def, rows)

	if t.softDeleted() {
		writeTombstoneFilter(w, def, false)
	}

	if err := t.execCounted(ctx, db, w, def, "update", rows, versionGuard(def, version)); err != nil {
		return err
	}

	if version != nil {
		for _, row := range rows {
			incrementVersion(field(row, version))
		}
	}

	return nil
}

// execCounted runs w and, when the rows are version-guarded, reports an
// OptimisticLockError unless every row matched.
// execCounted runs the statement. When mismatch is set, it checks that the
// statement matched every row and turns a shortfall into that error.
func (t *TableOf[R, K]) execCounted(ctx context.Context, db Executor, w *writeStmt, def *tableDef, op string, rows []*R, mismatch func(expected, actual int64) error) error {
	if w.err != nil {
		return w.err
	}

	logSQLForExecutor(ctx, db, op, w.sql.String(), w.args)

	// A single row is named by its key so the error says which one; the row itself is
	// never printed, because its columns may carry data that must not reach logs.
	target := def.name
	if len(rows) == 1 {
		target = fmt.Sprintf("%s %s=%v", def.name, def.primaryKey.name, value(rows[0], def.primaryKey))
	}

	result, err := db.ExecContext(ctx, w.sql.String(), w.args...)
	if err != nil {
		return fmt.Errorf("%s %s: %w", op, target, err)
	}

	if mismatch == nil {
		return nil
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s %s: %w", op, target, err)
	}

	if affected != int64(len(rows)) {
		return fmt.Errorf("%s %s: %w", op, target, mismatch(int64(len(rows)), affected))
	}

	return nil
}

// versionGuard checks the row count only when the table has a version column: it
// is what makes a mismatch mean "someone else changed it".
func versionGuard(def *tableDef, version *columnCore) func(int64, int64) error {
	if version == nil {
		return nil
	}

	return versionConflict(def.name)
}

// versionConflict is the mismatch error of a version-guarded write.
func versionConflict(table string) func(int64, int64) error {
	return func(expected, actual int64) error {
		return &OptimisticLockError{Table: table, Expected: expected, Actual: actual}
	}
}

// wrongRowState is the mismatch error of a write that needs the row in one state.
func wrongRowState(table, op, need string) func(int64, int64) error {
	return func(expected, actual int64) error {
		return &RowStateError{Table: table, Op: op, Need: need, Expected: expected, Actual: actual}
	}
}

func (t *TableOf[R, K]) hardDelete(ctx context.Context, db Executor, rows []*R, config batchConfig) error {
	if len(rows) == 0 {
		return nil
	}

	def, scope, err := t.prepareWrite(db, rows)
	if err != nil {
		return err
	}

	if err := checkKeys(def, rows, "delete"); err != nil {
		return err
	}

	version := def.column(def.managed.Version)
	size := effectiveChunkSize(config.size, 2, sqld.MaxBindParams(scope.dialect))

	for _, chunk := range chunks(rows, size) {
		w := &writeStmt{d: scope.dialect}
		w.text("DELETE FROM ").ident(def.name).text(" WHERE ")
		writeKeyMatch(w, def, chunk)

		if err := t.execCounted(ctx, db, w, def, "delete", chunk, versionGuard(def, version)); err != nil {
			return err
		}
	}

	return nil
}

// stampDeleted writes the tombstone and updated_at into row.
func (t *TableOf[R, K]) stampDeleted(row *R, now time.Time) error {
	def := t.def

	if err := applyTombstone(field(row, def.column(def.managed.DeletedAt)), now); err != nil {
		return fmt.Errorf("table %s: %w", def.name, err)
	}

	if col := def.column(def.managed.UpdatedAt); col != nil {
		if err := applyTimestamp(field(row, col), now); err != nil {
			return fmt.Errorf("table %s: %w", def.name, err)
		}
	}

	return nil
}

// tombstoneValues returns the values a soft delete at now writes, per column, for
// statements that hold no row.
func (t *TableOf[R, K]) tombstoneValues(now time.Time) (map[string]any, error) {
	row := new(R)
	if err := t.stampDeleted(row, now); err != nil {
		return nil, err
	}

	values := map[string]any{}

	for _, name := range []string{t.def.managed.DeletedAt, t.def.managed.UpdatedAt} {
		if col := t.def.column(name); col != nil {
			values[name] = field(row, col).Interface()
		}
	}

	return values, nil
}

// applyTombstone marks a deleted_at field. Integer columns hold Unix nanoseconds;
// time-shaped columns hold the instant.
func applyTombstone(v reflect.Value, ts time.Time) error {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(ts.UnixNano())
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(uint64(ts.UnixNano()))
		return nil
	}

	return applyTimestamp(v, ts)
}

// applyTimestamp writes ts into a time.Time, *time.Time or a nullable time wrapper
// that implements sql.Scanner (sql.NullTime, null.Time).
func applyTimestamp(v reflect.Value, ts time.Time) error {
	switch v.Type() {
	case reflect.TypeFor[time.Time]():
		v.Set(reflect.ValueOf(ts))
		return nil
	case reflect.TypeFor[*time.Time]():
		v.Set(reflect.ValueOf(&ts))
		return nil
	}

	if scanner, ok := reflect.TypeAssert[sql.Scanner](v.Addr()); ok {
		if err := scanner.Scan(ts); err != nil {
			return fmt.Errorf("set managed time column: %w", err)
		}

		return nil
	}

	return fmt.Errorf("managed time column has unsupported type %s", v.Type())
}

// BatchDeleteByPK deletes the rows whose primary key is in keys, as Delete would:
// a soft delete on a table with deleted_at (rows already deleted keep their
// tombstone), otherwise a hard delete. It does not check versions, but a soft
// delete increments them.
func (t *TableOf[R, K]) BatchDeleteByPK(ctx context.Context, db Executor, keys []K, options ...BatchOption) error {
	return t.deleteByPK(ctx, db, keys, options, t.def.managed.DeletedAt != "")
}

// BatchHardDeleteByPK removes the rows whose primary key is in keys, deleted rows
// included.
func (t *TableOf[R, K]) BatchHardDeleteByPK(ctx context.Context, db Executor, keys []K, options ...BatchOption) error {
	return t.deleteByPK(ctx, db, keys, options, false)
}

func (t *TableOf[R, K]) deleteByPK(ctx context.Context, db Executor, keys []K, options []BatchOption, soft bool) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpDelete), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		def, scope, err := t.prepareWrite(db, nil)
		if err != nil {
			return err
		}

		if len(keys) == 0 {
			return nil
		}

		ids := make([]any, len(keys))
		for i, key := range keys {
			ids[i] = key
		}

		var stamp map[string]any
		if soft {
			if stamp, err = t.tombstoneValues(stampTime()); err != nil {
				return err
			}
		}

		size := effectiveChunkSize(config.size, 1, sqld.MaxBindParams(scope.dialect)-len(stamp))

		for _, chunk := range chunks(ids, size) {
			w := &writeStmt{d: scope.dialect}

			if soft {
				w.text("UPDATE ").ident(def.name).text(" SET ")
				writeTombstoneSet(w, def, stamp)
			} else {
				w.text("DELETE FROM ").ident(def.name)
			}

			w.text(" WHERE ").ident(def.primaryKey.name).text(" IN (")

			for i, id := range chunk {
				if i > 0 {
					w.text(", ")
				}

				w.arg(id)
			}

			w.text(")")

			if soft {
				// A row that is already deleted keeps its original tombstone.
				w.text(" AND ").ident(def.managed.DeletedAt)

				if def.tombstoneIsZero {
					w.text(" = 0")
				} else {
					w.text(" IS NULL")
				}
			}

			if err := t.execCounted(ctx, db, w, def, "delete", nil, nil); err != nil {
				return err
			}
		}

		return nil
	})
}

func writeTombstoneSet(w *writeStmt, def *tableDef, stamp map[string]any) {
	w.ident(def.managed.DeletedAt).text(" = ").arg(stamp[def.managed.DeletedAt])

	if def.managed.UpdatedAt != "" {
		w.text(", ").ident(def.managed.UpdatedAt).text(" = ").arg(stamp[def.managed.UpdatedAt])
	}

	if def.managed.Version != "" {
		w.text(", ").ident(def.managed.Version).text(" = ").ident(def.managed.Version).text(" + 1")
	}
}

// reloadColumns reads cols of row back from the database, for values the database
// provided: a DEFAULT, a generated expression, or a version an upsert advanced.
func (t *TableOf[R, K]) reloadColumns(ctx context.Context, db Executor, scope execScope, def *tableDef, row *R, cols []*columnCore) error {
	pk := field(row, def.primaryKey)
	if len(cols) == 0 || pk.IsZero() {
		return nil
	}

	w := &writeStmt{d: scope.dialect}
	w.text("SELECT ")

	dest := make([]any, 0, len(cols))

	for i, col := range cols {
		if i > 0 {
			w.text(", ")
		}

		w.ident(col.name)

		dest = append(dest, col.scan(row))
	}

	w.text(" FROM ").ident(def.name).text(" WHERE ").ident(def.primaryKey.name).text(" = ").arg(pk.Interface())

	if w.err != nil {
		return w.err
	}

	logSQLForExecutor(ctx, db, "reload", w.sql.String(), w.args)

	if err := db.QueryRowContext(ctx, w.sql.String(), w.args...).Scan(dest...); err != nil {
		return fmt.Errorf("reload %s %s=%v: %w", def.name, def.primaryKey.name, pk.Interface(), err)
	}

	return nil
}

// updateColumn resolves a column Update was told to write.
func updateColumn(def *tableDef, col SQLColumn, writable func(*columnCore) bool) (*columnCore, error) {
	if isNilValue(col) {
		return nil, fmt.Errorf("update %s: column cannot be nil", def.name)
	}

	core := col.core()
	if err := core.err(); err != nil {
		return nil, err
	}

	own := def.column(core.name)
	if own == nil || isNilValue(core.table) || core.table.definition() != def || !core.plain {
		return nil, fmt.Errorf("update %s: %s is not a column of the table", def.name, core.name)
	}

	if !writable(own) {
		return nil, fmt.Errorf("update %s: column %s is maintained by TSQ or the database and cannot be written by Update", def.name, core.name)
	}

	return own, nil
}
