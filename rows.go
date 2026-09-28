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
// either way. A row changed since it was loaded fails with OptimisticLockError;
// without a version column, a row that is gone fails with RowStateError.
func (t *TableOf[R, K]) Update(ctx context.Context, db Executor, row *R, cols ...BoundColumn[R]) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpdate), func(ctx context.Context) error {
		config := batchConfig{size: 1}
		if len(cols) > 0 {
			config.only = sqlColumns(cols...)
		}

		return t.update(ctx, db, []*R{row}, config, stampTime())
	})
}

// BatchUpdate updates rows in as few statements as the batch size allows. The
// statements are not a transaction: when some rows are stale, the others are
// still written. The error then names the stale rows in its Keys, and the written
// rows carry their new version and updated_at, so only the stale ones need
// reloading. Wrap the call in WithTx for all or nothing. Two rows with the same
// primary key are refused.
func (t *TableOf[R, K]) BatchUpdate(ctx context.Context, db Executor, rows []*R, options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpdate), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		return t.update(ctx, db, rows, config, stampTime())
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
	// The stamps bind once per statement, the key match per row.
	size := effectiveChunkSize(config.size, keyMatchParams(def), sqld.MaxBindParams(scope.dialect)-len(stamp))

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

		if !deleted || t.softDeleted() {
			writeTombstoneFilter(w, def, !deleted)
		}

		need := "a live row"
		if !deleted {
			need = "a deleted row"
		}

		if err := t.execCounted(ctx, db, w, def, op, chunk, t.tombstoneMismatch(ctx, db, scope, def, version, op, need, chunk)); err != nil {
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

			// Each row gets its own value: a *time.Time shared by every row would let
			// a change to one row's time change them all.
			if col := def.column(def.managed.UpdatedAt); col != nil {
				if err := applyTimestamp(field(row, col), now); err != nil {
					return fmt.Errorf("table %s: %w", def.name, err)
				}
			}

			if version != nil {
				incrementVersion(field(row, version))
			}
		}
	}

	return nil
}

// tombstoneMismatch is the error of a delete or restore that matched fewer rows
// than it was given. The statement checks the version and the row's state at once,
// and the two failures need different handling: a row changed since it was loaded
// is an OptimisticLockError, which a retry after reloading fixes, and a row in the
// wrong state is a RowStateError, which it does not. With a version column the
// rows are read back to tell which; the read only happens on this error path.
func (t *TableOf[R, K]) tombstoneMismatch(ctx context.Context, db Executor, scope execScope, def *tableDef, version *columnCore, op, need string, rows []*R) func(int64, int64) error {
	state := wrongRowState(def.name, op, need)
	if version == nil {
		return state
	}

	return func(expected, actual int64) error {
		changed, err := t.versionsChanged(ctx, db, scope, def, version, rows)

		switch {
		case err != nil:
			return errors.Join(state(expected, actual), fmt.Errorf("read back the versions: %w", err))
		case changed:
			return versionConflict(def.name)(expected, actual)
		}

		return state(expected, actual)
	}
}

// versionsChanged reports whether one of rows is gone or holds another version than
// the one it was loaded with. Values are scanned into the fields' own types, so they
// compare the same whatever the driver hands back.
func (t *TableOf[R, K]) versionsChanged(ctx context.Context, db Executor, scope execScope, def *tableDef, version *columnCore, rows []*R) (bool, error) {
	w := &writeStmt{d: scope.dialect}
	w.text("SELECT ").ident(def.primaryKey.name).text(", ").ident(version.name).text(" FROM ").ident(def.name)
	w.text(" WHERE ").ident(def.primaryKey.name).text(" IN (")

	for i, row := range rows {
		if i > 0 {
			w.text(", ")
		}

		w.arg(value(row, def.primaryKey))
	}

	w.text(")")

	if w.err != nil {
		return false, w.err
	}

	result, err := db.QueryContext(ctx, w.sql.String(), w.args...)
	if err != nil {
		return false, err
	}

	defer func() { _ = result.Close() }()

	stored := make(map[string]string, len(rows))

	for result.Next() {
		key := reflect.New(field(new(R), def.primaryKey).Type())
		held := reflect.New(field(new(R), version).Type())

		if err := result.Scan(key.Interface(), held.Interface()); err != nil {
			return false, err
		}

		stored[keyText(key.Elem().Interface())] = keyText(held.Elem().Interface())
	}

	if err := result.Err(); err != nil {
		return false, err
	}

	for _, row := range rows {
		if held, ok := stored[keyText(value(row, def.primaryKey))]; !ok || held != keyText(value(row, version)) {
			return true, nil
		}
	}

	return false, nil
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

// fieldSnapshot holds some fields of rows as they were before a write set them,
// to put back on the rows the write did not store: a failed write must not leave
// the caller's rows holding values the database never saw.
type fieldSnapshot[R any] struct {
	rows   []*R
	cols   []*columnCore
	values [][]reflect.Value
}

func snapshotFields[R any](rows []*R, cols ...*columnCore) *fieldSnapshot[R] {
	s := &fieldSnapshot[R]{rows: rows}

	for _, col := range cols {
		if col != nil {
			s.cols = append(s.cols, col)
		}
	}

	for _, row := range rows {
		held := make([]reflect.Value, 0, len(s.cols))

		for _, col := range s.cols {
			f := field(row, col)
			v := reflect.New(f.Type()).Elem()
			v.Set(f)
			held = append(held, v)
		}

		s.values = append(s.values, held)
	}

	return s
}

// restore puts the fields back on every row not in written.
func (s *fieldSnapshot[R]) restore(written map[*R]bool) {
	for i, row := range s.rows {
		if written[row] {
			continue
		}

		for j, col := range s.cols {
			field(row, col).Set(s.values[i][j])
		}
	}
}

// isUnset reports whether a managed timestamp field holds no value yet.
func isUnset(v reflect.Value) bool {
	if v.IsZero() {
		return true
	}

	// A pointer to the zero time holds no time either, however it was made.
	if v.Kind() == reflect.Pointer {
		return isUnset(v.Elem())
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
	snapshot := snapshotFields(rows, def.column(def.managed.CreatedAt), def.column(def.managed.UpdatedAt), def.primaryKey)
	written := make(map[*R]bool, len(rows))

	for _, row := range rows {
		for _, name := range []string{def.managed.CreatedAt, def.managed.UpdatedAt} {
			if col := def.column(name); col != nil && isUnset(field(row, col)) {
				if err := applyTimestamp(field(row, col), now); err != nil {
					snapshot.restore(nil)
					return fmt.Errorf("table %s: %w", def.name, err)
				}
			}
		}
	}

	if err := t.insertGroups(ctx, db, scope, def, rows, config, written); err != nil {
		snapshot.restore(written)
		return err
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

// insertGroups inserts rows, recording in written the rows each statement stored.
func (t *TableOf[R, K]) insertGroups(ctx context.Context, db Executor, scope execScope, def *tableDef, rows []*R, config batchConfig, written map[*R]bool) error {
	// Rows that leave a column to the database (a generated key, an unset column
	// with a DEFAULT) omit it from the statement, so rows are grouped by what they
	// write and each group gets its own INSERT.
	groups := map[string][]*R{}
	order := []string{}

	for _, row := range rows {
		key := columnsKey(t.insertColumns(def, row))
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

			for _, row := range chunk {
				written[row] = true
			}
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

// columnsKey names a set of columns, to group rows that write the same ones.
func columnsKey(cols []*columnCore) string {
	var key strings.Builder

	for _, col := range cols {
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
		return assignInsertIDs(ctx, db, scope.dialect, def, rows, result)
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

// assignInsertIDs writes the generated keys of an INSERT into rows. A driver that
// cannot report them is an error: the rows would otherwise keep a zero key that
// every later write of them refuses, with nothing saying why.
func assignInsertIDs[R any](ctx context.Context, db Executor, d sqld.Dialect, def *tableDef, rows []*R, result sql.Result) error {
	lastID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("read the generated key: %w", err)
	}

	if len(rows) == 1 {
		setID(field(rows[0], def.primaryKey), lastID)
		return nil
	}

	affected, err := result.RowsAffected()
	if err != nil || affected != int64(len(rows)) {
		logForExecutor(ctx, db, slog.LevelWarn, "generated keys not assigned: rows affected mismatch",
			"table", def.name, "expected", len(rows), "actual", affected, "error", err)

		return nil
	}

	step, err := insertIDStep(ctx, db, d)
	if err != nil {
		return err
	}

	start, ok := d.BatchInsertStartID(lastID, affected, step)
	if !ok {
		return nil
	}

	for i, row := range rows {
		setID(field(row, def.primaryKey), start+int64(i)*step)
	}

	return nil
}

// insertIDStep is the distance between the keys one INSERT generates: MySQL's
// auto_increment_increment, which a multi-primary setup raises above 1. It is read
// on the statement's own executor, once per multi-row INSERT, so a session that
// sets it is followed.
func insertIDStep(ctx context.Context, db Executor, d sqld.Dialect) (int64, error) {
	query := d.InsertIDStepQuery()
	if query == "" {
		return 1, nil
	}

	var step int64
	if err := db.QueryRowContext(ctx, query).Scan(&step); err != nil {
		return 0, fmt.Errorf("read the auto-increment step: %w", err)
	}

	return max(step, 1), nil
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

	if len(rows) == 1 {
		w.text("(").ident(pk.name).text(" = ").arg(value(rows[0], pk))
		w.text(" AND ").ident(version.name).text(" = ").arg(value(rows[0], version)).text(")")

		return
	}

	// Not (pk = ? AND version = ?) OR ...: each OR nests the expression one level
	// deeper, and SQLite refuses one deeper than 1000, the default batch size. The
	// arms of a CASE are a flat list, spelled the same on every dialect.
	w.ident(pk.name).text(" IN (")

	for i, row := range rows {
		if i > 0 {
			w.text(", ")
		}

		w.arg(value(row, pk))
	}

	w.text(") AND CASE ").ident(pk.name)

	for _, row := range rows {
		w.text(" WHEN ").arg(value(row, pk)).text(" THEN ").ident(version.name).text(" = ").arg(value(row, version))
	}

	w.text(" END")
}

// keyMatchParams is how many parameters writeKeyMatch binds per row.
func keyMatchParams(def *tableDef) int {
	if def.managed.Version != "" {
		return 3
	}

	return 1
}

// checkKeys refuses a row without a key, and two rows with one key: a statement
// writes a row once, so one of the two would be dropped without a word (or, with a
// version column, reported as a conflict).
func checkKeys[R any](def *tableDef, rows []*R, op string) error {
	seen := make(map[string]int, len(rows))

	for i, row := range rows {
		if field(row, def.primaryKey).IsZero() {
			return fmt.Errorf("%s %s: row %d has a zero primary key", op, def.name, i)
		}

		key := keyText(value(row, def.primaryKey))
		if first, dup := seen[key]; dup {
			return fmt.Errorf("%s %s: rows %d and %d have the same primary key %v", op, def.name, first, i, value(row, def.primaryKey))
		}

		seen[key] = i
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

	// Each column binds a key and a value per row, and the WHERE clause its key match.
	size := effectiveChunkSize(config.size, 2*len(cols)+keyMatchParams(def), sqld.MaxBindParams(scope.dialect))

	// A stale row does not stop the batch: the rows after it are written too, and
	// one error names every row that was not.
	var conflict *OptimisticLockError

	for _, chunk := range chunks(rows, size) {
		// The statement reads updated_at from the rows, so it is stamped into them
		// just before, and put back on the rows it did not write: a refused update
		// must not leave the caller's rows holding a time the database never stored.
		restore, err := stampUpdatedAt(def, chunk, now)
		if err != nil {
			return err
		}

		written, err := t.updateChunk(ctx, db, scope, def, cols, version, chunk)
		if err == nil {
			continue
		}

		restore(written)

		stale, ok := errors.AsType[*OptimisticLockError](err)
		if !ok || stale.Keys == nil || len(rows) == len(chunk) {
			return err
		}

		if conflict == nil {
			conflict = &OptimisticLockError{Table: def.name, Expected: int64(len(rows)), Actual: int64(len(rows))}
		}

		conflict.Actual -= int64(len(stale.Keys))
		conflict.Keys = append(conflict.Keys, stale.Keys...)
	}

	if conflict != nil {
		return fmt.Errorf("update %s: %w", def.name, conflict)
	}

	return nil
}

// stampUpdatedAt writes now into the updated_at field of rows and returns what puts
// the previous values back on every row but those keep marks.
func stampUpdatedAt[R any](def *tableDef, rows []*R, now time.Time) (func(keep []bool), error) {
	col := def.column(def.managed.UpdatedAt)
	if col == nil {
		return func([]bool) {}, nil
	}

	previous := make([]reflect.Value, 0, len(rows))
	restore := func(keep []bool) {
		for i, value := range previous {
			if i >= len(keep) || !keep[i] {
				field(rows[i], col).Set(value)
			}
		}
	}

	for _, row := range rows {
		f := field(row, col)
		held := reflect.New(f.Type()).Elem()
		held.Set(f)
		previous = append(previous, held)

		if err := applyTimestamp(f, now); err != nil {
			restore(nil)
			return nil, fmt.Errorf("table %s: %w", def.name, err)
		}
	}

	return restore, nil
}

// updateChunk writes rows in one statement. When it fails, it reports which rows
// the database holds the update for (nil when none), and those rows carry their
// new version as the ones written in full do.
func (t *TableOf[R, K]) updateChunk(ctx context.Context, db Executor, scope execScope, def *tableDef, cols []*columnCore, version *columnCore, rows []*R) ([]bool, error) {
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

	var written []bool

	if err := t.execCounted(ctx, db, w, def, "update", rows, t.updateMismatch(ctx, db, scope, def, version, cols, rows, &written)); err != nil {
		if version != nil {
			for i, row := range rows {
				if i < len(written) && written[i] {
					incrementVersion(field(row, version))
				}
			}
		}

		return written, err
	}

	if version != nil {
		for _, row := range rows {
			incrementVersion(field(row, version))
		}
	}

	return nil, nil
}

// updateMismatch explains an update that matched fewer rows than it was given.
// Batches are not transactions, so the database may hold the update for some of
// the rows: they are read back, and a row counts as written when it holds the next
// version and every value the statement wrote. The rest are named in the error.
//
// Without a version column a shortfall is not a conflict: MySQL counts only the
// rows a statement changed, so an update that writes a row's current values
// reports none. Only rows that no longer exist are an error then.
func (t *TableOf[R, K]) updateMismatch(ctx context.Context, db Executor, scope execScope, def *tableDef, version *columnCore, cols []*columnCore, rows []*R, written *[]bool) func(int64, int64) error {
	return func(expected, actual int64) error {
		compared := make([]*columnCore, 0, len(cols)+1)
		if version != nil {
			compared = append(compared, version)
		}

		// updated_at is left out: a database may store it at a coarser precision
		// than the time that was bound.
		for _, col := range cols {
			if col.name != def.managed.UpdatedAt {
				compared = append(compared, col)
			}
		}

		if version == nil {
			compared = nil
		}

		stored, err := t.readBack(ctx, db, scope, def, compared, rows)

		var keys []any

		done := make([]bool, len(rows))

		for i, row := range rows {
			values, found := stored[keyText(value(row, def.primaryKey))]
			done[i] = found && err == nil

			for j, col := range compared {
				want := field(row, col)
				if col == version {
					next := reflect.New(want.Type()).Elem()
					next.Set(want)
					incrementVersion(next)
					want = next
				}

				if done[i] && values[j] != keyText(want.Interface()) {
					done[i] = false
				}
			}

			if !done[i] {
				keys = append(keys, value(row, def.primaryKey))
			}
		}

		*written = done

		need := "an existing row"
		if t.softDeleted() {
			need = "a live row"
		}

		switch {
		case err != nil && version != nil:
			return errors.Join(&OptimisticLockError{Table: def.name, Expected: expected, Actual: actual},
				fmt.Errorf("read back the rows: %w", err))
		case err != nil:
			return errors.Join(&RowStateError{Table: def.name, Op: "update", Need: need, Expected: expected, Actual: actual},
				fmt.Errorf("read back the rows: %w", err))
		case version != nil:
			return &OptimisticLockError{Table: def.name, Expected: expected, Actual: actual, Keys: keys}
		case len(keys) > 0:
			return &RowStateError{Table: def.name, Op: "update", Need: need, Expected: expected, Actual: expected - int64(len(keys)), Keys: keys}
		}

		return nil
	}
}

// readBack returns cols of the live rows among rows, keyed and rendered by keyText.
func (t *TableOf[R, K]) readBack(ctx context.Context, db Executor, scope execScope, def *tableDef, cols []*columnCore, rows []*R) (map[string][]string, error) {
	w := &writeStmt{d: scope.dialect}
	w.text("SELECT ").ident(def.primaryKey.name)

	for _, col := range cols {
		w.text(", ").ident(col.name)
	}

	w.text(" FROM ").ident(def.name).text(" WHERE ").ident(def.primaryKey.name).text(" IN (")

	for i, row := range rows {
		if i > 0 {
			w.text(", ")
		}

		w.arg(value(row, def.primaryKey))
	}

	w.text(")")

	if t.softDeleted() {
		writeTombstoneFilter(w, def, false)
	}

	if w.err != nil {
		return nil, w.err
	}

	result, err := db.QueryContext(ctx, w.sql.String(), w.args...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = result.Close() }()

	stored := make(map[string][]string, len(rows))

	for result.Next() {
		key := reflect.New(field(new(R), def.primaryKey).Type())
		dest := []any{key.Interface()}

		held := make([]reflect.Value, 0, len(cols))
		for _, col := range cols {
			v := reflect.New(field(new(R), col).Type())
			held = append(held, v)
			dest = append(dest, v.Interface())
		}

		if err := result.Scan(dest...); err != nil {
			return nil, err
		}

		values := make([]string, 0, len(held))
		for _, v := range held {
			values = append(values, keyText(v.Elem().Interface()))
		}

		stored[keyText(key.Elem().Interface())] = values
	}

	return stored, result.Err()
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
		if err := mismatch(int64(len(rows)), affected); err != nil {
			return fmt.Errorf("%s %s: %w", op, target, err)
		}
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
	size := effectiveChunkSize(config.size, keyMatchParams(def), sqld.MaxBindParams(scope.dialect))

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

			if soft && t.softDeleted() {
				// A row that is already deleted keeps its original tombstone,
				// unless the table is WithDeleted.
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
