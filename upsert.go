package tsq

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// upsertAlias names the proposed row on MySQL, which has no excluded table.
const upsertAlias = "tsq_new"

// Conflict says how an upsert matches a row that already exists, and what it
// writes over one: OnConflict names the key, Update the columns. The zero Conflict
// matches by primary key and writes every column.
type Conflict[R any] struct {
	key    []BoundColumn[R]
	update []BoundColumn[R]
}

// OnConflict matches an upsert by key: the primary key, or the columns of one
// unique index. On a table with an integer deleted_at, a unique index that
// includes deleted_at is named by its other columns and matches live rows only.
func OnConflict[R any](key BoundColumn[R], more ...BoundColumn[R]) Conflict[R] {
	return Conflict[R]{key: append([]BoundColumn[R]{key}, more...)}
}

// Update limits what an upsert writes over the row it matched to cols, plus
// updated_at, version and the cleared deleted_at, as Update(ctx, db, row, cols...)
// does. A row that matches nothing is still inserted whole.
func (c Conflict[R]) Update(col BoundColumn[R], more ...BoundColumn[R]) Conflict[R] {
	c.update = append([]BoundColumn[R]{col}, more...)

	return c
}

// Upsert inserts row, or updates the row that already has the same key; conflict
// names the key (the primary key when omitted) and, with Update, the columns an
// update writes.
//
// It inserts the columns Insert would: a generated column never, and a column the
// database defaults only when the row sets it, so an unset one takes the default
// on insert and keeps its value on update. An update writes those columns (or the
// ones Update names) except the key, the primary key and created_at, increments
// version without checking it, and refreshes updated_at. The row written is always
// live: deleted_at is cleared, so an upsert by primary key restores a deleted row.
// The primary key (the stored row's, when it conflicted), version, created_at and
// the columns the database filled are read back into row.
//
// MySQL matches the proposed row against every unique key, not just key, so there
// an upsert is refused while the table has another unique key the row could hit.
// A zero auto-increment primary key cannot hit anything.
func (t *TableOf[R, K]) Upsert(ctx context.Context, db Executor, row *R, conflict ...Conflict[R]) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpsert), func(ctx context.Context) error {
		if len(conflict) > 1 {
			return errors.New("upsert takes one Conflict; name the key and the columns in one tsq.OnConflict(...).Update(...)")
		}

		var c Conflict[R]
		if len(conflict) == 1 {
			c = conflict[0]
		}

		return t.upsert(ctx, db, []*R{row}, c, batchConfig{size: 1}, true)
	})
}

// BatchUpsert is Upsert for many rows, in as few statements as the batch size
// allows; pass the zero Conflict to match by primary key. It reads nothing back
// into rows, and two rows with the same key are an error, because PostgreSQL
// refuses to update one row twice in a statement.
func (t *TableOf[R, K]) BatchUpsert(ctx context.Context, db Executor, rows []*R, conflict Conflict[R], options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpsert), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		return t.upsert(ctx, db, rows, conflict, config, false)
	})
}

func (t *TableOf[R, K]) upsert(ctx context.Context, db Executor, rows []*R, conflict Conflict[R], config batchConfig, single bool) error {
	if len(rows) == 0 {
		return nil
	}

	def, scope, err := t.prepareWrite(db, rows)
	if err != nil {
		return err
	}

	target, err := upsertTarget(def, conflict.key)
	if err != nil {
		return fmt.Errorf("upsert into %s: %w", def.name, err)
	}

	update, err := upsertUpdate(def, target, conflict.update)
	if err != nil {
		return fmt.Errorf("upsert into %s: %w", def.name, err)
	}

	for _, row := range rows {
		if err := checkFullRow("upsert into", def.name, row); err != nil {
			return err
		}
	}

	now := stampTime()
	snapshot := snapshotFields(rows, def.column(def.managed.CreatedAt), def.column(def.managed.UpdatedAt), def.column(def.managed.DeletedAt), def.column(def.managed.Version))
	written := make(map[*R]bool, len(rows))

	if err := t.upsertRows(ctx, db, scope, def, rows, target, update, config, single, now, written); err != nil {
		snapshot.restore(written)
		return err
	}

	// A batch reads nothing back, so what the statement stored for an updated row
	// (its version plus one, its own created_at) is unknown here. Its stamps used
	// to stay on the row anyway, ahead of nothing: the next Update of it failed as
	// a version conflict. The rows keep the values they were passed.
	if !single {
		snapshot.restore(nil)
	}

	return nil
}

// upsertRows writes rows, recording in written the rows each statement stored.
func (t *TableOf[R, K]) upsertRows(ctx context.Context, db Executor, scope execScope, def *tableDef, rows []*R, target, update []string, config batchConfig, single bool, now time.Time, written map[*R]bool) error {
	for _, row := range rows {
		// An inserted row starts at version 1; an updated one keeps the stored
		// version plus one, which the single-row read-back copies in.
		if col := def.column(def.managed.Version); col != nil {
			startVersion(field(row, col))
		}

		if col := def.column(def.managed.CreatedAt); col != nil && isUnset(field(row, col)) {
			if err := applyTimestamp(field(row, col), now); err != nil {
				return fmt.Errorf("table %s: %w", def.name, err)
			}
		}

		if col := def.column(def.managed.UpdatedAt); col != nil {
			if err := applyTimestamp(field(row, col), now); err != nil {
				return fmt.Errorf("table %s: %w", def.name, err)
			}
		}

		if col := def.column(def.managed.DeletedAt); col != nil {
			field(row, col).SetZero()
		}
	}

	// Checked after the managed columns are set, because a target that includes
	// deleted_at compares the values the statement writes, not the ones passed in.
	if err := checkUpsertRows(def, target, rows, scope.dialect); err != nil {
		return fmt.Errorf("upsert into %s: %w", def.name, err)
	}

	var readBack []*columnCore
	if single {
		readBack = t.upsertReadBack(def, rows[0])
	}

	// Where the engine has RETURNING, one row reads its key (the stored row's, when
	// it conflicted) and readBack in the statement itself; otherwise they are read
	// by key afterwards.
	returning := single && scope.dialect.Returning(def.primaryKey.name) != ""

	// Neighbouring rows that write the same columns share a statement, as in
	// insertGroups: grouping every row of one shape first wrote rows out of order.
	// Every group is checked before any is written, so an impossible group no
	// longer fails the call after earlier rows were stored.
	var groups [][]*R

	last := ""

	for i, row := range rows {
		key := columnsKey(t.upsertColumns(def, row))
		if i == 0 || key != last {
			groups = append(groups, nil)
		}

		groups[len(groups)-1] = append(groups[len(groups)-1], row)
		last = key
	}

	for _, group := range groups {
		omitKey := def.autoIncrement && field(group[0], def.primaryKey).IsZero()
		if len(t.upsertColumns(def, group[0])) == 0 && !omitKey {
			return fmt.Errorf("upsert into %s: every column is left to the database", def.name)
		}
	}

	for _, group := range groups {
		cols := t.upsertColumns(def, group[0])

		// A row with only a generated key to write cannot conflict: its key does
		// not exist yet. It is inserted, as Insert inserts it.
		if len(cols) == 0 {
			var back []*columnCore
			if returning {
				back = readBack
			}

			for _, chunk := range chunks(group, effectiveChunkSize(config.size, 1, sqld.MaxBindParams(scope.dialect))) {
				if err := t.insertChunk(ctx, db, scope, def, nil, chunk, true, back); err != nil {
					return fmt.Errorf("upsert into %s: %w", def.name, err)
				}

				for _, row := range chunk {
					written[row] = true
				}
			}

			continue
		}

		size := effectiveChunkSize(config.size, len(cols), sqld.MaxBindParams(scope.dialect))
		for _, chunk := range chunks(group, size) {
			if returning {
				if err := t.upsertReturning(ctx, db, scope, def, cols, target, update, chunk[0], readBack); err != nil {
					return fmt.Errorf("upsert into %s: %w", def.name, err)
				}

				written[chunk[0]] = true

				continue
			}

			if err := t.upsertChunk(ctx, db, scope, def, cols, target, update, chunk, single); err != nil {
				return fmt.Errorf("upsert into %s: %w", def.name, err)
			}

			for _, row := range chunk {
				written[row] = true
			}

			if err := t.adoptStoredKeys(ctx, db, scope, def, target, chunk); err != nil {
				return fmt.Errorf("upsert into %s: %w", def.name, err)
			}
		}
	}

	if single && !returning {
		return t.reloadColumns(ctx, db, scope, def, rows[0], readBack)
	}

	return nil
}

// upsertColumns are the columns an upsert of row writes: those an insert writes,
// and deleted_at, which an upsert always clears even when the column has a
// default.
func (t *TableOf[R, K]) upsertColumns(def *tableDef, row *R) []*columnCore {
	cols := t.insertColumns(def, row)

	tombstone := def.column(def.managed.DeletedAt)
	if tombstone == nil || slices.Contains(cols, tombstone) {
		return cols
	}

	written := make([]*columnCore, 0, len(cols)+1)
	for _, col := range def.columns {
		if col == tombstone || slices.Contains(cols, col) {
			written = append(written, col)
		}
	}

	return written
}

// upsertTarget resolves key to the columns of the primary key or of one unique
// index.
// upsertUpdate resolves the columns Conflict.Update names, or nil when it names
// none. A column the update always writes or never writes is refused: naming it
// would say something the statement does not do.
func upsertUpdate[R any](def *tableDef, target []string, cols []BoundColumn[R]) ([]string, error) {
	if len(cols) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(cols))

	for _, col := range cols {
		if isNilValue(col) {
			return nil, errors.New("upsert update column cannot be nil")
		}

		core := col.core()
		if err := core.err(); err != nil {
			return nil, err
		}

		if isNilValue(core.table) || core.table.definition() != def || core.table.TableName() != def.name || !core.plain {
			return nil, fmt.Errorf("upsert update column %s must be a column of %s", core.name, def.name)
		}

		switch {
		case slices.Contains(target, core.name):
			return nil, fmt.Errorf("upsert update names %s, which is the key it matched by", core.name)
		case core.name == def.primaryKey.name, core.name == def.managed.CreatedAt:
			return nil, fmt.Errorf("upsert update names %s, which an update never writes", core.name)
		case core.name == def.managed.Version, core.name == def.managed.UpdatedAt, core.name == def.managed.DeletedAt:
			return nil, fmt.Errorf("upsert update names %s, which TSQ maintains", core.name)
		case core.fill == tsqdialect.FillGenerated:
			return nil, fmt.Errorf("upsert update names %s, which the database computes", core.name)
		case slices.Contains(names, core.name):
			return nil, fmt.Errorf("upsert update names %s twice", core.name)
		}

		names = append(names, core.name)
	}

	return names, nil
}

func upsertTarget[R any](def *tableDef, key []BoundColumn[R]) ([]string, error) {
	if len(key) == 0 {
		return []string{def.primaryKey.name}, nil
	}

	var names []string

	for _, col := range key {
		if isNilValue(col) {
			return nil, errors.New("upsert key column cannot be nil")
		}

		core := col.core()
		if err := core.err(); err != nil {
			return nil, err
		}

		if isNilValue(core.table) || core.table.definition() != def || core.table.TableName() != def.name || !core.plain {
			return nil, fmt.Errorf("upsert key %s must be a column of %s", core.name, def.name)
		}

		if slices.Contains(names, core.name) {
			return nil, fmt.Errorf("upsert key names %s twice", core.name)
		}

		names = append(names, core.name)
	}

	if len(names) == 1 && names[0] == def.primaryKey.name {
		return names, nil
	}

	for _, index := range def.indexes {
		if !index.Unique {
			continue
		}

		named := slices.DeleteFunc(slices.Clone(index.Columns), func(f string) bool {
			return f == def.managed.DeletedAt && !slices.Contains(names, f)
		})
		if sameColumns(named, names) {
			if len(named) != len(index.Columns) && !def.tombstoneIsZero {
				// NULL never equals NULL, so a nullable tombstone in the index means
				// no live row ever conflicts and every upsert inserts.
				return nil, fmt.Errorf("unique index %s includes a nullable %s, so it never matches; use an integer tombstone", index.Name, def.managed.DeletedAt)
			}

			return index.Columns, nil
		}
	}

	return nil, fmt.Errorf("upsert key (%s) is neither the primary key nor a unique index", strings.Join(names, ", "))
}

// keyText spells v as the database compares it: a pointer by what it points to, a
// Valuer by its value, a named type by its underlying one.
func keyText(v any) string {
	if converted, err := driver.DefaultParameterConverter.ConvertValue(bindValue(v)); err == nil {
		v = converted
	}

	if t, ok := v.(time.Time); ok {
		return t.UTC().Format(time.RFC3339Nano)
	}

	return fmt.Sprintf("%#v", v)
}

func sameColumns(a, b []string) bool {
	return len(a) == len(b) && !slices.ContainsFunc(a, func(s string) bool { return !slices.Contains(b, s) })
}

// checkUpsertRows refuses what would behave differently per dialect: two rows with
// one key, and, on MySQL, a row that could hit a unique key other than target.
func checkUpsertRows[R any](def *tableDef, target []string, rows []*R, d sqld.Dialect) error {
	byPK := len(target) == 1 && target[0] == def.primaryKey.name
	seen := make(map[string]bool, len(rows))

	for _, row := range rows {
		if byPK && def.autoIncrement && field(row, def.primaryKey).IsZero() {
			continue
		}

		parts := make([]string, 0, len(target))
		for _, name := range target {
			parts = append(parts, keyText(value(row, def.column(name))))
		}

		key := strings.Join(parts, "\x00")
		if seen[key] {
			return fmt.Errorf("two rows have the key (%s) = (%s)", strings.Join(target, ", "), strings.Join(parts, ", "))
		}

		seen[key] = true
	}

	if d.Name() != tsqdialect.MySQL {
		return nil
	}

	if !byPK {
		generated := def.autoIncrement && !slices.ContainsFunc(rows, func(row *R) bool {
			return !field(row, def.primaryKey).IsZero()
		})
		if !generated {
			return fmt.Errorf("MySQL would also match the primary key %s; leave it zero or upsert by it", def.primaryKey.name)
		}
	}

	for _, index := range def.indexes {
		if index.Unique && !sameColumns(index.Columns, target) {
			return fmt.Errorf("MySQL would also match unique index %s; upsert by it or drop it", index.Name)
		}
	}

	return nil
}

// upsertReadBack are the columns an upsert of row may leave different from it: the
// version of an updated row, its original created_at, and what the database
// fills. It is taken before the write, while an unset default is still unset.
func (t *TableOf[R, K]) upsertReadBack(def *tableDef, row *R) []*columnCore {
	var cols []*columnCore

	for _, name := range []string{def.managed.Version, def.managed.CreatedAt} {
		if col := def.column(name); col != nil {
			cols = append(cols, col)
		}
	}

	for _, col := range t.databaseFilled(def, row) {
		if col.name != def.managed.DeletedAt && !slices.Contains(cols, col) {
			cols = append(cols, col)
		}
	}

	return cols
}

// upsertStatement writes the INSERT ... ON CONFLICT / ON DUPLICATE KEY of rows,
// without anything returned.
// update names the columns an update writes besides the managed ones; nil is
// every column the statement inserts.
func (t *TableOf[R, K]) upsertStatement(d sqld.Dialect, def *tableDef, cols []*columnCore, target, update []string, rows []*R, single bool) *writeStmt {
	mysql := d.Name() == tsqdialect.MySQL

	w := &writeStmt{d: d}
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

	proposed := func(name string) {
		if mysql {
			w.text(upsertAlias + ".").ident(name)
		} else {
			w.text("excluded.").ident(name)
		}
	}

	if mysql {
		w.text(" AS " + upsertAlias + " ON DUPLICATE KEY UPDATE ")
	} else {
		w.text(" ON CONFLICT (")

		for i, name := range target {
			if i > 0 {
				w.text(", ")
			}

			w.ident(name)
		}

		w.text(") DO UPDATE SET ")
	}

	sets := 0
	set := func(name string) *writeStmt {
		if sets > 0 {
			w.text(", ")
		}

		sets++

		return w.ident(name).text(" = ")
	}

	for _, col := range cols {
		switch col.name {
		case def.primaryKey.name, def.managed.CreatedAt, def.managed.Version:
			continue
		}

		if slices.Contains(target, col.name) {
			continue
		}

		// Update(cols) narrows the update to its columns; updated_at and deleted_at
		// are written either way, as a row Update writes them too.
		if update != nil && !slices.Contains(update, col.name) && col.name != def.managed.UpdatedAt && col.name != def.managed.DeletedAt {
			continue
		}

		set(col.name)
		proposed(col.name)
	}

	// A column Update names that these rows leave out of the INSERT (a default:
	// column holding NULL) is written as NULL, as a row Update(cols) writes it: the
	// caller named it. Without a name it keeps its stored value.
	for _, name := range update {
		if !slices.ContainsFunc(cols, func(c *columnCore) bool { return c.name == name }) {
			set(name).text("NULL")
		}
	}

	// The existing row is named by the table: on MySQL a bare column is ambiguous
	// with the proposed row's alias.
	if v := def.managed.Version; v != "" {
		set(v).ident(def.name).text(".").ident(v).text(" + 1")
	}

	returnKey := single && def.autoIncrement

	switch {
	case mysql && returnKey:
		// LAST_INSERT_ID(expr) makes an update report the existing key too.
		pk := def.primaryKey.name
		set(pk).text("LAST_INSERT_ID(").ident(def.name).text(".").ident(pk).text(")")
	case sets == 0:
		set(target[0])
		proposed(target[0])
	}

	return w
}

// upsertReturning upserts one row and reads back, in the same statement, the key of
// the row it wrote (the stored row's, when it conflicted) and readBack.
func (t *TableOf[R, K]) upsertReturning(ctx context.Context, db Executor, scope execScope, def *tableDef, cols []*columnCore, target, update []string, row *R, readBack []*columnCore) error {
	w := t.upsertStatement(scope.dialect, def, cols, target, update, []*R{row}, true)

	names := []string{def.primaryKey.name}
	dest := []any{def.primaryKey.scan(row)}

	for _, col := range readBack {
		names = append(names, col.name)
		dest = append(dest, col.scan(row))
	}

	w.text(scope.dialect.Returning(names...))

	if w.err != nil {
		return w.err
	}

	logSQLForExecutor(ctx, db, "upsert", w.sql.String(), w.args)

	return db.QueryRowContext(ctx, w.sql.String(), w.args...).Scan(dest...)
}

func (t *TableOf[R, K]) upsertChunk(ctx context.Context, db Executor, scope execScope, def *tableDef, cols []*columnCore, target, update []string, rows []*R, single bool) error {
	mysql := scope.dialect.Name() == tsqdialect.MySQL
	returnKey := single && def.autoIncrement
	w := t.upsertStatement(scope.dialect, def, cols, target, update, rows, single)

	if returnKey && !mysql {
		w.text(" RETURNING ").ident(def.primaryKey.name)
	}

	if w.err != nil {
		return w.err
	}

	logSQLForExecutor(ctx, db, "upsert", w.sql.String(), w.args)

	if returnKey && !mysql {
		var id int64
		if err := db.QueryRowContext(ctx, w.sql.String(), w.args...).Scan(&id); err != nil {
			return err
		}

		setID(field(rows[0], def.primaryKey), id)

		return nil
	}

	result, err := db.ExecContext(ctx, w.sql.String(), w.args...)
	if err != nil {
		return err
	}

	// LAST_INSERT_ID(pk) in the update makes MySQL report the key of an updated row
	// too, so no key means the read-back that follows cannot happen: say so rather
	// than return with the documented columns unread.
	if returnKey {
		id, err := result.LastInsertId()

		switch {
		case err != nil:
			return fmt.Errorf("read the upserted key: %w", err)
		case id <= 0:
			return fmt.Errorf("read the upserted key: the database reported %d", id)
		}

		setID(field(rows[0], def.primaryKey), id)
	}

	return nil
}

// adoptStoredKeys gives rows the primary key of the row each one wrote. By a
// unique key other than the primary key, a row that conflicted updated a stored
// row with another key; a generated key comes back from the statement, but a key
// the caller assigns does not, and the row kept the key it proposed: the read-back
// then found nothing, after the update had happened.
func (t *TableOf[R, K]) adoptStoredKeys(ctx context.Context, db Executor, scope execScope, def *tableDef, target []string, rows []*R) error {
	if def.autoIncrement || (len(target) == 1 && target[0] == def.primaryKey.name) {
		return nil
	}

	cols := make([]*columnCore, 0, len(target))
	for _, name := range target {
		cols = append(cols, def.column(name))
	}

	// An OR per row nests one level deeper each: SQLite refuses a depth of 1000,
	// the default batch size, so a key of several columns is read in parts.
	if len(cols) > 1 && len(rows) > maxOrTerms {
		for _, part := range chunks(rows, maxOrTerms) {
			if err := t.adoptStoredKeys(ctx, db, scope, def, target, part); err != nil {
				return err
			}
		}

		return nil
	}

	w := &writeStmt{d: scope.dialect}
	w.text("SELECT ").ident(def.primaryKey.name)

	for _, col := range cols {
		w.text(", ").ident(col.name)
	}

	w.text(" FROM ").ident(def.name).text(" WHERE ")

	// One key column is matched with a flat IN; a key of several columns row by
	// row, which the depth limits of SQLite allow for a batch.
	if len(cols) == 1 {
		w.ident(cols[0].name).text(" IN (")

		for i, row := range rows {
			if i > 0 {
				w.text(", ")
			}

			w.arg(value(row, cols[0]))
		}

		w.text(")")
	} else {
		for i, row := range rows {
			if i > 0 {
				w.text(" OR ")
			}

			w.text("(")

			for j, col := range cols {
				if j > 0 {
					w.text(" AND ")
				}

				w.ident(col.name).text(" = ").arg(value(row, col))
			}

			w.text(")")
		}
	}

	if w.err != nil {
		return w.err
	}

	logSQLForExecutor(ctx, db, "upsert keys", w.sql.String(), w.args)

	result, err := db.QueryContext(ctx, w.sql.String(), w.args...)
	if err != nil {
		return fmt.Errorf("read the upserted keys: %w", err)
	}

	defer func() { _ = result.Close() }()

	stored := map[string]reflect.Value{}

	for result.Next() {
		holder := new(R)
		dest := []any{def.primaryKey.scan(holder)}

		for _, col := range cols {
			dest = append(dest, col.scan(holder))
		}

		if err := result.Scan(dest...); err != nil {
			return fmt.Errorf("read the upserted keys: %w", err)
		}

		stored[keysOf(holder, cols)] = field(holder, def.primaryKey)
	}

	if err := result.Err(); err != nil {
		return fmt.Errorf("read the upserted keys: %w", err)
	}

	for _, row := range rows {
		if key, ok := stored[keysOf(row, cols)]; ok {
			field(row, def.primaryKey).Set(key)
		}
	}

	return nil
}

// maxOrTerms bounds the rows a statement matches with one OR each.
const maxOrTerms = 200

// keysOf names the values of cols in row.
func keysOf[R any](row *R, cols []*columnCore) string {
	parts := make([]string, 0, len(cols))
	for _, col := range cols {
		parts = append(parts, keyText(value(row, col)))
	}

	return strings.Join(parts, "\x00")
}
