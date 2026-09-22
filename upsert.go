package tsq

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// upsertAlias names the proposed row on MySQL, which has no excluded table.
const upsertAlias = "tsq_new"

// Upsert inserts row, or updates the row that already has the same key. key is
// the primary key when omitted, or the columns of one unique index; on a table with
// an integer deleted_at, a unique index that includes deleted_at is named by its
// other columns and matches live rows only.
//
// It writes the columns Insert would: a generated column never, and a column the
// database defaults only when the row sets it, so an unset one takes the default
// on insert and keeps its value on update. An update writes those columns except
// the key, the primary key and created_at, increments version without checking
// it, and refreshes updated_at. The row written is always live: deleted_at is
// cleared, so an upsert by primary key restores a deleted row. A generated primary
// key, version, created_at and the columns the database filled are read back into
// row.
//
// MySQL matches the proposed row against every unique key, not just key, so there
// an upsert is refused while the table has another unique key the row could hit.
// A zero auto-increment primary key cannot hit anything.
func (t *TableOf[R, K]) Upsert(ctx context.Context, db Executor, row *R, key ...BoundColumn[R]) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpsert), func(ctx context.Context) error {
		return t.upsert(ctx, db, []*R{row}, key, batchConfig{size: 1}, true)
	})
}

// BatchUpsert is Upsert for many rows, in as few statements as the batch size
// allows. It reads nothing back into rows, and two rows with the same key are an
// error, because PostgreSQL refuses to update one row twice in a statement.
func (t *TableOf[R, K]) BatchUpsert(ctx context.Context, db Executor, rows []*R, key []BoundColumn[R], options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpsert), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		return t.upsert(ctx, db, rows, key, config, false)
	})
}

func (t *TableOf[R, K]) upsert(ctx context.Context, db Executor, rows []*R, key []BoundColumn[R], config batchConfig, single bool) error {
	if len(rows) == 0 {
		return nil
	}

	def, scope, err := t.prepareWrite(db, rows)
	if err != nil {
		return err
	}

	target, err := upsertTarget(def, key)
	if err != nil {
		return fmt.Errorf("upsert into %s: %w", def.name, err)
	}

	for _, row := range rows {
		if err := checkFullRow("upsert into", def.name, row); err != nil {
			return err
		}
	}

	now := stampTime()

	for _, row := range rows {
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

	groups := map[string][]*R{}
	order := []string{}

	for _, row := range rows {
		key := columnsKey(t.upsertColumns(def, row))
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}

		groups[key] = append(groups[key], row)
	}

	for _, key := range order {
		group := groups[key]

		cols := t.upsertColumns(def, group[0])
		if len(cols) == 0 {
			return fmt.Errorf("upsert into %s: every column is left to the database", def.name)
		}

		size := effectiveChunkSize(config.size, len(cols), sqld.MaxBindParams(scope.dialect))
		for _, chunk := range chunks(group, size) {
			if err := t.upsertChunk(ctx, db, scope, def, cols, target, chunk, single); err != nil {
				return fmt.Errorf("upsert into %s: %w", def.name, err)
			}
		}
	}

	if single {
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

func (t *TableOf[R, K]) upsertChunk(ctx context.Context, db Executor, scope execScope, def *tableDef, cols []*columnCore, target []string, rows []*R, single bool) error {
	d := scope.dialect
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

		set(col.name)
		proposed(col.name)
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

	if returnKey {
		if id, err := result.LastInsertId(); err == nil && id > 0 {
			setID(field(rows[0], def.primaryKey), id)
		}
	}

	return nil
}
