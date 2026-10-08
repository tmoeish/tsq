package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

type tableColumnChange struct {
	kind   string
	before *sqld.Column
	after  *tsqdialect.ColumnSpec
}

const (
	tableColumnAdd   = "add"
	tableColumnDrop  = "drop"
	tableColumnAlter = "alter"
)

func (r *Runtime) applySchemaPolicies(ctx context.Context) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	unlock, err := r.lockSchema(ctx)
	if err != nil {
		return err
	}

	defer unlock()

	// Manual is the default and the recommended production setup, so saying so is a
	// statement of the configured mode, not a warning that something may be wrong.
	// Logging it at warn level put two records in front of every user on every boot.
	// Before any DDL: a unique index the policies would create over rows that
	// share its values is refused here, where nothing has been changed yet.
	if changesSchema(r.indexPolicy) {
		if err := r.refuseUniqueIndexesOverDuplicates(ctx); err != nil {
			return err
		}
	}

	if r.tablePolicy == SchemaPolicyManual {
		r.info("tsq table management is disabled; create and reconcile tables in your migrations", "policy", r.tablePolicy)
	} else {
		if err := r.applyTablePolicy(ctx); err != nil {
			return err
		}
	}

	if r.indexPolicy == SchemaPolicyManual {
		r.info("tsq index management is disabled; create and reconcile indexes in your migrations", "policy", r.indexPolicy)
		return nil
	}

	return r.applyIndexPolicy(ctx)
}

// sqliteSchema lets the runtimes of one process change a SQLite schema one at a
// time: SQLite has no lock a connection can hold across statements. The runtime's
// constructor holds it from its first use of the database.
var sqliteSchema sync.Mutex

// changesSchema reports a policy that runs DDL.
func changesSchema(p SchemaPolicy) bool {
	return p == SchemaPolicyCreateMissing || p == SchemaPolicyReconcile
}

// lockSchema makes this runtime the only one changing the schema until the
// returned function is called. Several instances of a service start together,
// each finds the same table or column missing, and all but the first failed on
// "already exists" (PostgreSQL also on a duplicate key in its own catalog, when
// two CREATE TABLE of one name ran at once): the instance that waits for the lock
// then finds the schema as the first one left it, and has nothing to do.
//
// MySQL and PostgreSQL hold the lock on one connection, and every statement of
// the policies runs on that connection for as long: a pool of one connection has
// no other to give. A policy that changes nothing (Validate) takes no lock.
func (r *Runtime) lockSchema(ctx context.Context) (func(), error) {
	if (!changesSchema(r.tablePolicy) && !changesSchema(r.indexPolicy)) || r.dialect.Name() == tsqdialect.SQLite {
		return func() {}, nil
	}

	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("take the schema lock: %w", err)
	}

	unlock, ok, err := r.dialect.LockSchema(ctx, conn)
	if err != nil || !ok {
		_ = conn.Close()

		if err != nil {
			return nil, fmt.Errorf("take the schema lock: %w", err)
		}

		return func() {}, nil
	}

	r.schema = conn

	return func() {
		r.schema = nil

		// The lock is the session's: a connection that could not release it must
		// not go back to the pool holding it, where it would block every other
		// instance until the pool lets the connection go.
		released, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()

		if err := unlock(released); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}

		_ = conn.Close()
	}, nil
}

// schemaDB is what the schema policies run their statements on: the connection
// that holds the schema lock, or the pool when no lock is held.
func (r *Runtime) schemaDB() sqld.Executor {
	if r.schema != nil {
		return r.schema
	}

	return r.db
}

func (r *Runtime) applyTablePolicy(ctx context.Context) error {
	if r.tablePolicy == SchemaPolicyManual {
		return nil
	}

	// TSQ never removes a table: it creates what is declared and missing, and under
	// Reconcile it alters the columns that drifted and drops the ones no longer
	// declared. A table it no longer sees declared stays, because a runtime only
	// knows its own declarations and cannot tell "no longer declared here" from
	// "declared by someone else".
	for _, table := range r.tables {
		if err := r.applyTablePolicyForTable(ctx, table); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runtime) applyTablePolicyForTable(ctx context.Context, table *registeredTable) error {
	tableName := table.name
	if len(table.Columns) == 0 {
		return fmt.Errorf("table %s does not include runtime schema columns; regenerate TSQ code before using table management", tableName)
	}

	current, found, err := r.dialect.InspectColumns(ctx, r.schemaDB(), tableName)
	if err != nil {
		return fmt.Errorf("inspect table %s: %w", tableName, err)
	}

	if !found {
		if r.tablePolicy == SchemaPolicyValidate {
			return &MissingTableError{Table: tableName}
		}

		statement, err := renderCreateTableStatement(r.dialect, tableName, table.Columns)
		if err != nil {
			return err
		}

		return r.execDDL(ctx, statement)
	}

	changes := diffTableColumns(r.dialect, current, table.Columns)

	var unasked map[string]error

	if hasAlterColumnChange(changes) {
		current, unasked = r.adoptEngineSpelling(ctx, tableName, current, changes)
		changes = diffTableColumns(r.dialect, current, table.Columns)
	}

	if len(changes) == 0 {
		return nil
	}

	switch r.tablePolicy {
	case SchemaPolicyValidate:
		return &SchemaMismatchError{Table: tableName, Changes: tableColumnChangeLines(r.dialect, changes, unasked)}
	case SchemaPolicyCreateMissing:
		// Missing columns are added; a column that differs or is not declared is
		// Reconcile's to change, and nothing is added while one is there.
		added := slices.DeleteFunc(slices.Clone(changes), func(c tableColumnChange) bool { return c.kind != tableColumnAdd })
		if len(added) < len(changes) {
			return &SchemaMismatchError{Table: tableName, Changes: tableColumnChangeLines(r.dialect, changes, unasked)}
		}

		// SQLite cannot ADD every column to a table with rows: such a column is
		// added by rebuilding the table with it, which copies the rows.
		if addNeedsRebuild(r.dialect, added) {
			if err := r.rebuildTable(ctx, tableName, current, table.Columns); err != nil {
				return fmt.Errorf("add columns to %s: %w", tableName, err)
			}

			return nil
		}

		statements, err := renderTableColumnChanges(r.dialect, tableName, added)
		if err != nil {
			return fmt.Errorf("add columns to %s: %w", tableName, err)
		}

		for _, statement := range statements {
			if err := r.execDDL(ctx, statement); err != nil {
				return fmt.Errorf("add column to %s: %w", tableName, err)
			}
		}

	case SchemaPolicyReconcile:
		for _, change := range changes {
			if change.kind == tableColumnDrop {
				r.warn("schema reconcile drops a column and its data",
					"table", tableName, "column", change.before.Name, "policy", r.tablePolicy)
			}
		}

		if (r.dialect.AlterMode() == sqld.AlterRebuild && hasAlterColumnChange(changes)) || addNeedsRebuild(r.dialect, changes) {
			if err := r.rebuildTable(ctx, tableName, current, table.Columns); err != nil {
				return fmt.Errorf("reconcile table %s: %w", tableName, err)
			}

			return nil
		}

		if err := r.dropIndexesOfDroppedColumns(ctx, tableName, changes); err != nil {
			return fmt.Errorf("reconcile table %s: %w", tableName, err)
		}

		if err := r.dropIndexesInTheWay(ctx, table, changes); err != nil {
			return fmt.Errorf("reconcile table %s: %w", tableName, err)
		}

		statements, err := renderTableColumnChanges(r.dialect, tableName, changes)
		if err != nil {
			return fmt.Errorf("reconcile table %s: %w", tableName, err)
		}

		for _, statement := range statements {
			if err := r.execDDL(ctx, statement); err != nil {
				return fmt.Errorf("apply table change on %s: %w", tableName, err)
			}
		}
	}

	return nil
}

// dropIndexesOfDroppedColumns drops, on SQLite, the indexes over a column the
// reconcile drops: SQLite refuses DROP COLUMN on an indexed column, and TSQ never
// drops an undeclared index otherwise, so the start failed. MySQL and PostgreSQL
// drop such indexes with the column themselves.
func (r *Runtime) dropIndexesOfDroppedColumns(ctx context.Context, tableName string, changes []tableColumnChange) error {
	if r.dialect.Name() != tsqdialect.SQLite {
		return nil
	}

	dropped := map[string]bool{}

	for _, change := range changes {
		if change.kind == tableColumnDrop {
			dropped[strings.ToLower(change.before.Name)] = true
		}
	}

	if len(dropped) == 0 {
		return nil
	}

	indexes, err := r.dialect.ListIndexes(ctx, r.schemaDB(), tableName)
	if err != nil {
		return err
	}

	for _, index := range indexes {
		if index.PrimaryKey || index.Constraint {
			continue
		}

		if slices.ContainsFunc(index.Fields, func(field string) bool { return dropped[strings.ToLower(field)] }) {
			if err := r.execDDL(ctx, r.dialect.DropIndexSQL(tableName, index.Name)); err != nil {
				return fmt.Errorf("drop index %s over a dropped column: %w", index.Name, err)
			}
		}
	}

	return nil
}

// dropIndexesInTheWay drops, before the columns are altered, the indexes the
// index policy would drop afterwards anyway (no longer declared, under a name tsq
// gen derives) where they cover an altered column: MySQL refuses to make an
// indexed column a BLOB or TEXT ("used in key specification without a key
// length", 1170), and an index on a column becoming wider than an index key may
// be is refused the same way, so the start failed on an order of statements. A
// declared index, or one under another name, stays: the server's refusal is then
// the right answer.
func (r *Runtime) dropIndexesInTheWay(ctx context.Context, table *registeredTable, changes []tableColumnChange) error {
	if r.indexPolicy != SchemaPolicyReconcile {
		return nil
	}

	altered := map[string]bool{}

	for _, change := range changes {
		if change.kind == tableColumnAlter {
			altered[strings.ToLower(change.before.Name)] = true
		}
	}

	if len(altered) == 0 {
		return nil
	}

	indexes, err := r.dialect.ListIndexes(ctx, r.schemaDB(), table.name)
	if err != nil {
		return err
	}

	var over []sqld.Index

	for _, index := range indexes {
		if slices.ContainsFunc(index.Fields, func(field string) bool { return altered[strings.ToLower(field)] }) {
			over = append(over, index)
		}
	}

	return r.dropUndeclaredIndexes(ctx, table, over)
}

// rebuildTable rewrites a table in place (rename, recreate, copy, drop) for
// dialects that cannot ALTER columns directly. All statements run on a single
// transaction: executing BEGIN/COMMIT as separate pooled Exec calls could land
// on different connections and leave a dangling transaction.
func (r *Runtime) rebuildTable(
	ctx context.Context,
	tableName string,
	current []sqld.Column,
	desired []tsqdialect.ColumnSpec,
) error {
	rebuild, err := r.dialect.InspectRebuild(ctx, r.schemaDB(), tableName)
	if err != nil {
		return err
	}

	// The rebuilt table is created from the declared columns. What it cannot carry
	// over would be lost without a word, so the change is left to a migration.
	if len(rebuild.Blockers) > 0 {
		return fmt.Errorf("this change rebuilds %s on %s (a changed column, or a new one %s cannot add to a table with rows), which would lose %s; change the table in a migration",
			tableName, r.dialect.Name(), r.dialect.Name(), strings.Join(rebuild.Blockers, ", "))
	}

	statements, err := renderRebuildTableStatements(r.dialect, tableName, current, desired, rebuild.Objects)
	if err != nil {
		return err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply table rebuild on %s: %w", tableName, err)
		}
	}

	if err := rebuiltValuesFit(ctx, tx, r.dialect, tableName, current, desired); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("apply table rebuild on %s: %w", tableName, err)
	}

	// Logged once the rebuild committed: a statement that ran in a transaction that
	// was then rolled back was not applied.
	for _, statement := range statements {
		r.info("applied ddl", "table", tableName, "kind", "table_rebuild", "ddl", statement)
	}

	return nil
}

// adoptEngineSpelling settles the columns whose type or default compares different
// as text by asking the engine: each is created as declared in a temporary table,
// and where the engine spells that the way it spells the live column, the two are
// one thing (see sqldialect.AdoptSpelling). Only a column the text comparison
// calls different is probed, so a schema that matches costs nothing. A probe that
// cannot run (no right to create a temporary table, a declaration the engine
// refuses) leaves the text comparison's answer, and unasked carries why, by
// column, so that a mismatch reported for it says the engine was not asked.
func (r *Runtime) adoptEngineSpelling(ctx context.Context, tableName string, current []sqld.Column, changes []tableColumnChange) (settled []sqld.Column, unasked map[string]error) {
	var conn *sql.Conn

	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	settled = slices.Clone(current)

	for _, change := range changes {
		if change.kind != tableColumnAlter || change.before.PrimaryKey || change.after.PrimaryKey || change.after.AutoIncrement {
			continue
		}

		if sqld.SameColumnType(r.dialect, *change.before, *change.after) &&
			sqld.SameDefault(change.before.Default, sqld.DefaultSQL(r.dialect, *change.after)) {
			continue
		}

		// Under the schema lock the policies have a connection already, and a pool
		// of one has no second to give.
		probeOn := r.schema
		if probeOn == nil {
			if conn == nil {
				opened, err := r.db.Conn(ctx)
				if err != nil {
					return current, nil
				}

				conn = opened
			}

			probeOn = conn
		}

		same, supported, err := r.dialect.ProbeColumn(ctx, probeOn, tableName, *change.before, *change.after)
		if !supported {
			return current, nil
		}

		if err != nil {
			r.warn("could not ask the database how it spells a declared column; it is compared as text",
				"table", tableName, "column", change.after.Name, "error", err)

			if unasked == nil {
				unasked = map[string]error{}
			}

			unasked[change.after.Name] = err

			continue
		}

		for i := range settled {
			if settled[i].Name == change.before.Name {
				settled[i] = sqld.AdoptSpelling(r.dialect, settled[i], same, *change.after)
			}
		}
	}

	return settled, unasked
}

// retyped reports a column the rebuild carries into another type.
func retyped(dialect sqld.Dialect, before sqld.Column, after tsqdialect.ColumnSpec) bool {
	return before.Type.Kind != after.Type.Kind || !sqld.SameColumnType(dialect, before, after)
}

// rebuiltValuesFit refuses a rebuild that left, in a column whose type changed, a
// value a field of the new type does not read. SQLite stores anything under any
// declared type, so the copy itself never fails: 'Hello' moved into an integer
// column is still 'Hello', the schema then matched its declaration, and every read
// of the table failed on the row. MySQL and PostgreSQL refuse that change; here it
// is refused before the transaction commits, and the table is as it was.
func rebuiltValuesFit(ctx context.Context, tx *sql.Tx, dialect sqld.Dialect, tableName string, current []sqld.Column, desired []tsqdialect.ColumnSpec) error {
	existing := make(map[string]sqld.Column, len(current))
	for _, column := range current {
		existing[strings.ToLower(column.Name)] = column
	}

	for _, column := range desired {
		before, ok := existing[strings.ToLower(column.Name)]
		if !ok || column.Fill == tsqdialect.FillGenerated || !retyped(dialect, before, column) {
			continue
		}

		quoted := dialect.QuoteIdent(column.Name)

		condition, kind := sqld.SQLiteMisfit(quoted, column.Type)
		if condition == "" {
			continue
		}

		var (
			count  int64
			sample sql.NullString
		)

		query := fmt.Sprintf("SELECT COUNT(*), MIN(CAST(%s AS TEXT)) FROM %s WHERE %s", quoted, dialect.QuoteIdent(tableName), condition)
		if err := tx.QueryRowContext(ctx, query).Scan(&count, &sample); err != nil {
			return fmt.Errorf("check the rebuilt table %s: %w", tableName, err)
		}

		if count > 0 {
			return fmt.Errorf("column %s of %s becomes %s, and %d row(s) hold a value that is not %s (for example %q); "+
				"nothing was changed: fix the rows, or change the column in a migration",
				column.Name, tableName, dialect.ColumnTypeSQL(column.Type), count, kind, sample.String)
		}
	}

	return nil
}

func (r *Runtime) applyIndexPolicy(ctx context.Context) error {
	for _, table := range r.tables {
		if err := r.applyIndexPolicyForTable(ctx, table); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runtime) applyIndexPolicyForTable(ctx context.Context, table *registeredTable) error {
	tableName := table.name
	if _, found, err := r.dialect.InspectColumns(ctx, r.schemaDB(), tableName); err != nil {
		return err
	} else if !found {
		return &MissingTableError{Table: tableName}
	}

	currentIndexes, err := r.dialect.ListIndexes(ctx, r.schemaDB(), tableName)
	if err != nil {
		return fmt.Errorf("list indexes for %s: %w", tableName, err)
	}

	currentByName := make(map[string]sqld.Index, len(currentIndexes))
	for _, idx := range currentIndexes {
		currentByName[idx.Name] = idx
	}

	for _, idx := range table.Indexes {
		if err := validateIndexIdentifiers(tableName, idx.Name, idx.Columns); err != nil {
			return err
		}

		existing, found := currentByName[idx.Name]

		// A full-text index is compared by name where its columns cannot be:
		// PostgreSQL indexes an expression rather than columns and SQLite has none at
		// all, so a field comparison would ask to rebuild it on every boot. MySQL
		// reports the columns, and MATCH needs an index over exactly those it names:
		// one left over other columns failed every search (error 1191).
		if idx.FullText {
			if found && len(existing.Fields) > 0 && !sameIndexColumns(existing.Fields, idx.Columns) {
				if r.indexPolicy != SchemaPolicyReconcile {
					return fmt.Errorf("full-text index %s on table %s covers %v, expected %v", idx.Name, tableName, existing.Fields, idx.Columns)
				}

				if err := r.execDDL(ctx, r.dialect.DropIndexSQL(tableName, idx.Name)); err != nil {
					return fmt.Errorf("recreate full-text index %s on %s: %w", idx.Name, tableName, err)
				}

				found = false
			}

			if err := r.ensureFullTextIndex(ctx, tableName, idx, found); err != nil {
				return err
			}

			continue
		}

		if !found {
			if r.indexPolicy == SchemaPolicyValidate {
				return &MissingIndexError{Table: tableName, IndexSpec: cloneIndexSpec(idx)}
			}

			statement, err := r.dialect.EnsureIndex(ctx, r.schemaDB(), tableName, idx.Name, idx.Columns, idx.Unique)
			if err != nil {
				return fmt.Errorf("create index %s on %s: %w", idx.Name, tableName, err)
			}

			if statement != "" {
				r.info("applied ddl", "table", tableName, "kind", "index_create", "ddl", statement)
			}

			continue
		}

		definition := sqld.Index{
			Table:  existing.Table,
			Unique: existing.Unique,
			Fields: existing.Fields,
		}
		if err := sqld.ValidateIndex(tableName, idx.Unique, idx.Name, idx.Columns, definition); err != nil {
			if r.indexPolicy == SchemaPolicyValidate || r.indexPolicy == SchemaPolicyCreateMissing {
				return err
			}

			if existing.PrimaryKey || existing.Constraint {
				return fmt.Errorf("cannot rebuild index %s on table %s because it is backed by a primary key or constraint", idx.Name, tableName)
			}

			// The new definition is built under another name first: when the rows do
			// not fit it (duplicates against a new unique index), that fails and the
			// old index is still there. Dropping first left the table without the
			// unique index, open to the duplicates it kept out.
			probe := rebuildIndexName(idx.Name)
			if _, err := r.dialect.EnsureIndex(ctx, r.schemaDB(), tableName, probe, idx.Columns, idx.Unique); err != nil {
				return fmt.Errorf("recreate index %s on %s: %w", idx.Name, tableName, err)
			}

			if err := r.execDDL(ctx, r.dialect.DropIndexSQL(tableName, idx.Name)); err != nil {
				return err
			}

			createStatement, err := r.dialect.EnsureIndex(ctx, r.schemaDB(), tableName, idx.Name, idx.Columns, idx.Unique)
			if err != nil {
				return fmt.Errorf("recreate index %s on %s: %w", idx.Name, tableName, err)
			}

			if createStatement != "" {
				r.info("applied ddl", "table", tableName, "kind", "index_create", "ddl", createStatement)
			}

			if err := r.execDDL(ctx, r.dialect.DropIndexSQL(tableName, probe)); err != nil {
				return err
			}
		}
	}

	if r.indexPolicy == SchemaPolicyReconcile {
		return r.dropUndeclaredIndexes(ctx, table, currentIndexes)
	}

	return nil
}

// dropUndeclaredIndexes drops, under Reconcile, the indexes of a declared table
// that TSQ named and the table no longer declares. A //tsq:unique that was removed,
// or widened by a column (which changes its derived name), left the old unique
// index in place, refusing rows the declaration allows, under the one policy where
// the database follows the code and undeclared columns are dropped too.
//
// Only a name tsq gen derives for this table goes: ux_<table>_..., idx_<table>_...
// or ft_<table>_.... An index under any other name may be anyone's, as an index on
// a table this runtime does not declare is, and no policy touches those.
func (r *Runtime) dropUndeclaredIndexes(ctx context.Context, table *registeredTable, current []sqld.Index) error {
	declared := make(map[string]bool, len(table.Indexes))
	for _, idx := range table.Indexes {
		declared[strings.ToLower(idx.Name)] = true
	}

	for _, idx := range current {
		if idx.PrimaryKey || idx.Constraint || declared[strings.ToLower(idx.Name)] || !derivedIndexName(idx.Name, table.name) {
			continue
		}

		r.warn("schema reconcile drops an index the table no longer declares",
			"table", table.name, "index", idx.Name, "policy", r.indexPolicy)

		if err := r.execDDL(ctx, r.dialect.DropIndexSQL(table.name, idx.Name)); err != nil {
			return fmt.Errorf("drop index %s no longer declared on %s: %w", idx.Name, table.name, err)
		}
	}

	return nil
}

// derivedIndexName reports an index name tsq gen derives for table: a prefix for
// the kind of index, the table's name in snake case, then the columns. Case and
// underscores are left out of the comparison, so UserAccount, user_account and
// useraccount are one table name.
func derivedIndexName(index, table string) bool {
	squash := func(name string) string { return strings.ReplaceAll(strings.ToLower(name), "_", "") }

	for _, prefix := range []string{"ux_", "idx_", "ft_"} {
		if rest, ok := strings.CutPrefix(strings.ToLower(index), prefix); ok && strings.HasPrefix(squash(rest), squash(table)) {
			return true
		}
	}

	return false
}

// sameIndexColumns compares two column lists in order, without case.
func sameIndexColumns(left, right []string) bool {
	return slices.EqualFunc(left, right, strings.EqualFold)
}

func (r *Runtime) execDDL(ctx context.Context, statement string) error {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return nil
	}

	if _, err := r.schemaDB().ExecContext(ctx, statement); err != nil {
		return err
	}

	r.info("applied ddl", "ddl", statement)

	return nil
}

func diffTableColumns(
	dialect sqld.Dialect,
	current []sqld.Column,
	desired []tsqdialect.ColumnSpec,
) []tableColumnChange {
	// A generated column that exists is never touched: every dialect reports its
	// expression differently, so comparing would ask for the same change on every
	// boot. It leaves both sides of the comparison, or the live column would look
	// undeclared and be dropped. A changed expression is a migration.
	// SQLite and MySQL match column names without case, so "Name" and "name" are
	// one column there: comparing them exactly read as drop Name, add name, and the
	// drop ran first, with the data. PostgreSQL keeps the case of quoted names.
	key := func(name string) string {
		if dialect.Name() == tsqdialect.Postgres {
			return name
		}

		return strings.ToLower(name)
	}

	present := make(map[string]bool, len(current))
	for _, column := range current {
		present[key(column.Name)] = true
	}

	// A generated column that is there leaves the comparison; one that is declared
	// and missing stays in it, as a column to add. Left out too, a table without
	// it passed Validate and failed at the first read ("no such column").
	generated := map[string]bool{}

	for _, column := range desired {
		if column.Fill == tsqdialect.FillGenerated && present[key(column.Name)] {
			generated[key(column.Name)] = true
		}
	}

	desired = slices.DeleteFunc(slices.Clone(desired), func(c tsqdialect.ColumnSpec) bool { return generated[key(c.Name)] })
	current = slices.DeleteFunc(slices.Clone(current), func(c sqld.Column) bool { return generated[key(c.Name)] })

	currentByName := make(map[string]sqld.Column, len(current))
	for _, column := range current {
		currentByName[key(column.Name)] = column
	}

	desiredByName := make(map[string]tsqdialect.ColumnSpec, len(desired))
	for _, column := range desired {
		desiredByName[key(column.Name)] = column
	}

	changes := make([]tableColumnChange, 0)

	for _, column := range current {
		if _, ok := desiredByName[key(column.Name)]; !ok {
			columnCopy := column
			changes = append(changes, tableColumnChange{kind: tableColumnDrop, before: &columnCopy})
		}
	}

	for _, column := range desired {
		currentColumn, ok := currentByName[key(column.Name)]
		if !ok {
			columnCopy := column
			changes = append(changes, tableColumnChange{kind: tableColumnAdd, after: &columnCopy})

			continue
		}

		if columnsEqual(dialect, currentColumn, column) {
			continue
		}

		beforeCopy := currentColumn
		afterCopy := column
		changes = append(changes, tableColumnChange{
			kind:   tableColumnAlter,
			before: &beforeCopy,
			after:  &afterCopy,
		})
	}

	sort.SliceStable(changes, func(i, j int) bool {
		leftName := ddlColumnChangeName(changes[i])

		rightName := ddlColumnChangeName(changes[j])
		if leftName != rightName {
			return leftName < rightName
		}

		return changes[i].kind < changes[j].kind
	})

	return changes
}

func columnsEqual(dialect sqld.Dialect, left sqld.Column, right tsqdialect.ColumnSpec) bool {
	if !sqld.SameColumnType(dialect, left, right) ||
		left.PrimaryKey != right.PrimaryKey ||
		left.AutoIncrement != right.AutoIncrement ||
		left.Type.Nullable != right.Type.Nullable {
		return false
	}
	// Auto-increment columns carry dialect-managed defaults (e.g. PostgreSQL
	// SERIAL reports nextval('..._seq')) that the declared schema never
	// states. Treating them as drift would make reconcile DROP the sequence
	// default and break inserts, so ignore defaults for these columns.
	if left.PrimaryKey && left.AutoIncrement {
		return true
	}

	// The range constraint that keeps the column to its field (sqldialect.RangeCheck)
	// is part of the column: a table from before it was written gets it by Reconcile.
	if wanted, _ := sqld.RangeCheck(dialect, right); !sqld.SameRangeCheck(right.Name, left.Check, wanted) {
		return false
	}

	// The declared default is compared as the DDL spells it: a current-time
	// default is a UTC expression on MySQL and PostgreSQL, and a live
	// CURRENT_TIMESTAMP there (a table created before TSQ wrote UTC) differs.
	return sqld.SameDefault(left.Default, sqld.DefaultSQL(dialect, right))
}

func ddlColumnChangeName(change tableColumnChange) string {
	switch {
	case change.after != nil:
		return change.after.Name
	case change.before != nil:
		return change.before.Name
	default:
		return ""
	}
}

// tableColumnChangeLines describes the changes for a SchemaMismatchError: what
// to add or drop, and for a column that differs, what differs (the type as the
// database reports it against the declared spelling, NULL against NOT NULL, the
// default, the range constraint), with a note where the engine could not be
// asked whether two spellings are one (unasked, by column).
func tableColumnChangeLines(dialect sqld.Dialect, changes []tableColumnChange, unasked map[string]error) []string {
	lines := make([]string, 0, len(changes))
	for _, change := range changes {
		switch change.kind {
		case tableColumnAdd:
			lines = append(lines, "add column "+change.after.Name)
		case tableColumnDrop:
			lines = append(lines, "drop column "+change.before.Name)
		case tableColumnAlter:
			line := "alter column " + change.after.Name
			if parts := columnDifferences(dialect, *change.before, *change.after); len(parts) > 0 {
				line += " (" + strings.Join(parts, ", ") + ")"
			}

			if err := unasked[change.after.Name]; err != nil {
				line += "; the database could not be asked whether the two spellings are one, so they are compared as text: " + err.Error()
			}

			lines = append(lines, line)
		}
	}

	return lines
}

// columnDifferences lists what differs between a live column and its declaration,
// each as "<what> <live>, declared <wanted>".
func columnDifferences(dialect sqld.Dialect, before sqld.Column, after tsqdialect.ColumnSpec) []string {
	var parts []string

	if !sqld.SameColumnType(dialect, before, after) {
		live := before.NativeType
		if live == "" {
			live = dialect.ColumnTypeSQL(before.Type)
		}

		parts = append(parts, "type "+live+", declared "+dialect.ColumnTypeSQL(after.Type))
	}

	if before.Type.Nullable != after.Type.Nullable {
		parts = append(parts, nullability(before.Type.Nullable)+", declared "+nullability(after.Type.Nullable))
	}

	if before.PrimaryKey != after.PrimaryKey {
		parts = append(parts, fmt.Sprintf("primary key %t, declared %t", before.PrimaryKey, after.PrimaryKey))
	}

	if before.AutoIncrement != after.AutoIncrement {
		parts = append(parts, fmt.Sprintf("auto-increment %t, declared %t", before.AutoIncrement, after.AutoIncrement))
	}

	// An auto-increment key carries the engine's own default (a sequence).
	if wanted := sqld.DefaultSQL(dialect, after); (!before.PrimaryKey || !before.AutoIncrement) && !sqld.SameDefault(before.Default, wanted) {
		parts = append(parts, "default "+orNone(before.Default)+", declared "+orNone(wanted))
	}

	if wanted, _ := sqld.RangeCheck(dialect, after); !sqld.SameRangeCheck(after.Name, before.Check, wanted) {
		parts = append(parts, "range check "+orNone(before.Check)+", declared "+orNone(wanted))
	}

	return parts
}

func nullability(nullable bool) string {
	if nullable {
		return "NULL"
	}

	return "NOT NULL"
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}

	return s
}

// ensureFullTextIndex creates the full-text index when it is missing and the
// dialect has one to create.
func (r *Runtime) ensureFullTextIndex(ctx context.Context, tableName string, idx IndexSpec, found bool) error {
	if found {
		return nil
	}

	quoted := make([]string, 0, len(idx.Columns))
	for _, field := range idx.Columns {
		quoted = append(quoted, r.dialect.QuoteIdent(field))
	}

	statement := r.dialect.FullTextIndexSQL(tableName, idx.Name, quoted)
	if statement == "" {
		return nil
	}

	if r.indexPolicy == SchemaPolicyValidate {
		return &MissingIndexError{Table: tableName, IndexSpec: cloneIndexSpec(idx)}
	}

	if err := r.execDDL(ctx, statement); err != nil {
		return fmt.Errorf("create full-text index %s on %s: %w", idx.Name, tableName, err)
	}

	return nil
}

func renderCreateTableStatement(
	dialect sqld.Dialect,
	tableName string,
	columns []tsqdialect.ColumnSpec,
) (string, error) {
	lines := make([]string, 0, len(columns))
	for _, column := range columns {
		rendered, err := renderRuntimeDDLColumnSpec(dialect, column)
		if err != nil {
			return "", err
		}

		lines = append(lines, "    "+rendered)
	}

	var buf strings.Builder
	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
	buf.WriteString(dialect.QuoteIdent(tableName))
	buf.WriteString(" (\n")
	buf.WriteString(strings.Join(lines, ",\n"))
	buf.WriteString("\n);")

	return buf.String(), nil
}

func renderRuntimeDDLColumnSpec(dialect sqld.Dialect, column tsqdialect.ColumnSpec) (string, error) {
	return sqld.ColumnDefinitionSQL(dialect, column)
}

func renderTableColumnChanges(
	dialect sqld.Dialect,
	tableName string,
	changes []tableColumnChange,
) ([]string, error) {
	statements := make([]string, 0, len(changes))
	for _, change := range changes {
		switch change.kind {
		case tableColumnAdd:
			// The rows present get the zero value in a new NOT NULL column, as the
			// generator's migrations give them.
			rendered, err := sqld.AddColumnSQL(dialect, tableName, *change.after)
			if err != nil {
				return nil, err
			}

			statements = append(statements, rendered...)
		case tableColumnDrop:
			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s DROP COLUMN %s;",
				dialect.QuoteIdent(tableName),
				dialect.QuoteIdent(change.before.Name),
			))
		case tableColumnAlter:
			rendered := dialect.AlterColumnSQL(tableName, *change.before, *change.after)
			if len(rendered) == 0 {
				return nil, fmt.Errorf("manual change required for column %s", change.after.Name)
			}
			statements = append(statements, rendered...)
		}
	}

	return statements, nil
}

// addNeedsRebuild reports an added column the dialect can only add by rebuilding
// the table (SQLite: NOT NULL without a default, or a default that is not a
// constant).
func addNeedsRebuild(dialect sqld.Dialect, changes []tableColumnChange) bool {
	return slices.ContainsFunc(changes, func(change tableColumnChange) bool {
		return change.kind == tableColumnAdd && sqld.AddNeedsRebuild(dialect, *change.after)
	})
}

func hasAlterColumnChange(changes []tableColumnChange) bool {
	for _, change := range changes {
		if change.kind == tableColumnAlter {
			return true
		}
	}

	return false
}

func renderRebuildTableStatements(
	dialect sqld.Dialect,
	tableName string,
	current []sqld.Column,
	desired []tsqdialect.ColumnSpec,
	objects []sqld.RebuildObject,
) ([]string, error) {
	tempTable := "__tsq_rebuild_" + tableName

	createStatement, err := renderCreateTableStatement(dialect, tableName, desired)
	if err != nil {
		return nil, err
	}

	targets, sources := rebuildCopyColumns(dialect, current, desired)
	statements := []string{
		fmt.Sprintf(
			"ALTER TABLE %s RENAME TO %s;",
			dialect.QuoteIdent(tableName),
			dialect.QuoteIdent(tempTable),
		),
		createStatement,
	}

	if len(targets) > 0 {
		statements = append(statements, fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s;",
			dialect.QuoteIdent(tableName),
			strings.Join(targets, ", "),
			strings.Join(sources, ", "),
			dialect.QuoteIdent(tempTable),
		))
	}

	// AUTOINCREMENT promises never to reuse a key, and the counter lives in
	// sqlite_sequence under the table's name: the new table would otherwise count on
	// from its largest copied key, and hand out again the keys of rows deleted after
	// it. The old counter (renamed with the table) is carried over before it goes.
	if slices.ContainsFunc(desired, func(c tsqdialect.ColumnSpec) bool { return c.AutoIncrement }) {
		statements = append(statements,
			fmt.Sprintf("DELETE FROM sqlite_sequence WHERE name = %s;", sqlStringLiteral(tableName)),
			fmt.Sprintf("INSERT INTO sqlite_sequence (name, seq) SELECT %s, seq FROM sqlite_sequence WHERE name = %s;",
				sqlStringLiteral(tableName), sqlStringLiteral(tempTable)),
		)
	}

	statements = append(statements, fmt.Sprintf("DROP TABLE %s;", dialect.QuoteIdent(tempTable)))

	// Dropping the old table also drops its indexes and triggers; create them again
	// as they were, regardless of the index policy in effect.
	statements = append(statements, renderRebuildObjectStatements(desired, objects)...)

	return statements, nil
}

// renderRebuildObjectStatements are the statements that created the table's
// indexes and triggers, run again as they were, so an expression, a partial WHERE
// or a collation survives. An index over a column the rebuild drops goes with the
// column.
func renderRebuildObjectStatements(desired []tsqdialect.ColumnSpec, objects []sqld.RebuildObject) []string {
	// SQLite matches column names without case, so an index on "Code" is over the
	// declared column code, and a case-sensitive check dropped it.
	declared := make(map[string]bool, len(desired))
	for _, column := range desired {
		declared[strings.ToLower(column.Name)] = true
	}

	statements := make([]string, 0, len(objects))

	for _, object := range objects {
		if slices.ContainsFunc(object.Columns, func(column string) bool { return !declared[strings.ToLower(column)] }) {
			continue
		}

		statements = append(statements, object.SQL+";")
	}

	return statements
}

// rebuildCopyColumns lists the columns a rebuild copies and what it copies into
// them. A generated column is computed by the new table (SQLite refuses to insert
// into one), a new NOT NULL column without a default gets its type's zero value,
// and a column that becomes NOT NULL gets its default or zero value where a row
// holds NULL, so the copy does not fail on a table with rows. Names are matched
// without case, as SQLite matches them.
func rebuildCopyColumns(dialect sqld.Dialect, current []sqld.Column, desired []tsqdialect.ColumnSpec) (targets, sources []string) {
	existing := make(map[string]sqld.Column, len(current))
	for _, column := range current {
		existing[strings.ToLower(column.Name)] = column
	}

	for _, column := range desired {
		if column.Fill == tsqdialect.FillGenerated {
			continue
		}

		if before, ok := existing[strings.ToLower(column.Name)]; ok {
			source := dialect.QuoteIdent(before.Name)
			if retyped(dialect, before, column) {
				source = sqld.SQLiteRetypeSource(source, column.Type)
			}

			if fill := sqld.NullFill(dialect, before, column); fill != "" {
				source = fmt.Sprintf("COALESCE(%s, %s)", source, fill)
			}

			targets = append(targets, dialect.QuoteIdent(column.Name))
			sources = append(sources, source)

			continue
		}

		if zero := sqld.NewColumnFill(dialect, column); zero != "" {
			targets = append(targets, dialect.QuoteIdent(column.Name))
			sources = append(sources, zero)
		}
	}

	return targets, sources
}

// sqlStringLiteral quotes s as a SQL string literal.
func sqlStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// rebuildIndexName is the name an index is built under while it replaces another,
// kept within every dialect's 63-character limit.
func rebuildIndexName(name string) string {
	const prefix = "tsq_new_"

	if len(name) > 63-len(prefix) {
		name = name[:63-len(prefix)]
	}

	return prefix + name
}
