package tsq

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sort"
	"strings"

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

	// Manual is the default and the recommended production setup, so saying so is a
	// statement of the configured mode, not a warning that something may be wrong.
	// Logging it at warn level put two records in front of every user on every boot.
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

	current, found, err := r.dialect.InspectColumns(ctx, r.db, tableName)
	if err != nil {
		return fmt.Errorf("inspect table %s: %w", tableName, err)
	}

	if !found {
		if r.tablePolicy == SchemaPolicyValidate {
			return &MissingTableError{Name: tableName}
		}

		statement, err := renderCreateTableStatement(r.dialect, tableName, table.Columns)
		if err != nil {
			return err
		}

		return r.execDDL(ctx, statement)
	}

	changes := diffTableColumns(r.dialect, current, table.Columns)
	if len(changes) == 0 {
		return nil
	}

	switch r.tablePolicy {
	case SchemaPolicyValidate:
		return fmt.Errorf("table %s schema mismatch: %s", tableName, summarizeTableColumnChanges(changes))
	case SchemaPolicyCreateMissing:
		// Missing columns are added; a column that differs or is not declared is
		// Reconcile's to change, and nothing is added while one is there.
		added := slices.DeleteFunc(slices.Clone(changes), func(c tableColumnChange) bool { return c.kind != tableColumnAdd })
		if len(added) < len(changes) {
			return fmt.Errorf("table %s schema mismatch: %s", tableName, summarizeTableColumnChanges(changes))
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

		if r.dialect.AlterMode() == sqld.AlterRebuild && hasAlterColumnChange(changes) {
			if err := r.rebuildTable(ctx, tableName, current, table.Columns); err != nil {
				return fmt.Errorf("reconcile table %s: %w", tableName, err)
			}

			return nil
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
	rebuild, err := r.dialect.InspectRebuild(ctx, r.db, tableName)
	if err != nil {
		return err
	}

	// The rebuilt table is created from the declared columns. What it cannot carry
	// over would be lost without a word, so the change is left to a migration.
	if len(rebuild.Blockers) > 0 {
		return fmt.Errorf("changing a column type rebuilds %s on %s, which would lose %s; change the table in a migration",
			tableName, r.dialect.Name(), strings.Join(rebuild.Blockers, ", "))
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
	if _, found, err := r.dialect.InspectColumns(ctx, r.db, tableName); err != nil {
		return err
	} else if !found {
		return &MissingTableError{Name: tableName}
	}

	currentIndexes, err := r.dialect.ListIndexes(ctx, r.db, tableName)
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

		// A full-text index is compared by name only: PostgreSQL indexes an
		// expression rather than columns, MySQL reports a different index type, and
		// SQLite has none at all, so a field comparison would ask to rebuild it on
		// every boot.
		if idx.FullText {
			if err := r.ensureFullTextIndex(ctx, tableName, idx, found); err != nil {
				return err
			}

			continue
		}

		if !found {
			if r.indexPolicy == SchemaPolicyValidate {
				return &MissingIndexError{
					Table:   tableName,
					Name:    idx.Name,
					Columns: append([]string(nil), idx.Columns...),
					Unique:  idx.Unique,
				}
			}

			statement, err := r.dialect.EnsureIndex(ctx, r.db, tableName, idx.Name, idx.Columns, idx.Unique)
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
			if _, err := r.dialect.EnsureIndex(ctx, r.db, tableName, probe, idx.Columns, idx.Unique); err != nil {
				return fmt.Errorf("recreate index %s on %s: %w", idx.Name, tableName, err)
			}

			if err := r.execDDL(ctx, r.dialect.DropIndexSQL(tableName, idx.Name)); err != nil {
				return err
			}

			createStatement, err := r.dialect.EnsureIndex(ctx, r.db, tableName, idx.Name, idx.Columns, idx.Unique)
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

	return nil
}

func (r *Runtime) execDDL(ctx context.Context, statement string) error {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return nil
	}

	if _, err := r.db.ExecContext(ctx, statement); err != nil {
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
	// A generated column is created with the table and never touched afterwards:
	// every dialect reports it differently (SQLite's table_info omits it entirely),
	// so comparing would ask for the same change on every boot. It leaves both sides
	// of the comparison, or the live column would look undeclared and be dropped.
	// Adding one to a table that exists is a migration.
	// SQLite and MySQL match column names without case, so "Name" and "name" are
	// one column there: comparing them exactly read as drop Name, add name, and the
	// drop ran first, with the data. PostgreSQL keeps the case of quoted names.
	key := func(name string) string {
		if dialect.Name() == tsqdialect.Postgres {
			return name
		}

		return strings.ToLower(name)
	}

	generated := map[string]bool{}

	for _, column := range desired {
		if column.Fill == tsqdialect.FillGenerated {
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

	return sameDefault(left.Default, right.Default)
}

// sameDefault compares two spellings of a column default. Two quoted literals
// compare exactly, so 'Active' and 'active' differ; anything else compares without
// case, since keywords (CURRENT_TIMESTAMP) are spelled either way and MySQL reads a
// string default back without its quotes.
//
// Numbers compare by value and booleans as 1 and 0: MySQL reads a declared true
// back as 1 and a decimal 0 as 0.00, which used to ask for the same ALTER on every
// boot.
func sameDefault(left, right string) bool {
	a, aQuoted := normalizeDefaultLiteral(left)
	b, bQuoted := normalizeDefaultLiteral(right)

	if aQuoted && bQuoted {
		return a == b
	}

	if x, ok := defaultNumber(a); ok {
		if y, ok := defaultNumber(b); ok {
			return x.Cmp(y) == 0
		}
	}

	return strings.EqualFold(a, b)
}

// defaultNumber reads a default as an exact number, true and false included.
func defaultNumber(value string) (*big.Rat, bool) {
	switch strings.ToLower(value) {
	case "true":
		value = "1"
	case "false":
		value = "0"
	}

	return new(big.Rat).SetString(value)
}

// normalizeDefaultLiteral makes two spellings of the same default comparable: a
// declared 'USD' reads back as USD on MySQL and as 'USD'::character varying on
// PostgreSQL, and comparing those verbatim asks to set the default on every boot.
// It drops a cast outside the literal (a '::' inside one is text), unquotes a
// literal and reports whether it was one.
func normalizeDefaultLiteral(value string) (string, bool) {
	value = strings.TrimSpace(value)

	quoted := false

	for i := 0; i < len(value); i++ {
		switch {
		case value[i] == '\'':
			quoted = !quoted
		case !quoted && strings.HasPrefix(value[i:], "::"):
			value = strings.TrimSpace(value[:i])
			i = len(value)
		}
	}

	if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return strings.ReplaceAll(value[1:len(value)-1], "''", "'"), true
	}

	return value, false
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

func summarizeTableColumnChanges(changes []tableColumnChange) string {
	lines := make([]string, 0, len(changes))
	for _, change := range changes {
		switch change.kind {
		case tableColumnAdd:
			lines = append(lines, "add column "+change.after.Name)
		case tableColumnDrop:
			lines = append(lines, "drop column "+change.before.Name)
		case tableColumnAlter:
			lines = append(lines, "alter column "+change.after.Name)
		}
	}

	return strings.Join(lines, ", ")
}

// ensureFullTextIndex creates the full-text index when it is missing and the
// dialect has one to create.
func (r *Runtime) ensureFullTextIndex(ctx context.Context, tableName string, idx TableIndex, found bool) error {
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
		return &MissingIndexError{Table: tableName, Name: idx.Name, Columns: append([]string(nil), idx.Columns...)}
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
			rendered, err := renderRuntimeDDLColumnSpec(dialect, *change.after)
			if err != nil {
				return nil, err
			}

			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN %s;",
				dialect.QuoteIdent(tableName),
				rendered,
			))
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
	declared := make(map[string]bool, len(desired))
	for _, column := range desired {
		declared[column.Name] = true
	}

	statements := make([]string, 0, len(objects))

	for _, object := range objects {
		if slices.ContainsFunc(object.Columns, func(column string) bool { return !declared[column] }) {
			continue
		}

		statements = append(statements, object.SQL+";")
	}

	return statements
}

// rebuildCopyColumns lists the columns a rebuild copies and what it copies into
// them. A generated column is computed by the new table (SQLite refuses to insert
// into one), and a new NOT NULL column without a default gets its type's zero
// value, so the copy does not fail on a table with rows. Names are matched without
// case, as SQLite matches them.
func rebuildCopyColumns(dialect sqld.Dialect, current []sqld.Column, desired []tsqdialect.ColumnSpec) (targets, sources []string) {
	existing := make(map[string]string, len(current))
	for _, column := range current {
		existing[strings.ToLower(column.Name)] = column.Name
	}

	for _, column := range desired {
		if column.Fill == tsqdialect.FillGenerated {
			continue
		}

		if name, ok := existing[strings.ToLower(column.Name)]; ok {
			targets = append(targets, dialect.QuoteIdent(column.Name))
			sources = append(sources, dialect.QuoteIdent(name))

			continue
		}

		if column.Type.Nullable || column.Default != "" || column.AutoIncrement {
			continue
		}

		if zero, ok := zeroLiteral(column.Type); ok {
			targets = append(targets, dialect.QuoteIdent(column.Name))
			sources = append(sources, zero)
		}
	}

	return targets, sources
}

// zeroLiteral is the Go zero value of a column type as a SQL literal, for rows a
// new NOT NULL column is added to. A column of an explicit type: has none TSQ knows.
func zeroLiteral(t tsqdialect.ColumnType) (string, bool) {
	if t.RawType != "" {
		return "", false
	}

	switch t.Kind {
	case tsqdialect.KindString:
		return "''", true
	case tsqdialect.KindInt, tsqdialect.KindBool, tsqdialect.KindFloat:
		return "0", true
	case tsqdialect.KindBytes:
		return "X''", true
	case tsqdialect.KindTime:
		return "'0001-01-01 00:00:00+00:00'", true
	}

	return "", false
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
