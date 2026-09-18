package tsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

type tableColumnChange struct {
	kind   string
	before *tsqdialect.ColumnSpec
	after  *tsqdialect.ColumnSpec
}

const (
	tableColumnAdd   = "add"
	tableColumnDrop  = "drop"
	tableColumnAlter = "alter"
)

func resolveRuntimeDialect(driverName string) (tsqdialect.Dialect, error) {
	switch strings.ToLower(strings.TrimSpace(driverName)) {
	case "sqlite":
		return tsqdialect.SQLiteDialect{}, nil
	case "mysql":
		return tsqdialect.MySQLDialect{}, nil
	case "postgres", "postgresql", "pgx", "pq":
		return tsqdialect.PostgresDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported sql driver %q; expected sqlite, mysql, postgres, pgx, or pq", driverName)
	}
}

func defaultRuntimeLogger() Logger {
	return slog.Default()
}

func (r *Runtime) warn(msg string, args ...any) {
	r.log(context.Background(), slog.LevelWarn, msg, args...)
}

func (r *Runtime) info(msg string, args ...any) {
	r.log(context.Background(), slog.LevelInfo, msg, args...)
}

func (r *Runtime) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if r == nil || r.logger == nil {
		return
	}

	logWith(ctx, r.logger, level, msg, args...)
}

func logWith(ctx context.Context, logger Logger, level slog.Level, msg string, args ...any) {
	if logger == nil || !logger.Enabled(ctx, level) {
		return
	}

	attrs := make([]slog.Attr, 0, len(args)/2)
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}

		attrs = append(attrs, slog.Any(key, args[i+1]))
	}

	logger.LogAttrs(ctx, level, msg, attrs...)
}

// logForExecutor routes execution-time diagnostics to the runtime's configured
// Logger when the executor belongs to one, and to slog.Default() otherwise.
func logForExecutor(ctx context.Context, exec Executor, level slog.Level, msg string, args ...any) {
	if rt := runtimeForExecutor(exec); rt != nil && rt.logger != nil {
		rt.log(ctx, level, msg, args...)
		return
	}

	logWith(ctx, slog.Default(), level, msg, args...)
}

// logSQLForExecutor logs a rendered statement and its bound arguments when the
// executor belongs to a runtime constructed with WithSQLLogging. Executors
// that carry no runtime (a bare *sql.DB, a WrapExecutor result) have no place to
// read the setting from, so they never log.
//
// Both guards run before compactJSON so that marshalling the arguments is only paid
// for when the record is actually going to be emitted.
func logSQLForExecutor(ctx context.Context, exec Executor, operation, sqlText string, args []any) {
	rt := runtimeForExecutor(exec)
	if rt == nil || !rt.logSQL || rt.logger == nil {
		return
	}

	if !rt.logger.Enabled(ctx, slog.LevelDebug) {
		return
	}

	rt.log(ctx, slog.LevelDebug, operation, "sql", sqlText, "args", compactJSON(args))
}

// compactJSON marshals a value to compact JSON, returning an empty string when the
// value cannot be marshalled. It is used for diagnostics only, never for SQL.
func compactJSON(obj any) string {
	bs, err := json.Marshal(obj)
	if err != nil {
		return ""
	}

	return string(bs)
}

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

	// TSQ only ever adds: it creates what is declared and missing, and under
	// Reconcile it alters columns that drifted. It never removes a table it no
	// longer sees declared, because a runtime only knows its own declarations
	// and cannot tell "no longer declared here" from "declared by someone else".
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
	case SchemaPolicyValidate, SchemaPolicyCreateMissing:
		return fmt.Errorf("table %s schema mismatch: %s", tableName, summarizeTableColumnChanges(changes))
	case SchemaPolicyReconcile:
		for _, change := range changes {
			if change.kind == tableColumnDrop {
				r.warn("schema reconcile drops a column and its data",
					"table", tableName, "column", change.before.Name, "policy", r.tablePolicy)
			}
		}

		if r.dialect.AlterMode() == tsqdialect.AlterRebuild && hasAlterColumnChange(changes) {
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
	current []tsqdialect.ColumnSpec,
	desired []tsqdialect.ColumnSpec,
) error {
	existingIndexes, err := r.dialect.ListIndexes(ctx, r.db, tableName)
	if err != nil {
		return fmt.Errorf("list indexes for %s: %w", tableName, err)
	}

	statements, err := renderRebuildTableStatements(r.dialect, tableName, current, desired, existingIndexes)
	if err != nil {
		return err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, statement := range statements {
		r.info("applied ddl", "table", tableName, "kind", "table_rebuild", "ddl", statement)

		if _, err := tx.ExecContext(ctx, statement); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply table rebuild on %s: %w", tableName, err)
		}
	}

	return tx.Commit()
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

	currentByName := make(map[string]tsqdialect.Index, len(currentIndexes))
	for _, idx := range currentIndexes {
		currentByName[idx.Name] = idx
	}

	desiredByName := make(map[string]TableIndex, len(table.Indexes))
	for _, idx := range table.Indexes {
		desiredByName[idx.Name] = idx

		if err := validateIndexIdentifiers(tableName, idx.Name, idx.Fields); err != nil {
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
					Table:  tableName,
					Name:   idx.Name,
					Fields: append([]string(nil), idx.Fields...),
					Unique: idx.Unique,
				}
			}

			statement, err := r.dialect.EnsureIndex(ctx, r.db, tableName, idx.Name, idx.Fields, idx.Unique)
			if err != nil {
				return fmt.Errorf("create index %s on %s: %w", idx.Name, tableName, err)
			}

			if statement != "" {
				r.info("applied ddl", "table", tableName, "kind", "index_create", "ddl", statement)
			}

			continue
		}

		definition := tsqdialect.Index{
			Table:  existing.Table,
			Unique: existing.Unique,
			Fields: existing.Fields,
		}
		if err := validateIndex(tableName, idx.Unique, idx.Name, idx.Fields, definition); err != nil {
			if r.indexPolicy == SchemaPolicyValidate || r.indexPolicy == SchemaPolicyCreateMissing {
				return err
			}

			if existing.PrimaryKey || existing.Constraint {
				return fmt.Errorf("cannot rebuild index %s on table %s because it is backed by a primary key or constraint", idx.Name, tableName)
			}

			dropStatement := r.dialect.DropIndexSQL(tableName, idx.Name)
			if err := r.execDDL(ctx, dropStatement); err != nil {
				return err
			}

			createStatement, err := r.dialect.EnsureIndex(ctx, r.db, tableName, idx.Name, idx.Fields, idx.Unique)
			if err != nil {
				return fmt.Errorf("recreate index %s on %s: %w", idx.Name, tableName, err)
			}

			if createStatement != "" {
				r.info("applied ddl", "table", tableName, "kind", "index_create", "ddl", createStatement)
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

	r.info("applied ddl", "ddl", statement)

	if _, err := r.db.ExecContext(ctx, statement); err != nil {
		return err
	}

	return nil
}

func diffTableColumns(
	dialect tsqdialect.Dialect,
	current []tsqdialect.ColumnSpec,
	desired []tsqdialect.ColumnSpec,
) []tableColumnChange {
	// A generated column is created with the table and never touched afterwards:
	// every dialect reports it differently (SQLite's table_info omits it entirely),
	// so comparing would ask for the same change on every boot. It leaves both sides
	// of the comparison, or the live column would look undeclared and be dropped.
	// Adding one to a table that exists is a migration.
	generated := map[string]bool{}

	for _, column := range desired {
		if column.Fill == tsqdialect.FillGenerated {
			generated[column.Name] = true
		}
	}

	skipGenerated := func(c tsqdialect.ColumnSpec) bool { return generated[c.Name] }
	desired = slices.DeleteFunc(slices.Clone(desired), skipGenerated)
	current = slices.DeleteFunc(slices.Clone(current), skipGenerated)

	currentByName := make(map[string]tsqdialect.ColumnSpec, len(current))
	for _, column := range current {
		currentByName[column.Name] = column
	}

	desiredByName := make(map[string]tsqdialect.ColumnSpec, len(desired))
	for _, column := range desired {
		desiredByName[column.Name] = column
	}

	changes := make([]tableColumnChange, 0)

	for _, column := range current {
		if _, ok := desiredByName[column.Name]; !ok {
			columnCopy := column
			changes = append(changes, tableColumnChange{kind: tableColumnDrop, before: &columnCopy})
		}
	}

	for _, column := range desired {
		currentColumn, ok := currentByName[column.Name]
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

func columnsEqual(dialect tsqdialect.Dialect, left, right tsqdialect.ColumnSpec) bool {
	if !tsqdialect.SameColumnType(dialect, left, right) ||
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

	return normalizeDefaultLiteral(left.Default) == normalizeDefaultLiteral(right.Default)
}

// normalizeDefaultLiteral makes two spellings of the same default comparable: a
// declared 'USD' reads back as USD on MySQL and as 'USD'::character varying on
// PostgreSQL, and comparing those verbatim asks to set the default on every boot.
func normalizeDefaultLiteral(value string) string {
	value = strings.TrimSpace(value)

	if cast := strings.Index(value, "::"); cast >= 0 {
		value = strings.TrimSpace(value[:cast])
	}

	if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		value = value[1 : len(value)-1]
	}

	return strings.ToLower(value)
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

	quoted := make([]string, 0, len(idx.Fields))
	for _, field := range idx.Fields {
		quoted = append(quoted, r.dialect.QuoteIdent(field))
	}

	statement := r.dialect.FullTextIndexSQL(tableName, idx.Name, quoted)
	if statement == "" {
		return nil
	}

	if r.indexPolicy == SchemaPolicyValidate {
		return &MissingIndexError{Table: tableName, Name: idx.Name, Fields: append([]string(nil), idx.Fields...)}
	}

	if err := r.execDDL(ctx, statement); err != nil {
		return fmt.Errorf("create full-text index %s on %s: %w", idx.Name, tableName, err)
	}

	return nil
}

func renderCreateTableStatement(
	dialect tsqdialect.Dialect,
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

func renderRuntimeDDLColumnSpec(dialect tsqdialect.Dialect, column tsqdialect.ColumnSpec) (string, error) {
	return tsqdialect.ColumnDefinitionSQL(dialect, column)
}

func renderTableColumnChanges(
	dialect tsqdialect.Dialect,
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
	dialect tsqdialect.Dialect,
	tableName string,
	current []tsqdialect.ColumnSpec,
	desired []tsqdialect.ColumnSpec,
	existingIndexes []tsqdialect.Index,
) ([]string, error) {
	tempTable := "__tsq_rebuild_" + tableName

	createStatement, err := renderCreateTableStatement(dialect, tableName, desired)
	if err != nil {
		return nil, err
	}

	shared := sharedColumnNames(current, desired)
	statements := []string{
		fmt.Sprintf(
			"ALTER TABLE %s RENAME TO %s;",
			dialect.QuoteIdent(tableName),
			dialect.QuoteIdent(tempTable),
		),
		createStatement,
	}

	if len(shared) > 0 {
		quotedColumns := make([]string, 0, len(shared))
		for _, name := range shared {
			quotedColumns = append(quotedColumns, dialect.QuoteIdent(name))
		}

		statements = append(statements, fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s;",
			dialect.QuoteIdent(tableName),
			strings.Join(quotedColumns, ", "),
			strings.Join(quotedColumns, ", "),
			dialect.QuoteIdent(tempTable),
		))
	}

	statements = append(statements, fmt.Sprintf("DROP TABLE %s;", dialect.QuoteIdent(tempTable)))

	// Dropping the old table also drops its indexes; restore every secondary
	// index that still applies, regardless of the index policy in effect.
	statements = append(statements, renderRebuildIndexStatements(dialect, tableName, desired, existingIndexes)...)

	return statements, nil
}

func renderRebuildIndexStatements(
	dialect tsqdialect.Dialect,
	tableName string,
	desired []tsqdialect.ColumnSpec,
	existingIndexes []tsqdialect.Index,
) []string {
	desiredNames := make(map[string]struct{}, len(desired))
	for _, column := range desired {
		desiredNames[column.Name] = struct{}{}
	}

	statements := make([]string, 0, len(existingIndexes))

	for _, idx := range existingIndexes {
		// Primary-key and constraint-backed indexes are created with the table
		// itself and cannot be recreated as standalone indexes.
		if idx.PrimaryKey || idx.Constraint || len(idx.Fields) == 0 {
			continue
		}

		quotedFields := make([]string, 0, len(idx.Fields))
		applicable := true

		for _, field := range idx.Fields {
			if _, ok := desiredNames[field]; !ok {
				applicable = false
				break
			}

			quotedFields = append(quotedFields, dialect.QuoteIdent(field))
		}

		if !applicable {
			continue
		}

		statements = append(statements, dialect.CreateIndexSQL(tableName, idx.Name, quotedFields, idx.Unique))
	}

	return statements
}

func sharedColumnNames(current, desired []tsqdialect.ColumnSpec) []string {
	currentByName := make(map[string]struct{}, len(current))
	for _, column := range current {
		currentByName[column.Name] = struct{}{}
	}

	shared := make([]string, 0, len(desired))
	for _, column := range desired {
		if _, ok := currentByName[column.Name]; ok {
			shared = append(shared, column.Name)
		}
	}

	return shared
}
