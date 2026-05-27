package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

const managedTablesRegistryName = "_tsq_managed_tables"

type tableColumnChange struct {
	kind   string
	before *tsqdialect.DDLColumnSpec
	after  *tsqdialect.DDLColumnSpec
}

const (
	tableColumnAdd   = "add"
	tableColumnDrop  = "drop"
	tableColumnAlter = "alter"
)

func openRuntimeDB(driverName, dsn string) (*sql.DB, tsqdialect.Dialect, error) {
	dialect, err := resolveRuntimeDialect(driverName)
	if err != nil {
		return nil, nil, err
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, nil, err
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, nil, err
	}

	return db, dialect, nil
}

func resolveRuntimeDialect(driverName string) (tsqdialect.Dialect, error) {
	switch strings.ToLower(strings.TrimSpace(driverName)) {
	case "sqlite", "sqlite3":
		return tsqdialect.SQLiteDialect{}, nil
	case "mysql":
		return tsqdialect.MySQLDialect{}, nil
	case "postgres", "postgresql", "pgx", "pq":
		return tsqdialect.PostgresDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported sql driver %q; expected sqlite3, mysql, postgres, pgx, or pq", driverName)
	}
}

func resolveRuntimeLogger(options *RuntimeOptions) Logger {
	if options != nil && options.Logger != nil {
		return options.Logger
	}

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

	if !r.logger.Enabled(ctx, level) {
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

	r.logger.LogAttrs(ctx, level, msg, attrs...)
}

func (r *Runtime) applySchemaPolicies(ctx context.Context) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	if r.tablePolicy == SchemaPolicyManual {
		r.warn("tsq table management is disabled; create and reconcile tables in your migrations", "policy", r.tablePolicy)
	} else {
		if err := r.applyTablePolicy(ctx); err != nil {
			return err
		}
	}

	if r.indexPolicy == SchemaPolicyManual {
		r.warn("tsq index management is disabled; create and reconcile indexes in your migrations", "policy", r.indexPolicy)
		return nil
	}

	return r.applyIndexPolicy(ctx)
}

func (r *Runtime) applyTablePolicy(ctx context.Context) error {
	if r.tablePolicy == SchemaPolicyManual {
		return nil
	}

	registry, err := r.loadManagedTableRegistry(ctx)
	if err != nil {
		return err
	}

	desiredNames := make([]string, 0, len(r.tables))
	for _, table := range r.tables {
		if err := r.applyTablePolicyForTable(ctx, table); err != nil {
			return err
		}
		desiredNames = append(desiredNames, physicalTableName(table.Table))
	}

	if r.tablePolicy == SchemaPolicyManaged {
		for _, tableName := range registry {
			if containsString(desiredNames, tableName) {
				continue
			}

			_, found, err := r.dialect.InspectTableColumns(ctx, r.db, tableName)
			if err != nil {
				return err
			}

			if !found {
				continue
			}

			statement := fmt.Sprintf("DROP TABLE %s;", r.dialect.QuoteField(tableName))
			if err := r.execDDL(ctx, statement); err != nil {
				return fmt.Errorf("drop managed table %s: %w", tableName, err)
			}
		}
	}

	if err := r.saveManagedTableRegistry(ctx, desiredNames); err != nil {
		return err
	}

	return nil
}

func (r *Runtime) applyTablePolicyForTable(ctx context.Context, table *registeredTable) error {
	tableName := physicalTableName(table.Table)
	if len(table.Columns) == 0 {
		return fmt.Errorf("table %s does not include runtime schema columns; regenerate TSQ code before using table management", tableName)
	}

	current, found, err := r.dialect.InspectTableColumns(ctx, r.db, tableName)
	if err != nil {
		return fmt.Errorf("inspect table %s: %w", tableName, err)
	}

	if !found {
		if r.tablePolicy == SchemaPolicyValidate {
			return &ErrTableMissing{Name: tableName}
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
	case SchemaPolicyReconcile, SchemaPolicyManaged:
		statements, err := renderTableColumnChanges(r.dialect, tableName, current, table.Columns, changes)
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

func (r *Runtime) applyIndexPolicy(ctx context.Context) error {
	for _, table := range r.tables {
		if err := r.applyIndexPolicyForTable(ctx, table); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runtime) applyIndexPolicyForTable(ctx context.Context, table *registeredTable) error {
	tableName := physicalTableName(table.Table)
	if _, found, err := r.dialect.InspectTableColumns(ctx, r.db, tableName); err != nil {
		return err
	} else if !found {
		return &ErrTableMissing{Name: tableName}
	}

	currentIndexes, err := r.dialect.ListIndexes(ctx, r.db, tableName)
	if err != nil {
		return fmt.Errorf("list indexes for %s: %w", tableName, err)
	}

	currentByName := make(map[string]tsqdialect.NamedIndexDefinition, len(currentIndexes))
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
		if !found {
			if r.indexPolicy == SchemaPolicyValidate {
				return &ErrIndexMissing{
					Table:  tableName,
					Name:   idx.Name,
					Fields: append([]string(nil), idx.Fields...),
					Unique: idx.Unique,
				}
			}

			statement, err := r.dialect.EnsureIndex(ctx, r.db, tableName, idx.Unique, idx.Name, idx.Fields)
			if err != nil {
				return fmt.Errorf("create index %s on %s: %w", idx.Name, tableName, err)
			}

			if statement != "" {
				r.info("applied ddl", "table", tableName, "kind", "index_create", "ddl", statement)
			}

			continue
		}

		definition := tsqdialect.IndexDefinition{
			Table:  existing.Table,
			Unique: existing.Unique,
			Fields: existing.Fields,
		}
		if err := validateIndexDefinition(tableName, idx.Unique, idx.Name, idx.Fields, definition); err != nil {
			if r.indexPolicy == SchemaPolicyValidate || r.indexPolicy == SchemaPolicyCreateMissing {
				return err
			}

			if existing.PrimaryKey || existing.Constraint {
				return fmt.Errorf("cannot rebuild index %s on table %s because it is backed by a primary key or constraint", idx.Name, tableName)
			}

			dropStatement := r.dialect.DDLDropIndex(tableName, idx.Name)
			if err := r.execDDL(ctx, dropStatement); err != nil {
				return err
			}

			createStatement, err := r.dialect.EnsureIndex(ctx, r.db, tableName, idx.Unique, idx.Name, idx.Fields)
			if err != nil {
				return fmt.Errorf("recreate index %s on %s: %w", idx.Name, tableName, err)
			}

			if createStatement != "" {
				r.info("applied ddl", "table", tableName, "kind", "index_create", "ddl", createStatement)
			}
		}
	}

	if r.indexPolicy == SchemaPolicyManaged {
		for _, idx := range currentIndexes {
			if _, ok := desiredByName[idx.Name]; ok || idx.PrimaryKey || idx.Constraint {
				continue
			}

			statement := r.dialect.DDLDropIndex(tableName, idx.Name)
			if err := r.execDDL(ctx, statement); err != nil {
				return fmt.Errorf("drop unmanaged index %s on %s: %w", idx.Name, tableName, err)
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

func (r *Runtime) loadManagedTableRegistry(ctx context.Context) ([]string, error) {
	if err := r.ensureManagedTableRegistry(ctx); err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s",
		r.dialect.QuoteField("table_name"),
		r.dialect.QuoteField(managedTablesRegistryName),
		r.dialect.QuoteField("table_name"),
	)

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	var names []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return names, nil
}

func (r *Runtime) saveManagedTableRegistry(ctx context.Context, names []string) error {
	if err := r.ensureManagedTableRegistry(ctx); err != nil {
		return err
	}

	deleteStatement := fmt.Sprintf("DELETE FROM %s", r.dialect.QuoteField(managedTablesRegistryName))
	if _, err := r.db.ExecContext(ctx, deleteStatement); err != nil {
		return err
	}

	sort.Strings(names)

	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		insertStatement := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			r.dialect.QuoteField(managedTablesRegistryName),
			r.dialect.QuoteField("table_name"),
			r.dialect.BindVar(0),
		)
		if _, err := r.db.ExecContext(ctx, insertStatement, name); err != nil {
			return err
		}
	}

	return nil
}

func (r *Runtime) ensureManagedTableRegistry(ctx context.Context) error {
	statement, err := renderCreateTableStatement(r.dialect, managedTablesRegistryName, []tsqdialect.DDLColumnSpec{{
		Name: "table_name",
		Type: tsqdialect.DDLColumnType{
			Kind: tsqdialect.DDLColumnKindString,
			Size: 255,
		},
		PrimaryKey: true,
	}})
	if err != nil {
		return err
	}

	_, err = r.db.ExecContext(ctx, statement)

	return err
}

func diffTableColumns(
	dialect tsqdialect.Dialect,
	current []tsqdialect.DDLColumnSpec,
	desired []tsqdialect.DDLColumnSpec,
) []tableColumnChange {
	currentByName := make(map[string]tsqdialect.DDLColumnSpec, len(current))
	for _, column := range current {
		currentByName[column.Name] = column
	}

	desiredByName := make(map[string]tsqdialect.DDLColumnSpec, len(desired))
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

func columnsEqual(dialect tsqdialect.Dialect, left, right tsqdialect.DDLColumnSpec) bool {
	return strings.EqualFold(normalizedDDLColumnType(dialect, left.Type), normalizedDDLColumnType(dialect, right.Type)) &&
		left.PrimaryKey == right.PrimaryKey &&
		left.AutoIncrement == right.AutoIncrement &&
		left.Type.Nullable == right.Type.Nullable &&
		strings.EqualFold(strings.TrimSpace(left.Default), strings.TrimSpace(right.Default))
}

func normalizedDDLColumnType(dialect tsqdialect.Dialect, desc tsqdialect.DDLColumnType) string {
	return strings.ToUpper(strings.TrimSpace(dialect.DDLColumnType(desc)))
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

func renderCreateTableStatement(
	dialect tsqdialect.Dialect,
	tableName string,
	columns []tsqdialect.DDLColumnSpec,
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
	buf.WriteString("CREATE TABLE ")

	if clause := dialect.CreateTableIfNotExistsSuffix(); clause != "" {
		buf.WriteString(clause)
		buf.WriteByte(' ')
	}

	buf.WriteString(dialect.QuoteField(tableName))
	buf.WriteString(" (\n")
	buf.WriteString(strings.Join(lines, ",\n"))
	buf.WriteString("\n)")
	buf.WriteString(dialect.CreateTableSuffix())

	return buf.String(), nil
}

func renderRuntimeDDLColumnSpec(dialect tsqdialect.Dialect, column tsqdialect.DDLColumnSpec) (string, error) {
	quotedColumn := dialect.QuoteField(column.Name)
	if column.PrimaryKey && column.AutoIncrement {
		return dialect.DDLAutoIncrementPrimaryKey(quotedColumn, column.Type)
	}

	parts := []string{quotedColumn, dialect.DDLColumnType(column.Type)}
	if column.PrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	} else if !column.Type.Nullable {
		parts = append(parts, "NOT NULL")
	}

	if column.Default != "" {
		parts = append(parts, "DEFAULT "+column.Default)
	}

	return strings.Join(parts, " "), nil
}

func renderTableColumnChanges(
	dialect tsqdialect.Dialect,
	tableName string,
	current []tsqdialect.DDLColumnSpec,
	desired []tsqdialect.DDLColumnSpec,
	changes []tableColumnChange,
) ([]string, error) {
	if dialect.DDLAlterColumnMode() == tsqdialect.DDLAlterColumnRebuild && hasAlterColumnChange(changes) {
		return renderRebuildTableStatements(dialect, tableName, current, desired)
	}

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
				dialect.QuoteField(tableName),
				rendered,
			))
		case tableColumnDrop:
			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s DROP COLUMN %s;",
				dialect.QuoteField(tableName),
				dialect.QuoteField(change.before.Name),
			))
		case tableColumnAlter:
			rendered := dialect.DDLAlterColumnStatements(tableName, *change.before, *change.after)
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
	current []tsqdialect.DDLColumnSpec,
	desired []tsqdialect.DDLColumnSpec,
) ([]string, error) {
	tempTable := "__tsq_rebuild_" + tableName

	createStatement, err := renderCreateTableStatement(dialect, tableName, desired)
	if err != nil {
		return nil, err
	}

	shared := sharedColumnNames(current, desired)
	statements := []string{
		"BEGIN TRANSACTION;",
		fmt.Sprintf(
			"ALTER TABLE %s RENAME TO %s;",
			dialect.QuoteField(tableName),
			dialect.QuoteField(tempTable),
		),
		createStatement,
	}

	if len(shared) > 0 {
		quotedColumns := make([]string, 0, len(shared))
		for _, name := range shared {
			quotedColumns = append(quotedColumns, dialect.QuoteField(name))
		}

		statements = append(statements, fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s;",
			dialect.QuoteField(tableName),
			strings.Join(quotedColumns, ", "),
			strings.Join(quotedColumns, ", "),
			dialect.QuoteField(tempTable),
		))
	}

	statements = append(statements,
		fmt.Sprintf("DROP TABLE %s;", dialect.QuoteField(tempTable)),
		"COMMIT;",
	)

	return statements, nil
}

func sharedColumnNames(current, desired []tsqdialect.DDLColumnSpec) []string {
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

func containsString(items []string, target string) bool {
	return slices.Contains(items, target)
}
