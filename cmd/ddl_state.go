package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/juju/errors"

	"github.com/tmoeish/tsq"
)

const (
	ddlStateFilename = "ddl.json"
)

type ddlStateFile struct {
	GeneratedBy string           `json:"generated_by"`
	Version     string           `json:"version"`
	Snapshot    ddlSnapshot      `json:"snapshot"`
	Records     []ddlStateRecord `json:"records,omitempty"`
}

type ddlStateRecord struct {
	Sequence string                         `json:"sequence"`
	Tables   []ddlStateRecordTable          `json:"tables"`
	Dialects map[string]ddlStateDialectDiff `json:"dialects"`
}

type ddlStateRecordTable struct {
	Table   string   `json:"table"`
	Columns []string `json:"columns,omitempty"`
	Indexes []string `json:"indexes,omitempty"`
}

type ddlStateDialectDiff struct {
	AggregateSQL string `json:"aggregate_sql"`
}

type ddlSnapshot struct {
	Tables []ddlSnapshotTable `json:"tables"`
}

type ddlSnapshotTable struct {
	Name    string              `json:"name"`
	Columns []ddlSnapshotColumn `json:"columns"`
	Indexes []ddlSnapshotIndex  `json:"indexes,omitempty"`
}

type ddlSnapshotColumn struct {
	Name          string        `json:"name"`
	Kind          ddlColumnKind `json:"kind"`
	Bits          int           `json:"bits,omitempty"`
	Unsigned      bool          `json:"unsigned,omitempty"`
	Nullable      bool          `json:"nullable,omitempty"`
	Size          int           `json:"size,omitempty"`
	PrimaryKey    bool          `json:"primary_key,omitempty"`
	AutoIncrement bool          `json:"auto_increment,omitempty"`
	Default       string        `json:"default,omitempty"`
}

type ddlSnapshotIndex struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Unique bool     `json:"unique"`
}

type ddlChangeSet struct {
	Tables  []string
	ByTable map[string][]ddlChange
}

type ddlChange struct {
	kind      string
	table     string
	oldTable  *ddlSnapshotTable
	newTable  *ddlSnapshotTable
	oldColumn *ddlSnapshotColumn
	newColumn *ddlSnapshotColumn
	oldIndex  *ddlSnapshotIndex
	newIndex  *ddlSnapshotIndex
}

const (
	ddlChangeCreateTable = "create_table"
	ddlChangeDropTable   = "drop_table"
	ddlChangeAddColumn   = "add_column"
	ddlChangeDropColumn  = "drop_column"
	ddlChangeAlterColumn = "alter_column"
	ddlChangeAddIndex    = "add_index"
	ddlChangeDropIndex   = "drop_index"
)

func buildCurrentDDLSnapshot(tables []*tsq.StructInfo, resolver *ddlTypeResolver) (ddlSnapshot, error) {
	snapshot := ddlSnapshot{Tables: make([]ddlSnapshotTable, 0, len(tables))}

	for _, table := range tables {
		item, err := buildCurrentDDLTableSnapshot(table, resolver)
		if err != nil {
			return ddlSnapshot{}, errors.Trace(err)
		}

		snapshot.Tables = append(snapshot.Tables, item)
	}

	sort.Slice(snapshot.Tables, func(i, j int) bool {
		return snapshot.Tables[i].Name < snapshot.Tables[j].Name
	})

	return snapshot, nil
}

func buildCurrentDDLTableSnapshot(
	table *tsq.StructInfo,
	resolver *ddlTypeResolver,
) (ddlSnapshotTable, error) {
	result := ddlSnapshotTable{
		Name:    table.Table,
		Columns: make([]ddlSnapshotColumn, 0, len(table.Fields)),
		Indexes: make([]ddlSnapshotIndex, 0, len(table.UxList)+len(table.IdxList)),
	}

	for _, field := range orderedDDLFields(table) {
		desc, err := resolver.describeField(table, field)
		if err != nil {
			return ddlSnapshotTable{}, errors.Annotatef(err, "failed to describe %s.%s", table.TypeInfo.TypeName, field.Name)
		}

		result.Columns = append(result.Columns, ddlSnapshotColumn{
			Name:          field.Column,
			Kind:          desc.kind,
			Bits:          desc.bits,
			Unsigned:      desc.unsigned,
			Nullable:      desc.nullable,
			Size:          desc.size,
			PrimaryKey:    field.Name == table.ID,
			AutoIncrement: field.Name == table.ID && table.AI,
			Default:       ddlManagedDefaultClause(table, field, desc),
		})
	}

	appendIndexes := func(items []tsq.IndexInfo, unique bool) error {
		for _, idx := range items {
			fields := make([]string, 0, len(idx.Fields))
			for _, fieldName := range idx.Fields {
				field, ok := table.FieldMap[fieldName]
				if !ok {
					return errors.Errorf("index %s references unknown field %s", idx.Name, fieldName)
				}

				fields = append(fields, field.Column)
			}

			result.Indexes = append(result.Indexes, ddlSnapshotIndex{
				Name:   idx.Name,
				Fields: fields,
				Unique: unique,
			})
		}

		return nil
	}

	if err := appendIndexes(table.UxList, true); err != nil {
		return ddlSnapshotTable{}, err
	}

	if err := appendIndexes(table.IdxList, false); err != nil {
		return ddlSnapshotTable{}, err
	}

	sort.Slice(result.Indexes, func(i, j int) bool {
		return result.Indexes[i].Name < result.Indexes[j].Name
	})

	return result, nil
}

func loadDDLStateFile(outDir string) (*ddlStateFile, error) {
	filename := filepath.Join(outDir, ddlStateFilename)

	content, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	if !isGeneratedDDLArtifact(content) {
		return nil, errors.Errorf("refusing to read non-generated DDL state file: %s", filename)
	}

	var state ddlStateFile
	if err := json.Unmarshal(content, &state); err != nil {
		return nil, errors.Annotatef(err, "failed to parse DDL state file: %s", filename)
	}

	return &state, nil
}

func marshalDDLStateFile(
	version string,
	previous *ddlStateFile,
	current ddlSnapshot,
	recordTables []ddlStateRecordTable,
	dialects map[string]ddlStateDialectDiff,
) ([]byte, error) {
	state := ddlStateFile{
		GeneratedBy: "tsq-" + version,
		Version:     version,
		Snapshot:    current,
	}

	if previous != nil {
		state.Records = append(state.Records, previous.Records...)
	}

	if previous != nil && len(recordTables) > 0 {
		record := ddlStateRecord{
			Sequence: time.Now().Format(time.DateTime),
			Tables:   append([]ddlStateRecordTable(nil), recordTables...),
			Dialects: dialects,
		}

		state.Records = append(state.Records, record)
	}

	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(content, '\n'), nil
}

func diffDDLSnapshots(previous *ddlSnapshot, current ddlSnapshot) ddlChangeSet {
	result := ddlChangeSet{
		ByTable: make(map[string][]ddlChange),
	}

	currentByName := make(map[string]ddlSnapshotTable, len(current.Tables))
	for _, table := range current.Tables {
		currentByName[table.Name] = table
	}

	if previous == nil {
		for _, table := range current.Tables {
			tableCopy := table
			result.ByTable[table.Name] = []ddlChange{{
				kind:     ddlChangeCreateTable,
				table:    table.Name,
				newTable: &tableCopy,
			}}
			result.Tables = append(result.Tables, table.Name)
		}

		sort.Strings(result.Tables)

		return result
	}

	previousByName := make(map[string]ddlSnapshotTable, len(previous.Tables))
	for _, table := range previous.Tables {
		previousByName[table.Name] = table
	}

	allNames := make([]string, 0, len(previousByName)+len(currentByName))

	seen := make(map[string]struct{}, len(previousByName)+len(currentByName))
	for name := range previousByName {
		seen[name] = struct{}{}
		allNames = append(allNames, name)
	}

	for name := range currentByName {
		if _, ok := seen[name]; ok {
			continue
		}
		allNames = append(allNames, name)
	}

	sort.Strings(allNames)

	for _, tableName := range allNames {
		before, hadBefore := previousByName[tableName]
		after, hasAfter := currentByName[tableName]

		switch {
		case !hadBefore && hasAfter:
			afterCopy := after
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:     ddlChangeCreateTable,
				table:    tableName,
				newTable: &afterCopy,
			})
		case hadBefore && !hasAfter:
			beforeCopy := before
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:     ddlChangeDropTable,
				table:    tableName,
				oldTable: &beforeCopy,
			})
		default:
			diffExistingDDLTable(&result, before, after)
		}

		if len(result.ByTable[tableName]) == 0 {
			delete(result.ByTable, tableName)
			continue
		}

		result.Tables = append(result.Tables, tableName)
	}

	sort.Strings(result.Tables)

	return result
}

func diffExistingDDLTable(result *ddlChangeSet, before, after ddlSnapshotTable) {
	tableName := after.Name
	beforeTableCopy := before
	afterTableCopy := after

	beforeColumns := make(map[string]ddlSnapshotColumn, len(before.Columns))
	for _, column := range before.Columns {
		beforeColumns[column.Name] = column
	}

	afterColumns := make(map[string]ddlSnapshotColumn, len(after.Columns))
	for _, column := range after.Columns {
		afterColumns[column.Name] = column
	}

	for _, column := range before.Columns {
		if _, ok := afterColumns[column.Name]; ok {
			continue
		}

		columnCopy := column
		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:      ddlChangeDropColumn,
			table:     tableName,
			oldTable:  &beforeTableCopy,
			newTable:  &afterTableCopy,
			oldColumn: &columnCopy,
		})
	}

	for _, column := range after.Columns {
		beforeColumn, ok := beforeColumns[column.Name]
		if !ok {
			columnCopy := column
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:      ddlChangeAddColumn,
				table:     tableName,
				oldTable:  &beforeTableCopy,
				newTable:  &afterTableCopy,
				newColumn: &columnCopy,
			})

			continue
		}

		if reflect.DeepEqual(beforeColumn, column) {
			continue
		}

		beforeCopy := beforeColumn
		afterCopy := column
		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:      ddlChangeAlterColumn,
			table:     tableName,
			oldTable:  &beforeTableCopy,
			newTable:  &afterTableCopy,
			oldColumn: &beforeCopy,
			newColumn: &afterCopy,
		})
	}

	beforeIndexes := make(map[string]ddlSnapshotIndex, len(before.Indexes))
	for _, idx := range before.Indexes {
		beforeIndexes[idx.Name] = idx
	}

	afterIndexes := make(map[string]ddlSnapshotIndex, len(after.Indexes))
	for _, idx := range after.Indexes {
		afterIndexes[idx.Name] = idx
	}

	for _, idx := range before.Indexes {
		next, ok := afterIndexes[idx.Name]
		if !ok {
			idxCopy := idx
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:     ddlChangeDropIndex,
				table:    tableName,
				oldTable: &beforeTableCopy,
				newTable: &afterTableCopy,
				oldIndex: &idxCopy,
			})

			continue
		}

		if reflect.DeepEqual(idx, next) {
			continue
		}

		oldCopy := idx
		newCopy := next

		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:     ddlChangeDropIndex,
			table:    tableName,
			oldTable: &beforeTableCopy,
			newTable: &afterTableCopy,
			oldIndex: &oldCopy,
		})
		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:     ddlChangeAddIndex,
			table:    tableName,
			oldTable: &beforeTableCopy,
			newTable: &afterTableCopy,
			newIndex: &newCopy,
		})
	}

	for _, idx := range after.Indexes {
		if _, ok := beforeIndexes[idx.Name]; ok {
			continue
		}

		idxCopy := idx
		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:     ddlChangeAddIndex,
			table:    tableName,
			oldTable: &beforeTableCopy,
			newTable: &afterTableCopy,
			newIndex: &idxCopy,
		})
	}
}

func buildDDLRecordTables(changes ddlChangeSet) []ddlStateRecordTable {
	result := make([]ddlStateRecordTable, 0, len(changes.Tables))

	for _, tableName := range changes.Tables {
		ops := append([]ddlChange(nil), changes.ByTable[tableName]...)
		sort.SliceStable(ops, func(i, j int) bool {
			return compareDDLChanges(ops[i], ops[j]) < 0
		})

		item := ddlStateRecordTable{Table: tableName}

		for _, op := range ops {
			if op.kind == ddlChangeCreateTable {
				item.Columns = append(item.Columns, "create table")
				continue
			}

			line, isIndex := classifyDDLRecordLine(op)
			if line == "" {
				continue
			}

			if isIndex {
				item.Indexes = append(item.Indexes, line)
			} else {
				item.Columns = append(item.Columns, line)
			}
		}

		if len(item.Columns) == 0 && len(item.Indexes) == 0 {
			continue
		}

		result = append(result, item)
	}

	return result
}

func compareDDLChanges(left, right ddlChange) int {
	if diff := strings.Compare(left.table, right.table); diff != 0 {
		return diff
	}

	if diff := ddlChangeCategoryRank(left) - ddlChangeCategoryRank(right); diff != 0 {
		return diff
	}

	if diff := ddlChangeActionRank(left) - ddlChangeActionRank(right); diff != 0 {
		return diff
	}

	return strings.Compare(ddlChangeObjectName(left), ddlChangeObjectName(right))
}

func ddlChangeCategoryRank(change ddlChange) int {
	switch change.kind {
	case ddlChangeCreateTable, ddlChangeDropTable:
		return 0
	case ddlChangeAddColumn, ddlChangeAlterColumn, ddlChangeDropColumn:
		return 1
	case ddlChangeAddIndex, ddlChangeDropIndex:
		if ddlChangeIndexUnique(change) {
			return 2
		}

		return 3
	default:
		return 4
	}
}

func ddlChangeActionRank(change ddlChange) int {
	switch change.kind {
	case ddlChangeCreateTable, ddlChangeAddColumn, ddlChangeAddIndex:
		return 0
	case ddlChangeAlterColumn:
		return 1
	case ddlChangeDropColumn, ddlChangeDropIndex, ddlChangeDropTable:
		return 2
	default:
		return 3
	}
}

func ddlChangeObjectName(change ddlChange) string {
	switch change.kind {
	case ddlChangeCreateTable:
		return change.newTable.Name
	case ddlChangeDropTable:
		return change.oldTable.Name
	case ddlChangeAddColumn, ddlChangeAlterColumn:
		return change.newColumn.Name
	case ddlChangeDropColumn:
		return change.oldColumn.Name
	case ddlChangeAddIndex:
		return change.newIndex.Name
	case ddlChangeDropIndex:
		return change.oldIndex.Name
	default:
		return ""
	}
}

func ddlChangeIndexUnique(change ddlChange) bool {
	switch change.kind {
	case ddlChangeAddIndex:
		return change.newIndex != nil && change.newIndex.Unique
	case ddlChangeDropIndex:
		return change.oldIndex != nil && change.oldIndex.Unique
	default:
		return false
	}
}

func classifyDDLRecordLine(change ddlChange) (string, bool) {
	switch change.kind {
	case ddlChangeCreateTable:
		return "create table", false
	case ddlChangeDropTable:
		return "drop table", false
	case ddlChangeAddColumn:
		return "add column " + change.newColumn.Name, false
	case ddlChangeDropColumn:
		return "drop column " + change.oldColumn.Name, false
	case ddlChangeAlterColumn:
		return formatDDLAlterColumnSummary(*change.oldColumn, *change.newColumn), false
	case ddlChangeAddIndex:
		if change.newIndex.Unique {
			return "add unique index " + change.newIndex.Name, true
		}

		return "add index " + change.newIndex.Name, true
	case ddlChangeDropIndex:
		if change.oldIndex.Unique {
			return "drop unique index " + change.oldIndex.Name, true
		}

		return "drop index " + change.oldIndex.Name, true
	default:
		return "", false
	}
}

func formatDDLAlterColumnSummary(before, after ddlSnapshotColumn) string {
	details := make([]string, 0, 3)
	if ddlColumnTypeChanged(before, after) {
		details = append(details, "type")
	}

	if before.Nullable != after.Nullable {
		details = append(details, "nullability")
	}

	if before.Default != after.Default {
		details = append(details, "default")
	}

	line := "alter column " + after.Name
	if len(details) == 0 {
		return line
	}

	return line + " (" + strings.Join(details, ", ") + ")"
}

func renderDDLSnapshotAggregateFile(version string, snapshot ddlSnapshot, dialect ddlDialectSpec) []byte {
	var buf strings.Builder
	buf.WriteString(renderDDLHeader(version))
	buf.WriteString("-- Dialect: ")
	buf.WriteString(dialect.name)
	buf.WriteString("\n")

	for i, table := range snapshot.Tables {
		buf.WriteString("\n-- Table: ")
		buf.WriteString(table.Name)
		buf.WriteString("\n\n")
		buf.WriteString(renderDDLSnapshotTableBlock(table, dialect))
		buf.WriteByte('\n')

		if i < len(snapshot.Tables)-1 {
			buf.WriteByte('\n')
		}
	}

	return []byte(buf.String())
}

func renderDDLSnapshotTableBlock(table ddlSnapshotTable, dialect ddlDialectSpec) string {
	var buf strings.Builder
	buf.WriteString(renderDDLSnapshotCreateTable(table, dialect))

	indexStatements := renderDDLSnapshotIndexStatements(table, dialect)
	for _, stmt := range indexStatements {
		buf.WriteString("\n\n")
		buf.WriteString(stmt)
	}

	return buf.String()
}

func renderDDLSnapshotCreateTable(table ddlSnapshotTable, dialect ddlDialectSpec) string {
	lines := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		lines = append(lines, "    "+renderDDLSnapshotColumnDefinition(column, dialect))
	}

	var buf strings.Builder
	buf.WriteString("CREATE TABLE ")

	if clause := dialect.dialect.CreateTableIfNotExistsSuffix(); clause != "" {
		buf.WriteString(clause)
		buf.WriteByte(' ')
	}

	buf.WriteString(dialect.dialect.QuoteField(table.Name))
	buf.WriteString(" (\n")
	buf.WriteString(strings.Join(lines, ",\n"))
	buf.WriteString("\n)")
	buf.WriteString(dialect.dialect.CreateTableSuffix())

	return buf.String()
}

func renderDDLSnapshotColumnDefinition(column ddlSnapshotColumn, dialect ddlDialectSpec) string {
	quotedColumn := dialect.dialect.QuoteField(column.Name)
	if column.PrimaryKey && column.AutoIncrement {
		switch dialect.name {
		case ddlDialectSQLite:
			return quotedColumn + " INTEGER PRIMARY KEY " + dialect.dialect.AutoIncrementClause()
		case ddlDialectMySQL:
			return strings.Join([]string{
				quotedColumn,
				renderDDLColumnType(columnDescriptorFromSnapshot(column), dialect),
				"PRIMARY KEY",
				dialect.dialect.AutoIncrementClause(),
			}, " ")
		case ddlDialectPostgres:
			return quotedColumn + " " + renderDDLSerialType(columnDescriptorFromSnapshot(column))
		}
	}

	parts := []string{quotedColumn, renderDDLColumnType(columnDescriptorFromSnapshot(column), dialect)}
	if column.PrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	} else if !column.Nullable {
		parts = append(parts, "NOT NULL")
	}

	if column.Default != "" {
		parts = append(parts, "DEFAULT "+column.Default)
	}

	return strings.Join(parts, " ")
}

func renderDDLSnapshotIndexStatements(table ddlSnapshotTable, dialect ddlDialectSpec) []string {
	statements := make([]string, 0, len(table.Indexes))
	for _, idx := range table.Indexes {
		statements = append(statements, renderDDLIndexCreateStatement(table.Name, idx, dialect))
	}

	return statements
}

func renderDDLIndexCreateStatement(tableName string, idx ddlSnapshotIndex, dialect ddlDialectSpec) string {
	quotedFields := make([]string, 0, len(idx.Fields))
	for _, field := range idx.Fields {
		quotedFields = append(quotedFields, dialect.dialect.QuoteField(field))
	}

	quotedTable := dialect.dialect.QuoteField(tableName)
	quotedIndex := dialect.dialect.QuoteField(idx.Name)

	uniqueClause := ""
	if idx.Unique {
		uniqueClause = "UNIQUE "
	}

	if dialect.name == ddlDialectMySQL {
		return fmt.Sprintf(
			"ALTER TABLE %s ADD %sINDEX %s(%s)%s",
			quotedTable,
			uniqueClause,
			quotedIndex,
			strings.Join(quotedFields, ", "),
			dialect.dialect.CreateIndexSuffix(),
		)
	}

	return fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)%s",
		uniqueClause,
		quotedIndex,
		quotedTable,
		strings.Join(quotedFields, ", "),
		dialect.dialect.CreateIndexSuffix(),
	)
}

func renderDDLIncrementalArtifacts(
	version string,
	dialect ddlDialectSpec,
	changes ddlChangeSet,
) ([]ddlFileModel, ddlStateDialectDiff, error) {
	result := ddlStateDialectDiff{}
	models := make([]ddlFileModel, 0, 1)

	aggregateBody := renderDDLIncrementalAggregateBody(dialect, changes)
	aggregateSource := renderDDLIncrementalHeader(version, dialect.name, "") + aggregateBody + "\n"
	models = append(models, ddlFileModel{
		Filename: dialectIncrementalFilename(dialect.name),
		Source:   []byte(aggregateSource),
	})
	result.AggregateSQL = aggregateBody

	return models, result, nil
}

func renderDDLIncrementalHeader(version, dialectName, tableName string) string {
	var buf strings.Builder
	buf.WriteString(renderDDLHeader(version))
	buf.WriteString("-- Incremental DDL")

	if dialectName != "" {
		buf.WriteString(" for ")
		buf.WriteString(dialectName)
	}

	if tableName != "" {
		buf.WriteString(" / ")
		buf.WriteString(tableName)
	}

	buf.WriteString("\n\n")

	return buf.String()
}

func renderDDLIncrementalAggregateBody(dialect ddlDialectSpec, changes ddlChangeSet) string {
	if len(changes.Tables) == 0 {
		return "-- No schema changes."
	}

	var sections []string

	for _, tableName := range changes.Tables {
		body, ok := renderDDLIncrementalTableBody(dialect, tableName, changes.ByTable[tableName])
		if !ok {
			continue
		}

		sections = append(sections, "-- Table: "+tableName+"\n\n"+body)
	}

	if len(sections) == 0 {
		return "-- No schema changes."
	}

	return strings.Join(sections, "\n\n")
}

func renderDDLIncrementalTableBody(
	dialect ddlDialectSpec,
	tableName string,
	ops []ddlChange,
) (string, bool) {
	if len(ops) == 0 {
		return "", false
	}

	if dialect.name == ddlDialectSQLite && ddlChangesRequireSQLiteRebuild(ops) {
		body, ok := renderSQLiteRebuildTableBody(dialect, tableName, ops)
		if ok {
			return body, true
		}
	}

	lines := make([]string, 0, len(ops))
	for _, op := range ops {
		rendered := renderDDLChangeOperation(dialect, op)
		if len(rendered) == 0 {
			continue
		}
		lines = append(lines, rendered...)
	}

	if len(lines) == 0 {
		return "-- No schema changes for table: " + tableName, true
	}

	return strings.Join(lines, "\n\n"), true
}

func ddlChangesRequireSQLiteRebuild(ops []ddlChange) bool {
	for _, op := range ops {
		if op.kind == ddlChangeAlterColumn {
			return true
		}
	}

	return false
}

func renderSQLiteRebuildTableBody(dialect ddlDialectSpec, tableName string, ops []ddlChange) (string, bool) {
	var before *ddlSnapshotTable
	var after *ddlSnapshotTable

	for _, op := range ops {
		if before == nil && op.oldTable != nil {
			before = op.oldTable
		}

		if after == nil && op.newTable != nil {
			after = op.newTable
		}
	}

	if before == nil || after == nil {
		return renderDDLManualComment(tableName, "manual change required to rebuild table for sqlite"), true
	}

	tempTable := "__tsq_rebuild_" + tableName
	statements := []string{
		"BEGIN TRANSACTION;",
		fmt.Sprintf(
			"ALTER TABLE %s RENAME TO %s;",
			dialect.dialect.QuoteField(tableName),
			dialect.dialect.QuoteField(tempTable),
		),
		renderDDLSnapshotCreateTable(*after, dialect) + ";",
	}

	commonColumns := sharedDDLSnapshotColumns(*before, *after)
	if len(commonColumns) > 0 {
		quotedColumns := quoteDDLColumns(commonColumns, dialect)
		statements = append(statements, fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s;",
			dialect.dialect.QuoteField(tableName),
			strings.Join(quotedColumns, ", "),
			strings.Join(quotedColumns, ", "),
			dialect.dialect.QuoteField(tempTable),
		))
	}

	statements = append(statements, fmt.Sprintf("DROP TABLE %s;", dialect.dialect.QuoteField(tempTable)))
	statements = append(statements, renderDDLSnapshotIndexStatements(*after, dialect)...)

	statements = append(statements, "COMMIT;")

	return strings.Join(statements, "\n\n"), true
}

func sharedDDLSnapshotColumns(before, after ddlSnapshotTable) []string {
	beforeColumns := make(map[string]struct{}, len(before.Columns))
	for _, column := range before.Columns {
		beforeColumns[column.Name] = struct{}{}
	}

	names := make([]string, 0, len(after.Columns))
	for _, column := range after.Columns {
		if _, ok := beforeColumns[column.Name]; ok {
			names = append(names, column.Name)
		}
	}

	return names
}

func quoteDDLColumns(columns []string, dialect ddlDialectSpec) []string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, dialect.dialect.QuoteField(column))
	}

	return quoted
}

func renderDDLChangeOperation(dialect ddlDialectSpec, op ddlChange) []string {
	switch op.kind {
	case ddlChangeCreateTable:
		return []string{renderDDLSnapshotTableBlock(*op.newTable, dialect)}
	case ddlChangeDropTable:
		return []string{fmt.Sprintf("DROP TABLE %s;", dialect.dialect.QuoteField(op.oldTable.Name))}
	case ddlChangeAddColumn:
		if op.newColumn.PrimaryKey || op.newColumn.AutoIncrement {
			return []string{renderDDLManualComment(op.table, fmt.Sprintf("manual change required to add primary key column %s", op.newColumn.Name))}
		}

		return []string{fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s;",
			dialect.dialect.QuoteField(op.table),
			renderDDLSnapshotColumnDefinition(*op.newColumn, dialect),
		)}
	case ddlChangeDropColumn:
		return []string{fmt.Sprintf(
			"ALTER TABLE %s DROP COLUMN %s;",
			dialect.dialect.QuoteField(op.table),
			dialect.dialect.QuoteField(op.oldColumn.Name),
		)}
	case ddlChangeAlterColumn:
		return renderDDLAlterColumnStatements(dialect, op.table, *op.oldColumn, *op.newColumn)
	case ddlChangeAddIndex:
		return []string{renderDDLIndexCreateStatement(op.table, *op.newIndex, dialect)}
	case ddlChangeDropIndex:
		return []string{renderDDLDropIndexStatement(op.table, *op.oldIndex, dialect)}
	default:
		return nil
	}
}

func renderDDLAlterColumnStatements(
	dialect ddlDialectSpec,
	tableName string,
	before ddlSnapshotColumn,
	after ddlSnapshotColumn,
) []string {
	if before.PrimaryKey != after.PrimaryKey || before.AutoIncrement != after.AutoIncrement {
		return []string{renderDDLManualComment(tableName, fmt.Sprintf("manual change required for primary key column %s", after.Name))}
	}

	switch dialect.name {
	case ddlDialectMySQL:
		return []string{fmt.Sprintf(
			"ALTER TABLE %s MODIFY COLUMN %s;",
			dialect.dialect.QuoteField(tableName),
			renderDDLSnapshotColumnDefinition(after, dialect),
		)}
	case ddlDialectPostgres:
		statements := make([]string, 0, 3)
		quotedTable := dialect.dialect.QuoteField(tableName)
		quotedColumn := dialect.dialect.QuoteField(after.Name)

		if ddlColumnTypeChanged(before, after) {
			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
				quotedTable,
				quotedColumn,
				renderDDLColumnType(columnDescriptorFromSnapshot(after), dialect),
			))
		}

		if before.Nullable != after.Nullable {
			action := "SET"
			if after.Nullable {
				action = "DROP"
			}

			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s %s NOT NULL;",
				quotedTable,
				quotedColumn,
				action,
			))
		}

		if before.Default != after.Default {
			if after.Default == "" {
				statements = append(statements, fmt.Sprintf(
					"ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
					quotedTable,
					quotedColumn,
				))
			} else {
				statements = append(statements, fmt.Sprintf(
					"ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
					quotedTable,
					quotedColumn,
					after.Default,
				))
			}
		}

		if len(statements) == 0 {
			return []string{renderDDLManualComment(tableName, fmt.Sprintf("manual change required for column %s", after.Name))}
		}

		return statements

	default:
		return []string{renderDDLManualComment(tableName, fmt.Sprintf("manual change required for column %s on %s", after.Name, dialect.name))}
	}
}

func ddlColumnTypeChanged(before, after ddlSnapshotColumn) bool {
	return before.Kind != after.Kind ||
		before.Bits != after.Bits ||
		before.Unsigned != after.Unsigned ||
		before.Size != after.Size
}

func renderDDLDropIndexStatement(tableName string, idx ddlSnapshotIndex, dialect ddlDialectSpec) string {
	quotedIndex := dialect.dialect.QuoteField(idx.Name)
	if dialect.name == ddlDialectMySQL {
		return fmt.Sprintf(
			"DROP INDEX %s ON %s;",
			quotedIndex,
			dialect.dialect.QuoteField(tableName),
		)
	}

	return fmt.Sprintf("DROP INDEX %s;", quotedIndex)
}

func renderDDLManualComment(tableName, message string) string {
	return fmt.Sprintf("-- %s: %s", tableName, message)
}

func dialectIncrementalFilename(dialectName string) string {
	return dialectName + ".incremental.sql"
}

func columnDescriptorFromSnapshot(column ddlSnapshotColumn) ddlColumnDescriptor {
	return ddlColumnDescriptor{
		kind:     column.Kind,
		bits:     column.Bits,
		unsigned: column.Unsigned,
		nullable: column.Nullable,
		size:     column.Size,
	}
}
