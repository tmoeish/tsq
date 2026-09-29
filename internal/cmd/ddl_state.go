package cmd

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	"github.com/tmoeish/tsq/v5/internal/genmodel"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

const (
	ddlStateFilename = "tsq.json"
)

type ddlStateFile struct {
	GeneratedBy     string                        `json:"generated_by"`
	Version         string                        `json:"version"`
	Snapshot        ddlSnapshot                   `json:"snapshot"`
	InitialDialects map[string]ddlStateDialectSQL `json:"initial_dialects,omitempty"`
	RenderedRecords int                           `json:"rendered_records,omitempty"`
	Records         []ddlStateRecord              `json:"records,omitempty"`
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

type ddlStateDialectSQL struct {
	SQL string `json:"sql"`
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
	RawType       string        `json:"raw_type,omitempty"`
	PrimaryKey    bool          `json:"primary_key,omitempty"`
	AutoIncrement bool          `json:"auto_increment,omitempty"`
	Default       string        `json:"default,omitempty"`
	Fill          string        `json:"fill,omitempty"`
	Generated     string        `json:"generated,omitempty"`
}

type ddlSnapshotIndex struct {
	Name     string   `json:"name"`
	Fields   []string `json:"fields"`
	Unique   bool     `json:"unique"`
	FullText bool     `json:"full_text,omitempty"`
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
	// ddlChangeRenameColumn is a column whose name changed only in case: MySQL and
	// SQLite take both spellings for one column, so an add and a drop failed there.
	ddlChangeRenameColumn = "rename_column"
	ddlChangeAlterColumn  = "alter_column"
	ddlChangeAddIndex     = "add_index"
	ddlChangeDropIndex    = "drop_index"
)

func buildCurrentDDLSnapshot(tables []*genmodel.StructInfo, resolver *ddlTypeResolver) (ddlSnapshot, error) {
	snapshot := ddlSnapshot{Tables: make([]ddlSnapshotTable, 0, len(tables))}

	for _, table := range tables {
		item, err := buildCurrentDDLTableSnapshot(table, resolver)
		if err != nil {
			return ddlSnapshot{}, err
		}

		snapshot.Tables = append(snapshot.Tables, item)
	}

	sort.Slice(snapshot.Tables, func(i, j int) bool {
		return snapshot.Tables[i].Name < snapshot.Tables[j].Name
	})

	return snapshot, nil
}

func buildCurrentDDLTableSnapshot(
	table *genmodel.StructInfo,
	resolver *ddlTypeResolver,
) (ddlSnapshotTable, error) {
	result := ddlSnapshotTable{
		Name:    table.Table,
		Columns: make([]ddlSnapshotColumn, 0, len(table.Fields)),
		Indexes: make([]ddlSnapshotIndex, 0, len(table.Uniques)+len(table.Indexes)),
	}

	for _, field := range orderedDDLFields(table) {
		desc, err := resolver.describeField(table, field)
		if err != nil {
			return ddlSnapshotTable{}, fmt.Errorf("failed to describe %s.%s"+": %w", table.TypeInfo.TypeName, field.Name, err)
		}

		result.Columns = append(result.Columns, ddlSnapshotColumn{
			Name:          field.Column,
			Kind:          desc.kind,
			Bits:          desc.bits,
			Unsigned:      desc.unsigned,
			Nullable:      desc.nullable,
			Size:          desc.size,
			RawType:       desc.rawType,
			PrimaryKey:    field.Name == table.PrimaryKey,
			AutoIncrement: field.Name == table.PrimaryKey && table.AutoIncrement,
			Default:       ddlColumnDefault(table, field, desc),
			Fill:          desc.fill,
			Generated:     desc.generatedSQL,
		})
	}

	appendIndexes := func(items []genmodel.IndexInfo, unique, fullText bool) error {
		for _, idx := range items {
			fieldNames := idx.Fields
			if !fullText {
				fieldNames = indexFieldNames(table, idx.Fields)
			}

			fields := make([]string, 0, len(fieldNames))
			for _, fieldName := range fieldNames {
				field, ok := table.FieldsByName[fieldName]
				if !ok {
					return fmt.Errorf("index %s references unknown field %s", idx.Name, fieldName)
				}

				fields = append(fields, field.Column)
			}

			result.Indexes = append(result.Indexes, ddlSnapshotIndex{
				Name:     idx.Name,
				Fields:   fields,
				Unique:   unique,
				FullText: fullText,
			})
		}

		return nil
	}

	if err := appendIndexes(table.Uniques, true, false); err != nil {
		return ddlSnapshotTable{}, err
	}

	if err := appendIndexes(table.Indexes, false, false); err != nil {
		return ddlSnapshotTable{}, err
	}

	if err := appendIndexes(table.FullTexts, false, true); err != nil {
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
		return nil, fmt.Errorf("refusing to read non-generated DDL state file: %s", filename)
	}

	var state ddlStateFile
	if err := json.Unmarshal(content, &state); err != nil {
		return nil, fmt.Errorf("failed to parse DDL state file %s: %w", filename, err)
	}

	if state.RenderedRecords > len(state.Records) {
		state.RenderedRecords = len(state.Records)
	}

	return &state, nil
}

func marshalDDLStateFile(
	version string,
	previous *ddlStateFile,
	current ddlSnapshot,
	initialDialects map[string]ddlStateDialectSQL,
	renderedRecords int,
	recordTables []ddlStateRecordTable,
	dialects map[string]ddlStateDialectDiff,
	sequence string,
) ([]byte, error) {
	state := ddlStateFile{
		GeneratedBy:     "tsq-" + version,
		Version:         version,
		Snapshot:        current,
		InitialDialects: cloneDDLStateDialects(initialDialects),
		RenderedRecords: renderedRecords,
	}

	if previous != nil {
		state.Records = append(state.Records, previous.Records...)
	}

	if state.RenderedRecords > len(state.Records) {
		state.RenderedRecords = len(state.Records)
	}

	if sequence != "" && len(recordTables) > 0 {
		record := ddlStateRecord{
			Sequence: sequence,
			Tables:   append([]ddlStateRecordTable(nil), recordTables...),
			Dialects: cloneDDLStateDiffs(dialects),
		}

		state.Records = append(state.Records, record)
	}

	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(content, '\n'), nil
}

func cloneDDLStateDialects(items map[string]ddlStateDialectSQL) map[string]ddlStateDialectSQL {
	if len(items) == 0 {
		return nil
	}

	cloned := make(map[string]ddlStateDialectSQL, len(items))
	maps.Copy(cloned, items)

	return cloned
}

func cloneDDLStateDiffs(items map[string]ddlStateDialectDiff) map[string]ddlStateDialectDiff {
	if len(items) == 0 {
		return nil
	}

	cloned := make(map[string]ddlStateDialectDiff, len(items))
	maps.Copy(cloned, items)

	return cloned
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
			result.ByTable[table.Name] = []ddlChange{{
				kind:     ddlChangeCreateTable,
				table:    table.Name,
				newTable: new(table),
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
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:     ddlChangeCreateTable,
				table:    tableName,
				newTable: new(after),
			})
		case hadBefore && !hasAfter:
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:     ddlChangeDropTable,
				table:    tableName,
				oldTable: new(before),
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

	// A column whose name changed only in case is renamed, not dropped and added.
	renamedTo := map[string]string{}

	for _, column := range before.Columns {
		if _, ok := afterColumns[column.Name]; ok {
			continue
		}

		for _, other := range after.Columns {
			if _, taken := beforeColumns[other.Name]; !taken && strings.EqualFold(other.Name, column.Name) {
				renamedTo[column.Name] = other.Name
			}
		}
	}

	for _, column := range before.Columns {
		if _, ok := afterColumns[column.Name]; ok {
			continue
		}

		if name, ok := renamedTo[column.Name]; ok {
			renamed := afterColumns[name]
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:      ddlChangeRenameColumn,
				table:     tableName,
				oldTable:  &beforeTableCopy,
				newTable:  &afterTableCopy,
				oldColumn: new(column),
				newColumn: new(renamed),
			})

			// What else changed is an alter of the renamed column.
			column.Name = name
			beforeColumns[name] = column

			continue
		}

		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:      ddlChangeDropColumn,
			table:     tableName,
			oldTable:  &beforeTableCopy,
			newTable:  &afterTableCopy,
			oldColumn: new(column),
		})
	}

	for _, column := range after.Columns {
		beforeColumn, ok := beforeColumns[column.Name]
		if !ok {
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:      ddlChangeAddColumn,
				table:     tableName,
				oldTable:  &beforeTableCopy,
				newTable:  &afterTableCopy,
				newColumn: new(column),
			})

			continue
		}

		if reflect.DeepEqual(beforeColumn, column) {
			continue
		}

		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:      ddlChangeAlterColumn,
			table:     tableName,
			oldTable:  &beforeTableCopy,
			newTable:  &afterTableCopy,
			oldColumn: new(beforeColumn),
			newColumn: new(column),
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
			result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
				kind:     ddlChangeDropIndex,
				table:    tableName,
				oldTable: &beforeTableCopy,
				newTable: &afterTableCopy,
				oldIndex: new(idx),
			})

			continue
		}

		if reflect.DeepEqual(idx, next) {
			continue
		}

		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:     ddlChangeDropIndex,
			table:    tableName,
			oldTable: &beforeTableCopy,
			newTable: &afterTableCopy,
			oldIndex: new(idx),
		})
		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:     ddlChangeAddIndex,
			table:    tableName,
			oldTable: &beforeTableCopy,
			newTable: &afterTableCopy,
			newIndex: new(next),
		})
	}

	for _, idx := range after.Indexes {
		if _, ok := beforeIndexes[idx.Name]; ok {
			continue
		}

		result.ByTable[tableName] = append(result.ByTable[tableName], ddlChange{
			kind:     ddlChangeAddIndex,
			table:    tableName,
			oldTable: &beforeTableCopy,
			newTable: &afterTableCopy,
			newIndex: new(idx),
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

// ddlChangeCategoryRank orders a table's changes. An index is dropped before the
// columns change: dropping a column first fails on SQLite (the index names it)
// and takes the index with it on MySQL and PostgreSQL, so its DROP INDEX fails.
func ddlChangeCategoryRank(change ddlChange) int {
	switch change.kind {
	case ddlChangeCreateTable, ddlChangeDropTable:
		return 0
	case ddlChangeDropIndex:
		return 1
	case ddlChangeAddColumn, ddlChangeAlterColumn, ddlChangeDropColumn, ddlChangeRenameColumn:
		return 2
	case ddlChangeAddIndex:
		if ddlChangeIndexUnique(change) {
			return 3
		}

		return 4
	default:
		return 5
	}
}

func ddlChangeActionRank(change ddlChange) int {
	switch change.kind {
	case ddlChangeCreateTable, ddlChangeAddColumn, ddlChangeAddIndex:
		return 0
	case ddlChangeRenameColumn:
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
	case ddlChangeAddColumn, ddlChangeAlterColumn, ddlChangeRenameColumn:
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
	case ddlChangeRenameColumn:
		return "rename column " + change.oldColumn.Name + " to " + change.newColumn.Name, false
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
	buf.WriteString(ddlDialectName(dialect))
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

	var buf strings.Builder

	for _, column := range table.Columns {
		if migrationOwned(column) {
			buf.WriteString(renderDDLManualComment(table.Name, migrationOwnedNote(column)) + "\n")
			continue
		}

		lines = append(lines, "    "+renderDDLSnapshotColumnDefinition(column, dialect))
	}

	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
	buf.WriteString(dialect.dialect.QuoteIdent(table.Name))
	buf.WriteString(" (\n")
	buf.WriteString(strings.Join(lines, ",\n"))
	buf.WriteString("\n);")

	return buf.String()
}

// migrationOwned reports a column the database computes without an expression TSQ
// knows: generated with no SQL. TSQ cannot write it, so its DDL leaves it out.
func migrationOwned(column ddlSnapshotColumn) bool {
	return column.Fill == "generated" && column.Generated == ""
}

func migrationOwnedNote(column ddlSnapshotColumn) string {
	return fmt.Sprintf("%s is computed by the database (generated, no expression); add it in the migration that owns this table", column.Name)
}

func renderDDLSnapshotColumnDefinition(column ddlSnapshotColumn, dialect ddlDialectSpec) string {
	definition, err := renderDDLColumnSpec(dialect.dialect, ddlColumnSpecFromSnapshot(column))
	if err != nil {
		panic(err)
	}

	return definition
}

func renderDDLSnapshotIndexStatements(table ddlSnapshotTable, dialect ddlDialectSpec) []string {
	statements := make([]string, 0, len(table.Indexes))

	for _, idx := range table.Indexes {
		// A dialect without a full-text index of its own has nothing to write.
		statement := renderDDLIndexCreateStatement(table.Name, idx, dialect)
		if statement == "" {
			continue
		}

		// MySQL rejects some indexes the other dialects take; the statement stays,
		// and says why it will fail, for a schema that also runs on MySQL.
		if dialect.dialect.Name() == tsqdialect.MySQL && !idx.FullText {
			if problem := mysqlIndexProblem(idx.Name, idx.Fields, snapshotColumnType(table)); problem != "" {
				statements = append(statements, renderDDLManualComment(table.Name, problem))
			}
		}

		statements = append(statements, statement)
	}

	return statements
}

func renderDDLIndexCreateStatement(tableName string, idx ddlSnapshotIndex, dialect ddlDialectSpec) string {
	quotedFields := make([]string, 0, len(idx.Fields))
	for _, field := range idx.Fields {
		quotedFields = append(quotedFields, dialect.dialect.QuoteIdent(field))
	}

	if idx.FullText {
		// SQLite has no full-text index TSQ manages, so there is nothing to write.
		return dialect.dialect.FullTextIndexSQL(tableName, idx.Name, quotedFields)
	}

	return dialect.dialect.CreateIndexSQL(tableName, idx.Name, quotedFields, idx.Unique)
}

func renderDDLIncrementalArtifact(dialect ddlDialectSpec, changes ddlChangeSet) (ddlStateDialectDiff, error) {
	result := ddlStateDialectDiff{
		AggregateSQL: renderDDLIncrementalAggregateBody(dialect, changes),
	}

	return result, nil
}

func renderDDLIncrementalAggregateBody(dialect ddlDialectSpec, changes ddlChangeSet) string {
	if len(changes.Tables) == 0 {
		return "-- No schema changes."
	}

	var sections []string

	// Index names are global on PostgreSQL and SQLite, so an index that moves to
	// another table, or keeps its name through a renamed table, is created by one
	// table's section after the other's still holds it. Every index drop goes
	// first, and a dropped table (whose DROP is left commented) gives up the index
	// names a new index takes; an index holds no data.
	created := map[string]bool{}

	for _, tableName := range changes.Tables {
		for _, op := range changes.ByTable[tableName] {
			switch op.kind {
			case ddlChangeAddIndex:
				created[op.newIndex.Name] = true
			case ddlChangeCreateTable:
				for _, index := range op.newTable.Indexes {
					created[index.Name] = true
				}
			}
		}
	}

	var early []ddlChange

	byTable := make(map[string][]ddlChange, len(changes.ByTable))

	for _, tableName := range changes.Tables {
		for _, op := range changes.ByTable[tableName] {
			switch op.kind {
			case ddlChangeDropIndex:
				early = append(early, op)
				continue
			case ddlChangeDropTable:
				for _, index := range op.oldTable.Indexes {
					if created[index.Name] {
						early = append(early, ddlChange{kind: ddlChangeDropIndex, table: tableName, oldTable: op.oldTable, oldIndex: &index})
					}
				}
			}

			byTable[tableName] = append(byTable[tableName], op)
		}
	}

	var dropped []string

	for _, op := range early {
		dropped = append(dropped, renderDDLChangeOperation(dialect, op)...)
	}

	if len(dropped) > 0 {
		sections = append(sections, "-- Indexes dropped before the tables change\n\n"+strings.Join(dropped, "\n\n"))
	}

	for _, tableName := range changes.Tables {
		body, ok := renderDDLIncrementalTableBody(dialect, tableName, byTable[tableName])
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

	if dialect.dialect.AlterMode() == sqld.AlterRebuild && ddlChangesRequireTableRebuild(ops) {
		body, ok := renderSQLiteRebuildTableBody(dialect, tableName, ops)
		if ok {
			return body, true
		}
	}

	// Statements run in the order the summary lists them: indexes are dropped
	// before the columns they name (ddlChangeCategoryRank).
	ops = slices.Clone(ops)
	slices.SortStableFunc(ops, compareDDLChanges)

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

// ddlChangesRequireTableRebuild reports changes SQLite cannot make in place:
// altering a column, and adding a stored generated column (ALTER TABLE ADD
// COLUMN refuses STORED).
func ddlChangesRequireTableRebuild(ops []ddlChange) bool {
	for _, op := range ops {
		if op.kind == ddlChangeAlterColumn && !sqliteAlterUnenforced(*op.oldColumn, *op.newColumn) {
			return true
		}

		if op.kind == ddlChangeAddColumn && op.newColumn.Generated != "" {
			return true
		}
	}

	return false
}

// sqliteAlterUnenforced reports a column change SQLite would not enforce: only
// the declared type moved, within one type affinity (VARCHAR(20) to VARCHAR(40),
// INT to BIGINT). A rebuild for it copied the table and dropped its triggers and
// hand-made indexes to change nothing SQLite checks.
func sqliteAlterUnenforced(before, after ddlSnapshotColumn) bool {
	d := sqld.SQLiteDialect{}
	spelled := func(c ddlSnapshotColumn) string { return d.ColumnTypeSQL(ddlColumnSpecFromSnapshot(c).Type) }

	return before.Nullable == after.Nullable && before.Default == after.Default && before.Fill == after.Fill &&
		before.Generated == after.Generated && before.PrimaryKey == after.PrimaryKey && before.AutoIncrement == after.AutoIncrement &&
		sqliteAffinity(spelled(before)) == sqliteAffinity(spelled(after))
}

// sqliteAffinity is the type affinity SQLite gives a declared type, by its rules
// (datatype3.html, section 3.1).
func sqliteAffinity(declared string) string {
	t := strings.ToUpper(declared)

	switch {
	case strings.Contains(t, "INT"):
		return "INTEGER"
	case strings.Contains(t, "CHAR"), strings.Contains(t, "CLOB"), strings.Contains(t, "TEXT"):
		return "TEXT"
	case strings.Contains(t, "BLOB"), t == "":
		return "BLOB"
	case strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"):
		return "REAL"
	default:
		return "NUMERIC"
	}
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

	// The section is run by a client that may not stop at the first error: the
	// sqlite3 shell goes on to the DROP after a failed copy, and the table's rows
	// are gone. So the copy must not be able to fail: a new NOT NULL column gets
	// its type's zero value, a generated column is left to the new table, and what
	// cannot be filled that way is not rebuilt at all.
	// Columns are matched without case, as SQLite matches them: a column renamed
	// only in case is copied from its old spelling.
	existing := make(map[string]ddlSnapshotColumn, len(before.Columns))
	for _, column := range before.Columns {
		existing[strings.ToLower(column.Name)] = column
	}

	var (
		notes   []string
		targets []string
		sources []string
	)

	for _, column := range after.Columns {
		quoted := dialect.dialect.QuoteIdent(column.Name)

		switch {
		case column.Generated != "":
			continue
		case migrationOwned(column):
			return renderDDLManualComment(tableName, fmt.Sprintf(
				"manual rebuild required: %s is computed by the database with no expression TSQ knows, so a rebuilt table would lose it", column.Name)), true
		case existing[strings.ToLower(column.Name)].Name != "" && existing[strings.ToLower(column.Name)].Nullable && !column.Nullable:
			// A column that becomes NOT NULL: its NULLs take the default, or the
			// type's zero value, so the copy cannot fail on them.
			fill := column.Default
			if fill == "" {
				zero, ok := sqliteZeroLiteral(column)
				if !ok {
					return renderDDLManualComment(tableName, fmt.Sprintf(
						"manual rebuild required: %s becomes NOT NULL without a default, and its type:%s has no known zero value for the rows that hold NULL", column.Name, column.RawType)), true
				}

				fill = zero
			}

			notes = append(notes, renderDDLManualComment(tableName, fmt.Sprintf(
				"%s becomes NOT NULL; rows holding NULL get %s", column.Name, fill)))
			targets = append(targets, quoted)
			sources = append(sources, fmt.Sprintf("COALESCE(%s, %s)", dialect.dialect.QuoteIdent(existing[strings.ToLower(column.Name)].Name), fill))
		case existing[strings.ToLower(column.Name)].Name != "":
			targets = append(targets, quoted)
			sources = append(sources, dialect.dialect.QuoteIdent(existing[strings.ToLower(column.Name)].Name))
		case column.Nullable || column.Default != "" || column.AutoIncrement:
			// The new table fills it.
		default:
			zero, ok := sqliteZeroLiteral(column)
			if !ok {
				return renderDDLManualComment(tableName, fmt.Sprintf(
					"manual rebuild required: %s is NOT NULL without a default, and its type:%s has no known zero value to fill existing rows with", column.Name, column.RawType)), true
			}

			notes = append(notes, renderDDLManualComment(tableName, fmt.Sprintf(
				"%s is NOT NULL without a default; existing rows get %s", column.Name, zero)))
			targets = append(targets, quoted)
			sources = append(sources, zero)
		}
	}

	var dropped []string

	kept := make(map[string]bool, len(after.Columns))
	for _, column := range after.Columns {
		kept[strings.ToLower(column.Name)] = true
	}

	for _, column := range before.Columns {
		if !kept[strings.ToLower(column.Name)] {
			dropped = append(dropped, column.Name)
		}
	}

	newTable := "__tsq_new_" + tableName
	fresh := *after
	fresh.Name = newTable

	statements := append(notes,
		renderDDLManualComment(tableName, "rebuilt by copying its rows; triggers on it are dropped and must be created again"),
		// Dropping the old table with foreign keys on would cascade to, or fail
		// for, the rows that reference it (SQLite's documented rebuild procedure).
		"PRAGMA foreign_keys = OFF;",
		"BEGIN TRANSACTION;",
		renderDDLSnapshotCreateTable(fresh, dialect),
	)

	if len(targets) > 0 {
		statements = append(statements, fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s;",
			dialect.dialect.QuoteIdent(newTable),
			strings.Join(targets, ", "),
			strings.Join(sources, ", "),
			dialect.dialect.QuoteIdent(tableName),
		))
	}

	// AUTOINCREMENT's counter is kept under the table's name; without it the new
	// table would hand out the keys of rows deleted before the rebuild again.
	if slices.ContainsFunc(after.Columns, func(c ddlSnapshotColumn) bool { return c.AutoIncrement }) {
		statements = append(statements,
			fmt.Sprintf("DELETE FROM sqlite_sequence WHERE name = %s;", sqlLiteral(newTable)),
			fmt.Sprintf("INSERT INTO sqlite_sequence (name, seq) SELECT %s, seq FROM sqlite_sequence WHERE name = %s;",
				sqlLiteral(newTable), sqlLiteral(tableName)),
		)
	}

	statements = append(statements,
		fmt.Sprintf("DROP TABLE %s;", dialect.dialect.QuoteIdent(tableName)),
		fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", dialect.dialect.QuoteIdent(newTable), dialect.dialect.QuoteIdent(tableName)),
	)
	statements = append(statements, renderDDLSnapshotIndexStatements(*after, dialect)...)
	statements = append(statements, "PRAGMA foreign_key_check;", "COMMIT;", "PRAGMA foreign_keys = ON;")

	// A rebuild that leaves a column out drops its data, like DROP COLUMN.
	if len(dropped) > 0 {
		return destructive(tableName, "rebuilds the table without "+strings.Join(dropped, ", ")+" and their data",
			strings.Join(statements, "\n\n"))[0], true
	}

	return strings.Join(statements, "\n\n"), true
}

// sqliteZeroLiteral is the Go zero value of column's type as a SQLite literal,
// which a rebuild writes into a new NOT NULL column of existing rows. A column of
// an explicit type: has none TSQ knows.
func sqliteZeroLiteral(column ddlSnapshotColumn) (string, bool) {
	if column.RawType != "" {
		return "", false
	}

	switch column.Kind {
	case ddlColumnString:
		return "''", true
	case ddlColumnInt, ddlColumnBool, ddlColumnFloat:
		return "0", true
	case ddlColumnBytes:
		return "X''", true
	case ddlColumnTime:
		return "'0001-01-01 00:00:00+00:00'", true
	}

	return "", false
}

// sqlLiteral quotes s as a SQL string literal.
func sqlLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func renderDDLChangeOperation(dialect ddlDialectSpec, op ddlChange) []string {
	switch op.kind {
	case ddlChangeCreateTable:
		return []string{renderDDLSnapshotTableBlock(*op.newTable, dialect)}
	case ddlChangeDropTable:
		return destructive(op.oldTable.Name, "drops the table and its rows",
			fmt.Sprintf("DROP TABLE %s;", dialect.dialect.QuoteIdent(op.oldTable.Name)))
	case ddlChangeAddColumn:
		if migrationOwned(*op.newColumn) {
			return []string{renderDDLManualComment(op.table, migrationOwnedNote(*op.newColumn))}
		}

		if op.newColumn.PrimaryKey || op.newColumn.AutoIncrement {
			return []string{renderDDLManualComment(op.table, fmt.Sprintf("manual change required to add primary key column %s", op.newColumn.Name))}
		}

		statement := fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s;",
			dialect.dialect.QuoteIdent(op.table),
			renderDDLSnapshotColumnDefinition(*op.newColumn, dialect),
		)

		// Right on an empty table, and refused by every dialect on one with rows:
		// the migration is the user's to run, so it says so where it will be read.
		if column := op.newColumn; !column.Nullable && column.Default == "" && column.Generated == "" {
			return []string{
				renderDDLManualComment(op.table, fmt.Sprintf(
					"%s is NOT NULL without a default, which fails on a table with rows; declare default: or backfill it first", column.Name)),
				statement,
			}
		}

		return []string{statement}

	case ddlChangeDropColumn:
		return destructive(op.table, "drops column "+op.oldColumn.Name+" and its data", fmt.Sprintf(
			"ALTER TABLE %s DROP COLUMN %s;",
			dialect.dialect.QuoteIdent(op.table),
			dialect.dialect.QuoteIdent(op.oldColumn.Name),
		))
	case ddlChangeAlterColumn:
		return renderDDLAlterColumnStatements(dialect, op.table, *op.oldColumn, *op.newColumn)
	case ddlChangeRenameColumn:
		if dialect.dialect.Name() != tsqdialect.Postgres {
			return []string{renderDDLManualComment(op.table, fmt.Sprintf(
				"column %s is now spelled %s; %s matches column names without case, so there is nothing to run", op.oldColumn.Name, op.newColumn.Name, ddlDialectName(dialect)))}
		}

		return []string{fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;",
			dialect.dialect.QuoteIdent(op.table), dialect.dialect.QuoteIdent(op.oldColumn.Name), dialect.dialect.QuoteIdent(op.newColumn.Name))}
	case ddlChangeAddIndex:
		if statement := renderDDLIndexCreateStatement(op.table, *op.newIndex, dialect); statement != "" {
			return []string{statement}
		}

		return nil
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

	// A generated column is recreated, not altered: MySQL's MODIFY COLUMN would
	// write it as a plain column TSQ never fills, and PostgreSQL cannot change the
	// expression in place.
	if before.Generated != after.Generated || (before.Fill == "generated") != (after.Fill == "generated") {
		return []string{renderDDLManualComment(tableName, fmt.Sprintf(
			"manual change required: %s changes how the database computes it; drop and add the column in a migration", after.Name))}
	}

	if dialect.dialect.AlterMode() != sqld.AlterInPlace {
		if sqliteAlterUnenforced(before, after) {
			return []string{renderDDLManualComment(tableName, fmt.Sprintf(
				"column %s changes only its declared type within one SQLite type affinity, which SQLite does not enforce; nothing to run", after.Name))}
		}

		return []string{renderDDLManualComment(tableName, fmt.Sprintf("manual change required for column %s on %s", after.Name, ddlDialectName(dialect)))}
	}

	statements := dialect.dialect.AlterColumnSQL(tableName, sqld.Column{ColumnSpec: ddlColumnSpecFromSnapshot(before)}, ddlColumnSpecFromSnapshot(after))
	if len(statements) == 0 {
		return []string{renderDDLManualComment(tableName, fmt.Sprintf("manual change required for column %s", after.Name))}
	}

	return statements
}

func ddlColumnTypeChanged(before, after ddlSnapshotColumn) bool {
	return before.Kind != after.Kind ||
		before.Bits != after.Bits ||
		before.Unsigned != after.Unsigned ||
		before.Size != after.Size ||
		before.RawType != after.RawType
}

func renderDDLDropIndexStatement(tableName string, idx ddlSnapshotIndex, dialect ddlDialectSpec) string {
	return dialect.dialect.DropIndexSQL(tableName, idx.Name)
}

func renderDDLManualComment(tableName, message string) string {
	return fmt.Sprintf("-- %s: %s", tableName, message)
}

func renderDDLAggregateFileFromState(state ddlStateFile, dialectName string) ([]byte, error) {
	initial, ok := state.InitialDialects[dialectName]
	if !ok || strings.TrimSpace(initial.SQL) == "" {
		return nil, fmt.Errorf("missing initial DDL for dialect %s", dialectName)
	}

	var buf strings.Builder
	buf.WriteString(strings.TrimRight(initial.SQL, "\n"))

	for idx, record := range state.Records {
		if idx < state.RenderedRecords {
			continue
		}

		diff, ok := record.Dialects[dialectName]
		if !ok || strings.TrimSpace(diff.AggregateSQL) == "" {
			continue
		}

		buf.WriteString("\n\n")
		buf.WriteString(renderDDLHistorySection(record.Sequence, diff.AggregateSQL))
	}

	buf.WriteByte('\n')

	return []byte(buf.String()), nil
}

func renderDDLHistorySection(sequence, body string) string {
	var buf strings.Builder
	buf.WriteString("-- Migration: ")
	buf.WriteString(sequence)
	buf.WriteString("\n\n")
	buf.WriteString(strings.TrimSpace(body))

	return buf.String()
}

// snapshotColumnType looks up the type of a column of table by name.
func snapshotColumnType(table ddlSnapshotTable) func(string) (tsqdialect.ColumnType, bool) {
	return func(name string) (tsqdialect.ColumnType, bool) {
		for _, column := range table.Columns {
			if column.Name == name {
				return ddlColumnSpecFromSnapshot(column).Type, true
			}
		}

		return tsqdialect.ColumnType{}, false
	}
}

// destructiveMarker starts every statement a migration writes commented out
// because it destroys data: renaming a table or a db tag, or a directive typed
// wrongly, looks the same to the generator as removing it on purpose.
const destructiveMarker = "-- DESTRUCTIVE"

// destructive writes statements commented out behind destructiveMarker, to be
// run by hand once the drop is known to be meant.
func destructive(table, what string, statements ...string) []string {
	lines := []string{fmt.Sprintf("%s (%s %s): check it is meant, then run it by hand", destructiveMarker, table, what)}

	for _, statement := range statements {
		for line := range strings.SplitSeq(statement, "\n") {
			lines = append(lines, "-- "+line)
		}
	}

	return []string{strings.Join(lines, "\n")}
}
