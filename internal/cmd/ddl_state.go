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

	"golang.org/x/mod/semver"

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
	// Renderings is how each dialect spelled each column the last time the files
	// were written: dialect, then table, then column. The snapshot is the model;
	// this is what the SQL file's schema states for it, so that a TSQ version
	// that spells a column otherwise (a default, a type, a range constraint) with
	// no change in the model still gets a migration section (ddlRespellings).
	Renderings map[string]ddlDialectRenderings `json:"renderings,omitempty"`
}

// ddlDialectRenderings maps a table, then a column, to its rendering.
type ddlDialectRenderings map[string]map[string]ddlColumnRendering

// ddlColumnRendering is a column as one dialect spells it in CREATE TABLE.
type ddlColumnRendering struct {
	Type    string `json:"type"`
	Default string `json:"default,omitempty"`
	Check   string `json:"check,omitempty"`
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
	// wasSpelled and spelled are the renderings of a column TSQ now spells
	// otherwise (ddlChangeRespellColumn), and respelled names what changed, for
	// the record.
	wasSpelled *ddlColumnRendering
	spelled    *ddlColumnRendering
	respelled  string
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
	// ddlChangeRespellColumn is a column whose declaration did not change and
	// whose spelling by a dialect did, between two versions of TSQ.
	ddlChangeRespellColumn = "respell_column"
)

// buildDDLRenderings spells every column of snapshot as each dialect does.
func buildDDLRenderings(snapshot ddlSnapshot) map[string]ddlDialectRenderings {
	renderings := make(map[string]ddlDialectRenderings, len(ddlDialects))

	for _, dialect := range ddlDialects {
		tables := make(ddlDialectRenderings, len(snapshot.Tables))

		for _, table := range snapshot.Tables {
			columns := make(map[string]ddlColumnRendering, len(table.Columns))

			for _, column := range table.Columns {
				columns[column.Name] = renderDDLColumnSpelling(dialect, ddlColumnSpecFromSnapshot(column))
			}

			tables[table.Name] = columns
		}

		renderings[ddlDialectName(dialect)] = tables
	}

	return renderings
}

// renderDDLColumnSpelling is what a dialect's CREATE TABLE states for column: the
// type, the default as written, and the range constraint, each of which TSQ has
// changed between versions with the declaration staying as it was.
func renderDDLColumnSpelling(dialect ddlDialectSpec, column tsqdialect.ColumnSpec) ddlColumnRendering {
	rendering := ddlColumnRendering{Type: strings.TrimSpace(dialect.dialect.ColumnTypeSQL(column.Type))}

	if column.Default != "" {
		rendering.Default = sqld.DefaultSQL(dialect.dialect, column)
	}

	rendering.Check, _ = sqld.RangeCheck(dialect.dialect, column)

	return rendering
}

// ddlRespellings finds, per dialect, the columns the previous files spelled
// otherwise than TSQ spells them now while the declaration stayed the same: a
// table built from those files differs from the schema the runtime declares, so
// the difference goes into a migration section like a model change does. Files
// from before renderings were recorded have none to compare, and get none.
func ddlRespellings(previous *ddlStateFile, current ddlSnapshot, renderings map[string]ddlDialectRenderings) map[string][]ddlChange {
	if previous == nil || len(previous.Renderings) == 0 {
		return nil
	}

	previousTables := make(map[string]ddlSnapshotTable, len(previous.Snapshot.Tables))
	for _, table := range previous.Snapshot.Tables {
		previousTables[table.Name] = table
	}

	result := make(map[string][]ddlChange)

	for _, dialect := range ddlDialects {
		name := ddlDialectName(dialect)

		was, ok := previous.Renderings[name]
		if !ok {
			continue
		}

		for _, table := range current.Tables {
			before, ok := previousTables[table.Name]
			if !ok {
				continue
			}

			beforeColumns := make(map[string]ddlSnapshotColumn, len(before.Columns))
			for _, column := range before.Columns {
				beforeColumns[column.Name] = column
			}

			beforeCopy, afterCopy := before, table

			for _, column := range table.Columns {
				beforeColumn, ok := beforeColumns[column.Name]
				if !ok || !reflect.DeepEqual(beforeColumn, column) {
					continue // added or changed: the model diff has it
				}

				wasSpelled, ok := was[table.Name][column.Name]
				if !ok {
					continue
				}

				spelled := renderings[name][table.Name][column.Name]
				if wasSpelled == spelled {
					continue
				}

				result[name] = append(result[name], ddlChange{
					kind:       ddlChangeRespellColumn,
					table:      table.Name,
					oldTable:   &beforeCopy,
					newTable:   &afterCopy,
					oldColumn:  new(beforeColumn),
					newColumn:  new(column),
					wasSpelled: &wasSpelled,
					spelled:    &spelled,
					respelled:  respelledParts(wasSpelled, spelled),
				})
			}
		}
	}

	return result
}

// respelledParts names what differs between two renderings: type, default, check.
func respelledParts(was, now ddlColumnRendering) string {
	var parts []string

	if was.Type != now.Type {
		parts = append(parts, "type")
	}

	if was.Default != now.Default {
		parts = append(parts, "default")
	}

	if was.Check != now.Check {
		parts = append(parts, "range check")
	}

	return strings.Join(parts, ", ")
}

// withRespellings adds the respellings of one dialect to the model's changes.
func withRespellings(changes ddlChangeSet, respellings []ddlChange) ddlChangeSet {
	if len(respellings) == 0 {
		return changes
	}

	merged := ddlChangeSet{
		Tables:  slices.Clone(changes.Tables),
		ByTable: maps.Clone(changes.ByTable),
	}

	if merged.ByTable == nil {
		merged.ByTable = make(map[string][]ddlChange)
	}

	for _, change := range respellings {
		if !slices.Contains(merged.Tables, change.table) {
			merged.Tables = append(merged.Tables, change.table)
		}

		merged.ByTable[change.table] = append(slices.Clone(merged.ByTable[change.table]), change)
	}

	sort.Strings(merged.Tables)

	return merged
}

// respellingSummary folds the respellings of every dialect into one change per
// column, for the record, which names the dialects beside what each respelled.
func respellingSummary(respellings map[string][]ddlChange) []ddlChange {
	type key struct{ table, column string }

	notes := map[key][]string{}
	changes := map[key]ddlChange{}

	for _, dialect := range ddlDialects {
		name := ddlDialectName(dialect)

		for _, change := range respellings[name] {
			k := key{change.table, change.newColumn.Name}
			notes[k] = append(notes[k], name+": "+change.respelled)
			changes[k] = change
		}
	}

	keys := slices.SortedFunc(maps.Keys(changes), func(a, b key) int {
		if a.table != b.table {
			return strings.Compare(a.table, b.table)
		}

		return strings.Compare(a.column, b.column)
	})

	result := make([]ddlChange, 0, len(keys))

	for _, k := range keys {
		change := changes[k]
		sort.Strings(notes[k])
		change.respelled = strings.Join(notes[k], "; ")
		result = append(result, change)
	}

	return result
}

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
		return nil, fmt.Errorf("%s is not a state file tsq gen wrote, or no longer one: it does not read as JSON with a generated_by field "+
			"(a merge conflict left in it, or a file cut short); restore it from version control", filename)
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

// refuseNewerStateFile refuses to run over a state file a newer TSQ wrote: this
// one would write it back as its own, in its own form, dropping what the newer
// one records (a rendering, a field it does not know), and the next gen by the
// newer one would read that as a change and write a migration for nothing. A
// team's CLIs then ping-pong the file. A development build (no release version)
// skips the check: it carries no version to compare.
func refuseNewerStateFile(outDir string, state *ddlStateFile, version string) error {
	if state == nil || !semver.IsValid(version) || !semver.IsValid(state.Version) {
		return nil
	}

	if semver.Compare(state.Version, version) <= 0 {
		return nil
	}

	return fmt.Errorf("%s was written by tsq %s, newer than this tsq (%s); upgrade tsq (go install github.com/tmoeish/tsq/v5/cmd/tsq@%s), "+
		"or delete the state file and the generated SQL files to start their history over with this version",
		filepath.Join(outDir, ddlStateFilename), state.Version, version, state.Version)
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
	renderings map[string]ddlDialectRenderings,
) ([]byte, error) {
	state := ddlStateFile{
		GeneratedBy:     "tsq-" + version,
		Version:         version,
		Snapshot:        current,
		InitialDialects: cloneDDLStateDialects(initialDialects),
		RenderedRecords: renderedRecords,
		Renderings:      renderings,
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
	case ddlChangeAddColumn, ddlChangeAlterColumn, ddlChangeRespellColumn, ddlChangeDropColumn, ddlChangeRenameColumn:
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
	case ddlChangeAlterColumn, ddlChangeRespellColumn:
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
	case ddlChangeAddColumn, ddlChangeAlterColumn, ddlChangeRespellColumn, ddlChangeRenameColumn:
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
	case ddlChangeRespellColumn:
		return "respell column " + change.newColumn.Name + " (" + change.respelled + ")", false
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

	// The statement fails on MySQL when the columns are wider than a row: said
	// above it, as the index limits are, since the file is written for every dialect.
	if dialect.dialect.Name() == tsqdialect.MySQL {
		columns := make([]tsqdialect.ColumnSpec, 0, len(table.Columns))
		for _, column := range table.Columns {
			columns = append(columns, ddlColumnSpecFromSnapshot(column))
		}

		if problem := mysqlRowProblem(table.Name, columns); problem != "" {
			buf.WriteString(renderDDLManualComment(table.Name, problem) + "\n")
		}
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

	droppedNames := map[string]bool{}

	for _, op := range early {
		dropped = append(dropped, renderDDLChangeOperation(dialect, op)...)

		if op.oldIndex != nil {
			droppedNames[op.oldIndex.Name] = true
		}
	}

	// A unique index created under a name the section dropped first replaces an
	// index over the rows there are: where rows share its values, the CREATE
	// fails after the DROP ran, and the table is left with neither. The runtime's
	// Reconcile builds the new one under another name first; a file run by hand
	// says so instead, before the DROP.
	var replaced []string

	for _, tableName := range changes.Tables {
		for _, op := range byTable[tableName] {
			if op.kind == ddlChangeAddIndex && op.newIndex.Unique && droppedNames[op.newIndex.Name] {
				replaced = append(replaced, fmt.Sprintf("-- %s: unique index %s is recreated below over the rows there are; where rows share (%s), its CREATE fails after the DROP, leaving the table without it: check them first",
					tableName, op.newIndex.Name, strings.Join(op.newIndex.Fields, ", ")))
			}
		}
	}

	if len(dropped) > 0 {
		sections = append(sections, "-- Indexes dropped before the tables change\n\n"+strings.Join(append(replaced, dropped...), "\n\n"))
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

		// A default or a range constraint spelled otherwise is enforced; a type
		// spelled otherwise within one affinity is not.
		if op.kind == ddlChangeRespellColumn && (op.wasSpelled.Default != op.spelled.Default || op.wasSpelled.Check != op.spelled.Check) {
			return true
		}

		if op.kind == ddlChangeAddColumn && (op.newColumn.Generated != "" || sqliteAddNeedsRebuild(*op.newColumn)) {
			return true
		}
	}

	return false
}

// sqliteAddNeedsRebuild reports a column SQLite cannot ADD to a table with rows: a
// default that is not a constant (CURRENT_TIMESTAMP, an expression), which
// adding created_at to an existing table always has, or NOT NULL without a
// default. The rebuild creates the table with it, where both are allowed, and
// fills the rows with the default or the type's zero value.
func sqliteAddNeedsRebuild(column ddlSnapshotColumn) bool {
	return !migrationOwned(column) && sqld.AddNeedsRebuild(sqld.SQLiteDialect{}, ddlColumnSpecFromSnapshot(column))
}

// sqliteAlterUnenforced reports a column change SQLite would not enforce: only
// the declared type moved, within one type affinity (VARCHAR(20) to VARCHAR(40),
// INT to BIGINT). A rebuild for it copied the table and dropped its triggers and
// hand-made indexes to change nothing SQLite checks.
func sqliteAlterUnenforced(before, after ddlSnapshotColumn) bool {
	d := sqld.SQLiteDialect{}
	spelled := func(c ddlSnapshotColumn) string { return d.ColumnTypeSQL(ddlColumnSpecFromSnapshot(c).Type) }

	// The range constraint is enforced, so a field of another width is a change.
	beforeCheck, _ := sqld.RangeCheck(d, ddlColumnSpecFromSnapshot(before))
	afterCheck, _ := sqld.RangeCheck(d, ddlColumnSpecFromSnapshot(after))

	return before.Nullable == after.Nullable && before.Default == after.Default && before.Fill == after.Fill &&
		before.Generated == after.Generated && before.PrimaryKey == after.PrimaryKey && before.AutoIncrement == after.AutoIncrement &&
		sqld.SQLiteAffinity(spelled(before)) == sqld.SQLiteAffinity(spelled(after)) && beforeCheck == afterCheck
}

// sqliteConversionNote says what a rebuild does to the values of a column that
// becomes t, for the note above the statements.
func sqliteConversionNote(t tsqdialect.ColumnType) string {
	if t.Kind == tsqdialect.KindBool {
		return "any number but zero becomes true"
	}

	return "fractions are rounded"
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
		old, kept := existing[strings.ToLower(column.Name)]
		source := dialect.dialect.QuoteIdent(old.Name)

		// A column that changes type: SQLite stores any value under any type, so
		// the copy cannot fail, and a value that does not convert would stay as it
		// is under the new type, where no read takes it. What always converts is
		// converted; what may not is left to a hand that can look at the rows.
		if kept && column.Generated == "" && !migrationOwned(column) {
			oldType, newType := ddlColumnSpecFromSnapshot(old).Type, ddlColumnSpecFromSnapshot(column).Type

			switch sqld.SQLiteRetype(oldType, newType) {
			case sqld.RetypeMayFail:
				_, kind := sqld.SQLiteMisfit(quoted, newType)

				return renderDDLManualComment(tableName, fmt.Sprintf(
					"manual rebuild required: %s changes from %s to %s, and SQLite keeps a value that is not %s as it is, where no read takes it; "+
						"a script cannot refuse it, so convert the column by hand (SchemaPolicyReconcile does it when every value converts, and refuses when one does not)",
					column.Name, oldType.Kind, newType.Kind, kind)), true
			case sqld.RetypeConvert:
				source = sqld.SQLiteRetypeSource(source, newType)

				notes = append(notes, renderDDLManualComment(tableName, fmt.Sprintf(
					"%s changes from %s to %s; %s", column.Name, oldType.Kind, newType.Kind, sqliteConversionNote(newType))))
			case sqld.RetypeAsIs:
			}
		}

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
				zero, ok := sqld.ZeroLiteral(sqld.SQLiteDialect{}, ddlColumnSpecFromSnapshot(column).Type)
				if !ok {
					return renderDDLManualComment(tableName, fmt.Sprintf(
						"manual rebuild required: %s becomes NOT NULL without a default, and its type:%s has no known zero value for the rows that hold NULL", column.Name, column.RawType)), true
				}

				fill = zero
			}

			notes = append(notes, renderDDLManualComment(tableName, fmt.Sprintf(
				"%s becomes NOT NULL; rows holding NULL get %s", column.Name, fill)))
			targets = append(targets, quoted)
			sources = append(sources, fmt.Sprintf("COALESCE(%s, %s)", source, fill))
		case existing[strings.ToLower(column.Name)].Name != "":
			targets = append(targets, quoted)
			sources = append(sources, source)
		case column.Nullable || column.Default != "" || column.AutoIncrement:
			// The new table fills it.
		default:
			zero, ok := sqld.ZeroLiteral(sqld.SQLiteDialect{}, ddlColumnSpecFromSnapshot(column).Type)
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

		// NOT NULL without a default is refused on a table with rows (by MySQL for
		// a time only). The rows get the type's zero value, as the SQLite rebuild
		// gives them: the column is added with that default, which is then dropped.
		// The runtime policies add a column with the same statements.
		spec := ddlColumnSpecFromSnapshot(*op.newColumn)

		statements, err := sqld.AddColumnSQL(dialect.dialect, op.table, spec)
		if err != nil {
			panic(err)
		}

		switch column := op.newColumn; {
		case len(statements) > 1:
			statements = append([]string{renderDDLManualComment(op.table, fmt.Sprintf(
				"%s is NOT NULL without a default; existing rows get %s", column.Name, sqld.NewColumnFill(dialect.dialect, spec)))}, statements...)
		case !column.Nullable && column.Default == "" && column.Generated == "":
			statements = append([]string{renderDDLManualComment(op.table, fmt.Sprintf(
				"%s is NOT NULL without a default and its type: has no zero value TSQ knows, so this fails on a table with rows; "+
					"add the column with a DEFAULT of your own and drop the default afterwards, or let the field hold NULL", column.Name))}, statements...)
		}

		return statements

	case ddlChangeDropColumn:
		return destructive(op.table, "drops column "+op.oldColumn.Name+" and its data", fmt.Sprintf(
			"ALTER TABLE %s DROP COLUMN %s;",
			dialect.dialect.QuoteIdent(op.table),
			dialect.dialect.QuoteIdent(op.oldColumn.Name),
		))
	case ddlChangeAlterColumn:
		return renderDDLAlterColumnStatements(dialect, op.table, *op.oldColumn, *op.newColumn)
	case ddlChangeRespellColumn:
		return renderDDLRespellColumnStatements(dialect, op)
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

	// A declaration that changed (a size on a []byte) can spell the same type on a
	// dialect (BYTEA): MySQL copied the whole table to change nothing, and PostgreSQL
	// asked for a manual change.
	if beforeSpec, afterSpec := ddlColumnSpecFromSnapshot(before), ddlColumnSpecFromSnapshot(after); before.Nullable == after.Nullable &&
		before.Default == after.Default && dialect.dialect.ColumnTypeSQL(beforeSpec.Type) == dialect.dialect.ColumnTypeSQL(afterSpec.Type) {
		return []string{renderDDLManualComment(tableName, fmt.Sprintf(
			"column %s is declared differently but %s spells its type the same; nothing to run", after.Name, ddlDialectName(dialect)))}
	}

	if dialect.dialect.AlterMode() != sqld.AlterInPlace {
		if sqliteAlterUnenforced(before, after) {
			return []string{renderDDLManualComment(tableName, fmt.Sprintf(
				"column %s changes only its declared type within one SQLite type affinity, which SQLite does not enforce; nothing to run", after.Name))}
		}

		return []string{renderDDLManualComment(tableName, fmt.Sprintf("manual change required for column %s on %s", after.Name, ddlDialectName(dialect)))}
	}

	beforeColumn, afterSpec := sqld.Column{ColumnSpec: ddlColumnSpecFromSnapshot(before)}, ddlColumnSpecFromSnapshot(after)
	// The table the migration meets is the one the earlier declaration created,
	// range constraint included; one from before those were written gets it from
	// the runtime's Reconcile, or by hand.
	beforeColumn.Check, _ = sqld.RangeCheck(dialect.dialect, beforeColumn.ColumnSpec)

	statements := dialect.dialect.AlterColumnSQL(tableName, beforeColumn, afterSpec)
	if len(statements) == 0 {
		return []string{renderDDLManualComment(tableName, fmt.Sprintf("manual change required for column %s", after.Name))}
	}

	// The same note the SQLite rebuild writes: the NULLs of a column that becomes
	// NOT NULL are filled first, and whoever runs the migration should know with what.
	// A raw type has no zero value TSQ knows, so there the statement is written as
	// it is and the note says what stops it.
	if before.Nullable && !after.Nullable {
		if fill := sqld.NullFill(dialect.dialect, beforeColumn, afterSpec); fill != "" {
			statements = append([]string{renderDDLManualComment(tableName, fmt.Sprintf(
				"%s becomes NOT NULL; rows holding NULL get %s", after.Name, fill))}, statements...)
		} else if after.Default == "" {
			statements = append([]string{renderDDLManualComment(tableName, fmt.Sprintf(
				"%s becomes NOT NULL and has no zero value TSQ knows for type:%s; fill the rows holding NULL first, or the change is refused", after.Name, after.RawType))}, statements...)
		}
	}

	return statements
}

// renderDDLRespellColumnStatements alters a column from the spelling the files
// had to the one TSQ writes now, the declaration being the same: the earlier
// spelling stands in for the live column, as an inspected one does for the
// runtime's Reconcile. SQLite rebuilds for a default or a range constraint
// (ddlChangesRequireTableRebuild) and has nothing to run for a type spelled
// otherwise within one affinity.
func renderDDLRespellColumnStatements(dialect ddlDialectSpec, op ddlChange) []string {
	note := renderDDLManualComment(op.table, fmt.Sprintf("%s: TSQ now spells its %s otherwise (was %s)",
		op.newColumn.Name, op.respelled, describeDDLRendering(*op.wasSpelled)))

	if dialect.dialect.AlterMode() != sqld.AlterInPlace {
		return []string{renderDDLManualComment(op.table, fmt.Sprintf(
			"column %s is spelled otherwise by this version of TSQ, within one SQLite type affinity, which SQLite does not enforce; nothing to run", op.newColumn.Name))}
	}

	after := ddlColumnSpecFromSnapshot(*op.newColumn)
	was := after
	was.Type = tsqdialect.ColumnType{RawType: op.wasSpelled.Type, Nullable: after.Type.Nullable}
	was.Default = op.wasSpelled.Default
	before := sqld.Column{ColumnSpec: was, NativeType: op.wasSpelled.Type, Check: op.wasSpelled.Check}

	statements := dialect.dialect.AlterColumnSQL(op.table, before, after)
	if len(statements) == 0 {
		return []string{renderDDLManualComment(op.table, fmt.Sprintf("manual change required for column %s", op.newColumn.Name))}
	}

	return append([]string{note}, statements...)
}

// describeDDLRendering is a rendering in one line, for a comment.
func describeDDLRendering(r ddlColumnRendering) string {
	parts := []string{r.Type}

	if r.Default != "" {
		parts = append(parts, "DEFAULT "+r.Default)
	}

	if r.Check != "" {
		parts = append(parts, "CHECK ("+r.Check+")")
	}

	return strings.Join(parts, " ")
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
