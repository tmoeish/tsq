package tsq

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// Table is a query source: a table declared with NewTable, an alias of one, or a
// CTE. Only TSQ implements it.
type Table interface {
	// TableName returns the name queries use to refer to the source: the alias of
	// an aliased table, otherwise its own name. It is not called Name so that a
	// generated table struct can have a column field called Name.
	TableName() string

	// source renders the FROM/JOIN item.
	source() sqlExpr
	// definition returns the physical table behind the source, or nil for a CTE.
	definition() *tableDef
	// cteBody returns the query a CTE stands for, or nil.
	cteBody() cteQuery
	// hasColumn reports whether the source exposes a column of that name.
	hasColumn(name string) bool
	// softDeleted reports whether queries must leave out the source's deleted rows.
	softDeleted() bool
}

// managedColumns names the columns TSQ maintains on the caller's behalf. An empty
// name means the table does not declare that role.
type managedColumns struct {
	Version   string // Version is the optimistic-lock column.
	CreatedAt string // CreatedAt is set once, when the row is inserted.
	UpdatedAt string // UpdatedAt is refreshed by every update, including soft deletes.
	DeletedAt string // DeletedAt carries the soft-delete tombstone; when set, Delete is a soft delete.
}

// TableIndex declares one physical index of a table.
type TableIndex struct {
	Name   string   // Name is the physical index name.
	Fields []string // Fields lists the indexed column names in order.
	Unique bool     // Unique reports whether the index enforces uniqueness.
	// FullText marks a full-text index, which tsq.Matches searches. Where the
	// dialect has none (SQLite), nothing is created and Matches falls back to a
	// substring match.
	FullText bool
}

// cloneTableIndex copies an index, fields included, so that adding a field to
// TableIndex cannot be forgotten here.
func cloneTableIndex(index TableIndex) TableIndex {
	index.Fields = slices.Clone(index.Fields)

	return index
}

// tableDef is the untyped description of a physical table.
type tableDef struct {
	name          string
	columns       []*columnCore
	byName        map[string]*columnCore
	primaryKey    *columnCore
	autoIncrement bool
	managed       managedColumns
	search        []SearchColumn
	schema        []tsqdialect.ColumnSpec
	indexes       []TableIndex
	// tombstoneIsZero says a live row has deleted_at = 0 (integer tombstones)
	// rather than deleted_at IS NULL.
	tombstoneIsZero bool
	defined         bool
	err             error
}

func (d *tableDef) column(name string) *columnCore {
	if d == nil || name == "" {
		return nil
	}

	return d.byName[name]
}

// TableOf is the descriptor of a table whose rows are R and whose primary key is
// a K.
//
// Generated code wraps it in a struct with one field per column, built by one
// function that creates the table, then its columns, then defines it:
//
//	type CourseTable struct {
//		*tsq.TableOf[Course, int64]
//		ID    tsq.Column[Course, int64]
//		Title tsq.Column[Course, string]
//	}
//
//	var TableCourse = newCourseTable()
//
// Every query that names TableCourse.Title depends on TableCourse, so package
// initialization completes the table before any query uses it.
type TableOf[R any, K comparable] struct {
	def            *tableDef
	keys           *tableKeys[R, K]
	includeDeleted bool
	alias          string
}

// tableKeys is the typed state every copy of a table shares: the primary key and
// the queries built from it.
type tableKeys[R any, K comparable] struct {
	pk   Column[R, K]
	get  [2]lazyQuery[R]
	all  [2]lazyQuery[R]
	list [2]lazyQuery[R]
}

// TableSpec is the definition of a table.
type TableSpec[R any, K comparable] struct {
	// Columns lists every column of the table, in declaration order.
	Columns []BoundColumn[R]
	// PrimaryKey is the single primary-key column.
	PrimaryKey Column[R, K]
	// AutoIncrement reports whether the database generates the primary key.
	AutoIncrement bool
	// Version, CreatedAt, UpdatedAt and DeletedAt are the managed columns, or nil.
	Version   BoundColumn[R]
	CreatedAt BoundColumn[R]
	UpdatedAt BoundColumn[R]
	DeletedAt BoundColumn[R]
	// Search lists the columns keyword search matches against.
	Search []SearchColumn
	// Schema is the physical column definition, used by the schema policies.
	Schema []tsqdialect.ColumnSpec
	// Indexes are the declared indexes, used by the schema policies.
	Indexes []TableIndex
}

// NewTable starts the declaration of a table named name. The table is unusable
// until Define completes it.
func NewTable[R any, K comparable](name string) *TableOf[R, K] {
	t := &TableOf[R, K]{def: &tableDef{name: name}, keys: &tableKeys[R, K]{}}
	if err := validateBuiltInIdentifier(name); err != nil {
		t.def.err = fmt.Errorf("table name: %w", err)
	}

	return t
}

// Define completes the table and returns it. A definition error is reported by
// every query and write that uses the table.
func (t *TableOf[R, K]) Define(spec TableSpec[R, K]) *TableOf[R, K] {
	d := t.def
	if d.defined {
		d.err = errors.Join(d.err, fmt.Errorf("table %s is defined twice", d.name))
		return t
	}

	d.defined = true
	d.byName = make(map[string]*columnCore, len(spec.Columns))

	fail := func(format string, args ...any) {
		d.err = errors.Join(d.err, fmt.Errorf("table %s: "+format, append([]any{d.name}, args...)...))
	}

	own := func(role string, col SQLColumn) *columnCore {
		if isNilValue(col) {
			return nil
		}

		core := col.core()
		if core.err() != nil {
			fail("%s: %v", role, core.err())
			return nil
		}

		if isNilValue(core.table) || core.table.definition() != d || !core.plain {
			fail("%s %s is not a column of this table", role, core.name)
			return nil
		}

		return core
	}

	fill := make(map[string]tsqdialect.Fill, len(spec.Schema))
	for _, column := range spec.Schema {
		fill[column.Name] = column.Fill
	}

	for _, col := range spec.Columns {
		core := own("column", col)
		if core == nil {
			continue
		}

		core.fill = fill[core.name]

		if _, dup := d.byName[core.name]; dup {
			fail("column %s is declared twice", core.name)
			continue
		}

		d.columns = append(d.columns, core)
		d.byName[core.name] = core
	}

	if len(d.columns) == 0 {
		fail("no columns")
	}

	registered := func(role string, col SQLColumn) string {
		core := own(role, col)
		if core == nil {
			return ""
		}

		if d.byName[core.name] != core {
			fail("%s %s is not in Columns", role, core.name)
			return ""
		}

		return core.name
	}

	if isNilValue(spec.PrimaryKey) {
		fail("no primary key")
	} else if name := registered("primary key", spec.PrimaryKey); name != "" {
		d.primaryKey = d.byName[name]
		t.keys.pk = spec.PrimaryKey
	}

	d.autoIncrement = spec.AutoIncrement

	if col := spec.DeletedAt; !isNilValue(col) && col.core().scan != nil {
		switch reflect.ValueOf(col.core().scan(new(R))).Elem().Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			d.tombstoneIsZero = true
		}
	}

	d.managed = managedColumns{
		Version:   registered("version column", spec.Version),
		CreatedAt: registered("created_at column", spec.CreatedAt),
		UpdatedAt: registered("updated_at column", spec.UpdatedAt),
		DeletedAt: registered("deleted_at column", spec.DeletedAt),
	}

	for _, col := range spec.Search {
		if registered("search column", col) != "" {
			d.search = append(d.search, col)
		}
	}

	seen := make(map[string]bool, len(spec.Schema))
	for _, column := range spec.Schema {
		if d.byName[column.Name] == nil {
			fail("schema column %s is not in Columns", column.Name)
		}

		if seen[column.Name] {
			fail("schema column %s is declared twice", column.Name)
		}

		seen[column.Name] = true
	}

	d.schema = slices.Clone(spec.Schema)

	for _, index := range spec.Indexes {
		if err := validateBuiltInIdentifier(index.Name); err != nil {
			fail("index name: %v", err)
		}

		if len(index.Fields) == 0 {
			fail("index %s has no fields", index.Name)
		}

		for _, field := range index.Fields {
			if d.byName[field] == nil {
				fail("index %s references unknown column %s", index.Name, field)
			}
		}

		d.indexes = append(d.indexes, cloneTableIndex(index))
	}

	return t
}

// TableName returns the name queries use for the table: its alias, if it has
// one, otherwise its own name.
func (t *TableOf[R, K]) TableName() string {
	if t.alias != "" {
		return t.alias
	}

	return t.def.name
}

// Columns returns every column of the table, in declaration order, bound to
// this table (to its alias, for an aliased table).
func (t *TableOf[R, K]) Columns() []BoundColumn[R] {
	result := make([]BoundColumn[R], 0, len(t.def.columns))
	for _, core := range t.def.columns {
		if t.alias != "" {
			core = rebind(core, t)
		}

		result = append(result, columnImpl[R, any]{exprImpl[any]{c: core}})
	}

	return result
}

// FullText returns the table's full-text index, or the one named. Pass it to
// tsq.Matches to search it.
func (t *TableOf[R, K]) FullText(name ...string) FullTextIndex {
	if err := t.Err(); err != nil {
		return FullTextIndex{err: err}
	}

	var found []TableIndex

	for _, index := range t.def.indexes {
		if index.FullText && (len(name) == 0 || index.Name == name[0]) {
			found = append(found, index)
		}
	}

	switch {
	case len(found) == 0 && len(name) > 0:
		return FullTextIndex{err: fmt.Errorf("table %s has no full-text index named %s", t.def.name, name[0])}
	case len(found) == 0:
		return FullTextIndex{err: fmt.Errorf("table %s declares no full-text index; add //tsq:fulltext", t.def.name)}
	case len(found) > 1:
		return FullTextIndex{err: fmt.Errorf("table %s has %d full-text indexes; name the one to search", t.def.name, len(found))}
	}

	return FullTextIndex{table: t, index: found[0]}
}

// SearchColumns returns the columns keyword search matches against.
func (t *TableOf[R, K]) SearchColumns() []SearchColumn { return slices.Clone(t.def.search) }

// Schema returns the declared physical columns.
func (t *TableOf[R, K]) Schema() []tsqdialect.ColumnSpec { return slices.Clone(t.def.schema) }

// Indexes returns the declared indexes.
func (t *TableOf[R, K]) Indexes() []TableIndex {
	result := make([]TableIndex, 0, len(t.def.indexes))
	for _, index := range t.def.indexes {
		result = append(result, cloneTableIndex(index))
	}

	return result
}

// WithDeleted returns the table without its soft-delete scope. A table with a
// deleted_at column leaves deleted rows out of every query and of UpdateTable and
// DeleteFrom; select from, join or update WithDeleted() to include them. Its
// columns are the table's columns.
func (t *TableOf[R, K]) WithDeleted() *TableOf[R, K] {
	next := *t
	next.includeDeleted = true

	return &next
}

// As returns the table under an alias, for joining it more than once. Its
// Columns are bound to the alias; bind a single column with Column.WithTable.
// An empty alias, or the table's own name, returns the table unaliased.
func (t *TableOf[R, K]) As(alias string) *TableOf[R, K] {
	next := *t

	next.alias = strings.TrimSpace(alias)
	if next.alias == t.def.name {
		next.alias = ""
	}

	return &next
}

// Err reports why the table definition is invalid, or nil.
func (t *TableOf[R, K]) Err() error {
	if !t.def.defined {
		return errors.Join(t.def.err, fmt.Errorf("table %s is used before Define", t.def.name))
	}

	return t.def.err
}

func (t *TableOf[R, K]) source() sqlExpr {
	if t.alias != "" {
		return sqlJoin(sqlIdent(t.def.name), sqlText(" AS "), sqlIdent(t.alias))
	}

	return sqlIdent(t.def.name)
}

func (t *TableOf[R, K]) aliased() bool           { return t.alias != "" }
func (t *TableOf[R, K]) rowType(R)               {}
func (t *TableOf[R, K]) definition() *tableDef   { return t.def }
func (t *TableOf[R, K]) cteBody() cteQuery       { return nil }
func (t *TableOf[R, K]) hasColumn(n string) bool { return t.def.byName[n] != nil }
func (t *TableOf[R, K]) softDeleted() bool {
	return !t.includeDeleted && t.def.managed.DeletedAt != ""
}

// ready returns the definition or the reason it cannot be used.
func (t *TableOf[R, K]) ready() (*tableDef, error) {
	if err := t.Err(); err != nil {
		return nil, err
	}

	return t.def, nil
}

// liveRows renders the condition that keeps table's live rows, for a table that
// is softDeleted.
func liveRows(table Table) sqlExpr {
	def := table.definition()
	op := " IS NULL"

	if def.tombstoneIsZero {
		op = " = 0"
	}

	return sqlJoin(columnRef(table, def.managed.DeletedAt), sqlText(op))
}

// liveSource renders table as a derived table holding only its live rows, for
// joins where a condition in WHERE or ON would not filter the right rows.
func liveSource(table Table) sqlExpr {
	def := table.definition()
	inner := &TableOf[struct{}, int]{def: def}

	return sqlJoin(
		sqlText("(SELECT * FROM "), sqlIdent(def.name), sqlText(" WHERE "), liveRows(inner),
		sqlText(") AS "), sqlIdent(table.TableName()),
	)
}

// tableRef renders the name a query uses for table.
func tableRef(table Table) sqlExpr { return sqlIdent(table.TableName()) }

func tableHasColumn(table Table, name string) bool {
	return !isNilValue(table) && table.hasColumn(name)
}

// tableErr reports a table that cannot be used in a query.
func tableErr(table Table) error {
	if isNilValue(table) {
		return errors.New("table cannot be nil")
	}

	if def := table.definition(); def != nil {
		if !def.defined {
			return fmt.Errorf("table %s is used before Define", def.name)
		}

		return def.err
	}

	if body := table.cteBody(); body != nil {
		return body.err()
	}

	return nil
}

// cteQuery is the type-erased query behind a CTE.
type cteQuery interface {
	queryRenderer
	err() error
	outputNames() []string
	sources() []Table
	// nullableOutput reports whether the named output column can be NULL.
	nullableOutput(name string) bool
}

type cteTable struct {
	name string
	body cteQuery
}

// CTE declares a non-recursive common table expression named name. Columns of the
// query are referenced through it with Column.WithTable.
func CTE[O any](name string, query QueryStage[O]) Table {
	name = strings.TrimSpace(name)

	spec, err := stageSpec(query)
	if err == nil && name == "" {
		err = errors.New("cte name cannot be empty")
	}

	if err == nil {
		err = validateBuiltInIdentifier(name)
	}

	return cteTable{name: name, body: &cteSpec[O]{spec: spec, buildErr: err}}
}

func (c cteTable) TableName() string     { return c.name }
func (c cteTable) source() sqlExpr       { return sqlIdent(c.name) }
func (c cteTable) definition() *tableDef { return nil }
func (c cteTable) cteBody() cteQuery     { return c.body }
func (c cteTable) softDeleted() bool     { return false }

func (c cteTable) hasColumn(n string) bool {
	return slices.Contains(c.body.outputNames(), n)
}

// debugSQL renders a fragment in SQLite syntax with ? placeholders, for String
// methods and error messages.
func debugSQL(e sqlExpr) string {
	r := newRenderer(sqld.SQLiteDialect{})
	r.write(e)

	return debugStatement(r)
}

// debugStatement finishes r and prints parameters by name.
func debugStatement(r *renderer) string {
	stmt, err := r.finish()
	if err != nil {
		return "<invalid: " + err.Error() + ">"
	}

	var b strings.Builder

	for _, c := range stmt.chunks {
		switch {
		case c.hasValue:
			b.WriteString("?")
		case c.param != nil:
			b.WriteString(":" + c.param.label())
		default:
			b.WriteString(c.text)
		}
	}

	return b.String()
}
