package tsq

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// Table is a query source: a table declared with NewTable, an alias of one, or a
// CTE. Only TSQ implements it.
type Table interface {
	// Name returns the name queries use to refer to the source: the alias of an
	// aliased table, otherwise its own name.
	Name() string

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

// TableOf is the descriptor of a table whose rows are R.
//
// Generated code declares a table in three steps so that Go's package
// initialization order follows the dependencies it can see:
//
//	var courseTable = tsq.NewTable[Course]("course")
//	var Course_ID = tsq.NewColumn(courseTable, "id", "id", func(r *Course) *int64 { return &r.ID })
//	var TableCourse = courseTable.Define(tsq.TableSpec[Course]{Columns: ..., PrimaryKey: Course_ID})
//
// Queries use TableCourse, which depends on every column, so no query can be
// initialized before the table is complete.
type TableOf[R any] struct {
	def            *tableDef
	includeDeleted bool
}

// TableSpec is the definition of a table.
type TableSpec[R any] struct {
	// Columns lists every column of the table, in declaration order.
	Columns []BoundColumn[R]
	// PrimaryKey is the single primary-key column.
	PrimaryKey BoundColumn[R]
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
func NewTable[R any](name string) *TableOf[R] {
	t := &TableOf[R]{def: &tableDef{name: name}}
	if err := validateBuiltInIdentifier(name); err != nil {
		t.def.err = fmt.Errorf("table name: %w", err)
	}

	return t
}

// Define completes the table and returns it. A definition error is reported by
// every query and write that uses the table.
func (t *TableOf[R]) Define(spec TableSpec[R]) *TableOf[R] {
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

	for _, col := range spec.Columns {
		core := own("column", col)
		if core == nil {
			continue
		}

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

		d.indexes = append(d.indexes, TableIndex{Name: index.Name, Fields: slices.Clone(index.Fields), Unique: index.Unique})
	}

	return t
}

// Name returns the table name.
func (t *TableOf[R]) Name() string { return t.def.name }

// Columns returns every column of the table, in declaration order.
func (t *TableOf[R]) Columns() []BoundColumn[R] {
	result := make([]BoundColumn[R], 0, len(t.def.columns))
	for _, core := range t.def.columns {
		result = append(result, columnImpl[R, any]{c: core})
	}

	return result
}

// SearchColumns returns the columns keyword search matches against.
func (t *TableOf[R]) SearchColumns() []SearchColumn { return slices.Clone(t.def.search) }

// Schema returns the declared physical columns.
func (t *TableOf[R]) Schema() []tsqdialect.ColumnSpec { return slices.Clone(t.def.schema) }

// Indexes returns the declared indexes.
func (t *TableOf[R]) Indexes() []TableIndex {
	result := make([]TableIndex, 0, len(t.def.indexes))
	for _, index := range t.def.indexes {
		result = append(result, TableIndex{Name: index.Name, Fields: slices.Clone(index.Fields), Unique: index.Unique})
	}

	return result
}

// WithDeleted returns the table without its soft-delete scope. A table with a
// deleted_at column leaves deleted rows out of every query and of UpdateTable and
// DeleteFrom; select from, join or update WithDeleted() to include them. Its
// columns are the table's columns.
func (t *TableOf[R]) WithDeleted() *TableOf[R] {
	return &TableOf[R]{def: t.def, includeDeleted: true}
}

// As returns the table under an alias, for joining it more than once.
func (t *TableOf[R]) As(alias string) Table { return AliasTable(t, alias) }

// Err reports why the table definition is invalid, or nil.
func (t *TableOf[R]) Err() error {
	if !t.def.defined {
		return errors.Join(t.def.err, fmt.Errorf("table %s is used before Define", t.def.name))
	}

	return t.def.err
}

func (t *TableOf[R]) source() sqlExpr         { return sqlIdent(t.def.name) }
func (t *TableOf[R]) definition() *tableDef   { return t.def }
func (t *TableOf[R]) cteBody() cteQuery       { return nil }
func (t *TableOf[R]) hasColumn(n string) bool { return t.def.byName[n] != nil }
func (t *TableOf[R]) softDeleted() bool {
	return !t.includeDeleted && t.def.managed.DeletedAt != ""
}

// ready returns the definition or the reason it cannot be used.
func (t *TableOf[R]) ready() (*tableDef, error) {
	if err := t.Err(); err != nil {
		return nil, err
	}

	return t.def, nil
}

type aliasTable struct {
	base  Table
	alias string
}

// AliasTable returns table under alias. Columns follow with Column.As or
// Column.WithTable.
func AliasTable(table Table, alias string) Table {
	alias = strings.TrimSpace(alias)
	if isNilValue(table) || alias == "" || alias == table.Name() {
		return table
	}

	if a, ok := table.(aliasTable); ok {
		table = a.base
	}

	return aliasTable{base: table, alias: alias}
}

func (a aliasTable) Name() string { return a.alias }

func (a aliasTable) source() sqlExpr {
	return sqlJoin(a.base.source(), sqlText(" AS "), sqlIdent(a.alias))
}

func (a aliasTable) definition() *tableDef   { return a.base.definition() }
func (a aliasTable) cteBody() cteQuery       { return a.base.cteBody() }
func (a aliasTable) hasColumn(n string) bool { return a.base.hasColumn(n) }
func (a aliasTable) softDeleted() bool       { return a.base.softDeleted() }

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
	inner := &TableOf[struct{}]{def: def}

	return sqlJoin(
		sqlText("(SELECT * FROM "), sqlIdent(def.name), sqlText(" WHERE "), liveRows(inner),
		sqlText(") AS "), sqlIdent(table.Name()),
	)
}

// tableRef renders the name a query uses for table.
func tableRef(table Table) sqlExpr { return sqlIdent(table.Name()) }

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

func (c cteTable) Name() string          { return c.name }
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
	r := newRenderer(tsqdialect.SQLiteDialect{})
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
