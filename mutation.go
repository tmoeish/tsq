package tsq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// UpdateTable starts an UPDATE of every row of table that matches Where. On a table
// with a deleted_at column, deleted rows are left alone; pass table.WithDeleted() to
// include them. updated_at is refreshed when the statement runs, unless Set assigns
// it.
//
// It does not check the version column, but it increments it, so a row loaded
// before the update fails its own Update with OptimisticLockError. Use
// TableOf.Update to write one row under the version check.
func UpdateTable[R any](table RowTable[R]) *UpdateStage[R] {
	return &UpdateStage[R]{m: newMutationSpec(table, mutationUpdate)}
}

// DeleteFrom starts a soft delete of every row of table that matches Where: an
// UPDATE that stamps the tombstone when the statement runs, of rows not already
// deleted unless table is WithDeleted. It takes only a table with a deleted_at
// column; HardDeleteFrom removes rows.
//
// T is a type parameter rather than the interface itself so that passing a table
// without deleted_at fails to compile naming the missing method,
// needsDeletedAtOrHardDeleteFrom, instead of reporting that R cannot be inferred.
func DeleteFrom[T SoftDeleteTable[R], R any](table T) DeleteStage[R] {
	return &deleteBuilder[R]{m: newMutationSpec[R](table, mutationSoftDelete)}
}

// HardDeleteFrom starts a DELETE of every row of table that matches Where,
// deleted rows included.
func HardDeleteFrom[R any](table RowTable[R]) DeleteStage[R] {
	return &deleteBuilder[R]{m: newMutationSpec(table, mutationDelete)}
}

type mutationKind uint8

const (
	mutationUpdate mutationKind = iota
	mutationDelete
	mutationSoftDelete
)

type assignment struct {
	column string
	value  exprInfo
}

// RowTable is a table whose rows are R: a *TableOf, or a generated table struct,
// which embeds one. Only TSQ implements it.
type RowTable[R any] interface {
	writeTarget
	rowType(R)
}

// writeTarget is what a statement by condition needs from its table, without the
// key type, which the statement stages do not carry.
type writeTarget interface {
	Table
	Err() error
	updatedAtValue(now time.Time) (any, error)
	tombstoneValues(now time.Time) (map[string]any, error)
	aliased() bool
}

type mutationSpec[R any] struct {
	table   writeTarget
	def     *tableDef
	kind    mutationKind
	assigns []assignment
	filters []Condition
	err     error
}

func newMutationSpec[R any](table RowTable[R], kind mutationKind) mutationSpec[R] {
	if isNilValue(table) {
		return mutationSpec[R]{kind: kind, err: errors.New("table cannot be nil")}
	}

	m := mutationSpec[R]{table: table, def: table.definition(), kind: kind}
	if table.aliased() {
		m.err = fmt.Errorf("a statement by condition writes %s itself, not an alias of it", m.def.name)
	}

	return m
}

func (m mutationSpec[R]) clone() mutationSpec[R] {
	m.assigns = slices.Clone(m.assigns)
	m.filters = slices.Clone(m.filters)

	return m
}

func (m *mutationSpec[R]) fail(err error) {
	if m.err == nil {
		m.err = err
	}
}

// UpdateStage is an UPDATE without assignments: Set or SetNull comes first, so an
// UPDATE that sets nothing does not compile. Set is a generic method, which
// interface methods cannot be, so the stage is a concrete type.
type UpdateStage[R any] struct {
	m mutationSpec[R]
}

// Set assigns rhs, a column, Param, Val or typed subquery, to col. A NOT NULL
// column refuses a value that can be NULL, such as a nullable column or a
// subquery; wrap it in Coalesce.
func (b *UpdateStage[R]) Set[T any](col Column[R, T], rhs Operand[T]) *SetStage[R] {
	return assign(b.m, col, fitted(col, rhs, rhsInfo(rhs)))
}

// SetNull assigns NULL to col, which must be a NullColumn.
func (b *UpdateStage[R]) SetNull[T any](col NullColumn[R, T]) *SetStage[R] {
	return assign(b.m, col, nullValue)
}

// SetStage is an UPDATE with assignments: more Set calls, then Where.
type SetStage[R any] struct {
	m mutationSpec[R]
}

var nullValue = exprInfo{sql: sqlText("NULL"), null: nullness{always: true}}

func assign[R any](m mutationSpec[R], col SQLColumn, value exprInfo) *SetStage[R] {
	n := &SetStage[R]{m: m.clone()}

	// A nil table or column is a build error, as everywhere else in the builder,
	// not a nil dereference here.
	switch {
	case n.m.err != nil:
		return n
	case n.m.def == nil:
		n.m.fail(errors.New("update is not started; start it with tsq.UpdateTable(table)"))
		return n
	case isNilValue(col):
		n.m.fail(errors.New("assignment target cannot be nil"))
		return n
	}

	core := col.core()
	switch {
	case core.err() != nil:
		n.m.fail(core.err())
	case isNilValue(core.table) || core.table.definition() != m.def || core.table.TableName() != m.table.TableName() || !core.plain:
		n.m.fail(fmt.Errorf("assignment target %s must be a column of %s", core.name, m.table.TableName()))
	case core.name == m.def.managed.Version:
		n.m.fail(fmt.Errorf("column %s is the version column; it is incremented automatically", core.name))
	case !core.nullable && value.null.always:
		n.m.fail(fmt.Errorf("column %s is NOT NULL, but the value assigned to it can be NULL", core.name))
	}

	for _, a := range n.m.assigns {
		if a.column == core.name {
			n.m.fail(fmt.Errorf("column %s is assigned twice", core.name))
		}

		// MySQL evaluates a single-table UPDATE's assignments left to right, so a
		// later one reads the value an earlier one just wrote; PostgreSQL and SQLite
		// read the row as it was. Swapping two columns gave different rows.
		if slices.Contains(value.bare, columnKey{m.table.TableName(), a.column}) {
			n.m.fail(fmt.Errorf("the value assigned to %s reads %s, which the statement assigns before it; "+
				"MySQL would read the new value and the other dialects the old one", core.name, a.column))
		}
	}

	n.m.assigns = append(n.m.assigns, assignment{column: core.name, value: value})

	return n
}

// Set assigns rhs to col as UpdateStage.Set does.
func (b *SetStage[R]) Set[T any](col Column[R, T], rhs Operand[T]) *SetStage[R] {
	return assign(b.m, col, fitted(col, rhs, rhsInfo(rhs)))
}

// fitted marks a value or parameter assigned to col as written to it, so that it
// is held to the column (fitValue) as a row's values are. Any other right-hand
// side is the engine's to compute.
func fitted[T any](col SQLColumn, rhs Operand[T], info exprInfo) exprInfo {
	if info.err != nil || isNilValue(col) || col.core() == nil {
		return info
	}

	switch rhs := rhs.(type) {
	case Value[T]:
		info.sql = sqlFitValue(rhs.v, col.core().fit())
	case Param[T]:
		info.sql = sqlFitParam(rhs.spec, col.core().fit())
	}

	return info
}

// SetNull assigns NULL to col, which must be a NullColumn.
func (b *SetStage[R]) SetNull[T any](col NullColumn[R, T]) *SetStage[R] {
	return assign(b.m, col, nullValue)
}

// Where limits the update; its conditions are ANDed. A statement has exactly one
// WHERE; to update every row, say so with Where(tsq.And()).
func (b *SetStage[R]) Where(cond Condition, more ...Condition) MutationStage[R] {
	return where(b.m, list(cond, more))
}

// DeleteStage is a delete waiting for its WHERE clause, which is required.
type DeleteStage[R any] interface {
	sealedStage()

	// Where limits the delete; its conditions are ANDed. To delete every row, say
	// so with Where(tsq.And()).
	Where(cond Condition, more ...Condition) MutationStage[R]
}

type deleteBuilder[R any] struct {
	m mutationSpec[R]
}

func (*deleteBuilder[R]) sealedStage() {}

func (b *deleteBuilder[R]) Where(cond Condition, more ...Condition) MutationStage[R] {
	return where(b.m, list(cond, more))
}

func where[R any](m mutationSpec[R], conds []Condition) MutationStage[R] {
	m = m.clone()
	m.filters = append(m.filters, conds...)

	return mutationStage[R]{m: m}
}

// MutationStage is an UPDATE or DELETE ready to build or run.
type MutationStage[R any] interface {
	sealedStage()

	Build() (*Mutation[R], error)
	MustBuild() *Mutation[R]
	Exec(ctx context.Context, db Executor, args ...Arg) (int64, error)
}

type mutationStage[R any] struct {
	m mutationSpec[R]
}

func (mutationStage[R]) sealedStage() {}

func (s mutationStage[R]) Build() (*Mutation[R], error) {
	m := s.m
	if m.err != nil {
		return nil, m.err
	}

	if m.table == nil {
		return nil, errors.New("mutation table cannot be nil")
	}

	if err := m.table.Err(); err != nil {
		return nil, err
	}

	infos := make([]exprInfo, 0, len(m.assigns)+len(m.filters))
	for _, a := range m.assigns {
		infos = append(infos, a.value)
	}

	for _, c := range m.filters {
		infos = append(infos, conditionInfo(c))
	}

	// UPDATE ... FROM and multi-table DELETE are spelled differently on every
	// dialect, so a statement may only reference its own table.
	for _, info := range infos {
		if info.err != nil {
			return nil, info.err
		}

		for name, t := range info.allTables() {
			if t.definition() != m.def || name != m.table.TableName() {
				return nil, fmt.Errorf("the statement on %s cannot reference %s", m.table.TableName(), name)
			}
		}
	}

	return &Mutation[R]{m: m}, nil
}

func (s mutationStage[R]) MustBuild() *Mutation[R] {
	m, err := s.Build()
	if err != nil {
		panic(err)
	}

	return m
}

func (s mutationStage[R]) Exec(ctx context.Context, db Executor, args ...Arg) (int64, error) {
	m, err := s.Build()
	if err != nil {
		return 0, err
	}

	return m.Exec(ctx, db, args...)
}

// Mutation is a built UPDATE or DELETE. It is immutable and safe for concurrent use.
type Mutation[R any] struct {
	m     mutationSpec[R]
	cache sync.Map // dialect name -> *statement
}

// deletedAtParam and updatedAtParam carry the soft-delete stamp, bound when the
// statement runs rather than when it is built, so a package-level statement does
// not stamp every row with the time the program started.
var (
	deletedAtParam = newParamSpec("deleted_at", paramScalar)
	updatedAtParam = newParamSpec("updated_at", paramScalar)
)

func (m *Mutation[R]) render(r *renderer) {
	def := m.m.def

	switch m.m.kind {
	case mutationDelete:
		r.writeText("DELETE FROM ")
		r.writeIdent(def.name)
	default:
		r.writeText("UPDATE ")
		r.writeIdent(def.name)
		r.writeText(" SET ")

		sets := make([]sqlExpr, 0, len(m.m.assigns)+3)
		for _, a := range m.m.assigns {
			sets = append(sets, sqlJoin(sqlIdent(a.column), sqlText(" = "), a.value.sql))
		}

		if m.m.kind == mutationUpdate && m.stampsUpdatedAt() {
			sets = append(sets, sqlJoin(sqlIdent(def.managed.UpdatedAt), sqlText(" = "), sqlParam(updatedAtParam)))
		}

		if m.m.kind == mutationSoftDelete {
			sets = append(sets, sqlJoin(sqlIdent(def.managed.DeletedAt), sqlText(" = "), sqlParam(deletedAtParam)))

			if def.managed.UpdatedAt != "" {
				sets = append(sets, sqlJoin(sqlIdent(def.managed.UpdatedAt), sqlText(" = "), sqlParam(updatedAtParam)))
			}
		}

		if v := def.managed.Version; v != "" {
			sets = append(sets, sqlJoin(sqlIdent(v), sqlText(" = "), sqlIdent(v), sqlText(" + 1")))
		}

		r.write(sqlList(", ", sets))
	}

	r.writeText(" WHERE ")

	where := andAll(m.m.filters).sql
	if m.m.kind != mutationDelete && m.m.table.softDeleted() {
		// UpdateTable and a soft DeleteFrom leave deleted rows alone, like queries do.
		where = sqlJoin(sqlText("("), where, sqlText(") AND "), liveRows(m.m.table))
	}

	r.write(where)
}

// stampsUpdatedAt reports that an UPDATE refreshes updated_at itself: the table has
// one and the caller did not assign it.
func (m *Mutation[R]) stampsUpdatedAt() bool {
	name := m.m.def.managed.UpdatedAt

	return name != "" && !slices.ContainsFunc(m.m.assigns, func(a assignment) bool { return a.column == name })
}

func (m *Mutation[R]) statement(d sqld.Dialect) (*statement, error) {
	if m == nil || m.m.def == nil {
		return nil, errors.New("statement is not built; make one with tsq.UpdateTable, DeleteFrom or HardDeleteFrom and Build")
	}

	if cached, ok := m.cache.Load(d.Name()); ok {
		return cached.(*statement), nil
	}

	// MySQL refuses a statement that reads, in a subquery, the table it writes
	// (error 1093); SQLite and PostgreSQL run it.
	if d.Name() == tsqdialect.MySQL && m.readsItsTable() {
		return nil, fmt.Errorf("MySQL cannot read %s in a subquery of a statement that writes it (error 1093); "+
			"read the keys first and pass them as a list", m.m.table.TableName())
	}

	r := newRenderer(d)
	m.render(r)

	stmt, err := r.finish()
	if err != nil {
		return nil, err
	}

	m.cache.Store(d.Name(), stmt)

	return stmt, nil
}

// SQL renders the statement for dialect with args bound, as it would run. A soft
// delete is rendered with the current time.
func (m *Mutation[R]) SQL(dialect tsqdialect.Name, args ...Arg) (string, []any, error) {
	exec, err := WrapExecutor(noopExecutor{}, dialect)
	if err != nil {
		return "", nil, err
	}

	return m.prepare(exec, args)
}

func (m *Mutation[R]) prepare(db Executor, args []Arg) (string, []any, error) {
	if m == nil {
		return "", nil, errors.New("mutation cannot be nil")
	}

	scope, err := executorScope(db)
	if err != nil {
		return "", nil, err
	}

	stmt, err := m.statement(scope.dialect)
	if err != nil {
		return "", nil, err
	}

	var builtin map[*paramSpec]any

	if m.m.kind == mutationUpdate && m.stampsUpdatedAt() {
		stamp, err := m.m.table.updatedAtValue(stampTime())
		if err != nil {
			return "", nil, err
		}

		builtin = map[*paramSpec]any{updatedAtParam: stamp}
	}

	if m.m.kind == mutationSoftDelete {
		stamp, err := m.m.table.tombstoneValues(stampTime())
		if err != nil {
			return "", nil, err
		}

		builtin = map[*paramSpec]any{deletedAtParam: stamp[m.m.def.managed.DeletedAt]}
		if name := m.m.def.managed.UpdatedAt; name != "" {
			builtin[updatedAtParam] = stamp[name]
		}
	}

	bound, err := bindArgs(stmt.params(), args, builtin)
	if err != nil {
		return "", nil, err
	}

	return stmt.assemble(scope.dialect, bound)
}

// Exec runs the statement and returns the number of rows it affected, as the
// driver reports it: MySQL counts only the rows whose values changed (unless the
// DSN sets clientFoundRows=true), PostgreSQL and SQLite every row matched. On a
// table with version or updated_at every matched row changes, so they agree.
func (m *Mutation[R]) Exec(ctx context.Context, db Executor, args ...Arg) (int64, error) {
	return traceExecutor1(ctx, db, m.traceInfo(), func(ctx context.Context) (int64, error) {
		sqlText, sqlArgs, err := m.prepare(db, args)
		if err != nil {
			return 0, err
		}

		logSQLForExecutor(ctx, db, string(m.traceInfo().Op), sqlText, sqlArgs)

		result, err := db.ExecContext(ctx, sqlText, sqlArgs...)
		if err != nil {
			return 0, fmt.Errorf("exec on %s: %w", m.m.table.TableName(), err)
		}

		return result.RowsAffected()
	})
}

// traceInfo names the statement for tracers: an update, a soft delete or a hard
// delete of the target table.
func (m *Mutation[R]) traceInfo() TraceInfo {
	info := TraceInfo{Op: TraceOpUpdate}

	switch {
	case m == nil:
	case m.m.kind == mutationSoftDelete:
		info.Op = TraceOpDelete
	case m.m.kind == mutationDelete:
		info.Op = TraceOpHardDelete
	}

	if m != nil && m.m.def != nil {
		info.Table = m.m.def.name
	}

	return info
}

// readsItsTable reports whether a subquery of the statement reads its table.
func (m *Mutation[R]) readsItsTable() bool {
	exprs := make([]sqlExpr, 0, len(m.m.assigns)+len(m.m.filters))
	for _, a := range m.m.assigns {
		exprs = append(exprs, a.value.sql)
	}

	for _, c := range m.m.filters {
		exprs = append(exprs, conditionInfo(c).sql)
	}

	for _, e := range exprs {
		for _, part := range e.parts {
			if part.kind == partQuery && part.query.readsTable(m.m.table.TableName()) {
				return true
			}
		}
	}

	return false
}
