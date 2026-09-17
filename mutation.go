package tsq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// UpdateTable starts an UPDATE of every row of table that matches Where. On a table
// with a deleted_at column, deleted rows are left alone; pass table.WithDeleted() to
// include them.
//
// It does not check the version column, but it increments it, so a row loaded
// before the update fails its own Update with OptimisticLockError. Use
// TableOf.Update to write one row under the version check.
func UpdateTable[R any](table *TableOf[R]) *UpdateBuilder[R] {
	return &UpdateBuilder[R]{m: mutationSpec[R]{table: table, kind: mutationUpdate}}
}

// DeleteFrom starts a delete of every row of table that matches Where. On a table
// with a deleted_at column it is a soft delete, stamped when the statement runs, of
// rows not already deleted.
func DeleteFrom[R any](table *TableOf[R]) *DeleteBuilder[R] {
	kind := mutationDelete
	if table != nil && table.def.managed.DeletedAt != "" && !table.includeDeleted {
		kind = mutationSoftDelete
	}

	return &DeleteBuilder[R]{m: mutationSpec[R]{table: table, kind: kind}}
}

// HardDeleteFrom starts a DELETE of every row of table that matches Where,
// deleted rows included.
func HardDeleteFrom[R any](table *TableOf[R]) *DeleteBuilder[R] {
	return &DeleteBuilder[R]{m: mutationSpec[R]{table: table, kind: mutationDelete}}
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

type mutationSpec[R any] struct {
	table   *TableOf[R]
	kind    mutationKind
	assigns []assignment
	filters []Condition
	err     error
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

// UpdateBuilder collects the assignments of an UPDATE. Its Set methods are generic,
// which interface methods cannot be, so it is a concrete type.
type UpdateBuilder[R any] struct {
	m mutationSpec[R]
}

func (b *UpdateBuilder[R]) assign(col SQLColumn, value exprInfo) *UpdateBuilder[R] {
	n := &UpdateBuilder[R]{m: b.m.clone()}

	core := col.core()
	switch {
	case core.err() != nil:
		n.m.fail(core.err())
	case isNilValue(core.table) || core.table.definition() != b.m.table.def || core.table.Name() != b.m.table.Name() || !core.plain:
		n.m.fail(fmt.Errorf("assignment target %s must be a column of %s", core.name, b.m.table.Name()))
	case core.name == b.m.table.def.managed.Version:
		n.m.fail(fmt.Errorf("column %s is the version column; it is incremented automatically", core.name))
	case !core.nullable && value.null.always:
		n.m.fail(fmt.Errorf("column %s is NOT NULL, but the value assigned to it can be NULL", core.name))
	}

	for _, a := range n.m.assigns {
		if a.column == core.name {
			n.m.fail(fmt.Errorf("column %s is assigned twice", core.name))
		}
	}

	n.m.assigns = append(n.m.assigns, assignment{column: core.name, value: value})

	return n
}

// Set assigns rhs, a column, Param, Val or typed subquery, to col. A NOT NULL
// column refuses a value that can be NULL, such as a nullable column or a
// subquery; wrap it in Coalesce.
func (b *UpdateBuilder[R]) Set[T any](col TypedColumn[R, T], rhs RHS[T]) *UpdateBuilder[R] {
	return b.assign(col, rhsInfo(rhs))
}

// SetNull assigns NULL to col, which must be a NullColumn.
func (b *UpdateBuilder[R]) SetNull[T any](col NullColumn[R, T]) *UpdateBuilder[R] {
	return b.assign(col, exprInfo{sql: sqlText("NULL"), null: nullness{always: true}})
}

// Where limits the update. A statement has exactly one WHERE; to update every row,
// say so with Where(tsq.And()).
func (b *UpdateBuilder[R]) Where(conds ...Condition) MutationStage[R] {
	return where(b.m, conds)
}

// DeleteBuilder is a delete waiting for its WHERE clause.
type DeleteBuilder[R any] struct {
	m mutationSpec[R]
}

// Where limits the delete. To delete every row, say so with Where(tsq.And()).
func (b *DeleteBuilder[R]) Where(conds ...Condition) MutationStage[R] {
	return where(b.m, conds)
}

func where[R any](m mutationSpec[R], conds []Condition) MutationStage[R] {
	m = m.clone()
	if len(conds) == 0 {
		m.fail(errors.New("where requires at least one condition; use And() to match every row"))
	}

	m.filters = append(m.filters, conds...)

	return mutationStage[R]{m: m}
}

// MutationStage is an UPDATE or DELETE ready to build or run.
type MutationStage[R any] interface {
	Build() (*Mutation[R], error)
	MustBuild() *Mutation[R]
	Exec(ctx context.Context, db Executor, args ...Arg) (int64, error)
}

type mutationStage[R any] struct {
	m mutationSpec[R]
}

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

	if m.kind == mutationUpdate && len(m.assigns) == 0 {
		return nil, errors.New("update requires at least one assignment")
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
			if t.definition() != m.table.def || name != m.table.Name() {
				return nil, fmt.Errorf("the statement on %s cannot reference %s", m.table.Name(), name)
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
	def := m.m.table.def

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

func (m *Mutation[R]) statement(d tsqdialect.Dialect) (*statement, error) {
	if cached, ok := m.cache.Load(d.Name()); ok {
		return cached.(*statement), nil
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
func (m *Mutation[R]) SQL(dialect tsqdialect.Dialect, args ...Arg) (string, []any, error) {
	if isNilValue(dialect) {
		return "", nil, errors.New("dialect cannot be nil")
	}

	return m.prepare(WrapExecutor(noopExecutor{}, dialect), args)
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

	if m.m.kind == mutationSoftDelete {
		stamp, err := m.m.table.tombstoneValues(time.Now())
		if err != nil {
			return "", nil, err
		}

		builtin = map[*paramSpec]any{deletedAtParam: stamp[m.m.table.def.managed.DeletedAt]}
		if name := m.m.table.def.managed.UpdatedAt; name != "" {
			builtin[updatedAtParam] = stamp[name]
		}
	}

	bound, err := bindArgs(stmt.params(), args, builtin)
	if err != nil {
		return "", nil, err
	}

	return stmt.assemble(scope.dialect, bound)
}

// Exec runs the statement and returns the number of rows it changed.
func (m *Mutation[R]) Exec(ctx context.Context, db Executor, args ...Arg) (int64, error) {
	return traceExecutor1(ctx, db, TraceOpExec, func(ctx context.Context) (int64, error) {
		sqlText, sqlArgs, err := m.prepare(db, args)
		if err != nil {
			return 0, err
		}

		logSQLForExecutor(ctx, db, "exec", sqlText, sqlArgs)

		result, err := db.ExecContext(ctx, sqlText, sqlArgs...)
		if err != nil {
			return 0, fmt.Errorf("exec on %s: %w", m.m.table.Name(), err)
		}

		return result.RowsAffected()
	})
}
