package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// The query builder is staged: every method returns an interface that offers only
// the calls SQL allows next, so Where twice, Having without GroupBy or a JOIN after
// WHERE do not compile. One concrete builder implements the methods; the stage
// interfaces are what restrict them. The phase check inside the builder only
// catches a caller who type-asserts their way past an interface.

// QueryStage is a complete query: it can be built or run directly. Running a stage
// builds it on every call; build once and reuse the *Query on hot paths.
//
// A stage is also a Subquery of its rows, so a stage from SelectValue can stand on
// the right of a comparison or IN, and any stage can go to Exists.
type QueryStage[O any] interface {
	Subquery[O]

	Build() (*Query[O], error)
	MustBuild() *Query[O]
	Get(ctx context.Context, db Executor, args ...Arg) (*O, error)
	Find(ctx context.Context, db Executor, args ...Arg) (*O, error)
	Exists(ctx context.Context, db Executor, args ...Arg) (bool, error)
	Count(ctx context.Context, db Executor, args ...Arg) (int64, error)
	List(ctx context.Context, db Executor, args ...Arg) ([]*O, error)
	Page(ctx context.Context, db Executor, p Paging, args ...Arg) (*Page[O], error)
}

// Sortable is the part of a stage that can order and slice the result.
type Sortable[O any] interface {
	OrderBy(orders ...OrderBy) OrderedStage[O]
	Limit(limit int) OrderedStage[O]
	Offset(offset int) OrderedStage[O]
}

// ResultSortable is the part of a grouped or combined stage that can order and
// slice the result. Those rows are not rows of a table, so an ordered result
// cannot be locked: PostgreSQL refuses FOR UPDATE with GROUP BY, HAVING and set
// operations, and ordering first does not change that.
type ResultSortable[O any] interface {
	OrderBy(orders ...OrderBy) OrderedResultStage[O]
	Limit(limit int) OrderedResultStage[O]
	Offset(offset int) OrderedResultStage[O]
}

// Lockable is the part of a stage that can lock the rows it reads. Row locks only
// mean something inside a transaction.
type Lockable[O any] interface {
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
}

// Combinable is the part of a stage that can be combined with another query.
type Combinable[O any] interface {
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// Groupable is the part of a stage that can group rows.
type Groupable[O any] interface {
	GroupBy(cols ...SQLColumn) GroupedStage[O]
}

// SelectStage is a query with columns but no FROM table yet.
type SelectStage[O any] interface {
	From(table Table) JoinStage[O]
}

// JoinStage is a query that can still take joins.
type JoinStage[O any] interface {
	QueryStage[O]
	Groupable[O]
	Sortable[O]
	Lockable[O]
	Combinable[O]
	Join(table Table, on ...Condition) JoinStage[O]
	InnerJoin(table Table, on ...Condition) JoinStage[O]
	LeftJoin(table Table, on ...Condition) JoinStage[O]
	RightJoin(table Table, on ...Condition) JoinStage[O]
	FullJoin(table Table, on ...Condition) JoinStage[O]
	CrossJoin(table Table) JoinStage[O]
	// Correlate declares outer-query tables this query references without joining
	// them, which makes it a correlated subquery. Such a query only runs inside a
	// query that provides those tables.
	Correlate(tables ...Table) JoinStage[O]
	// Where sets the WHERE clause; its conditions are ANDed.
	Where(conds ...Condition) WhereStage[O]
	// Search sets the columns a Keyword argument is matched against.
	Search(cols ...SearchColumn) SearchStage[O]
}

// WhereStage is a query with a WHERE clause.
type WhereStage[O any] interface {
	QueryStage[O]
	Groupable[O]
	Sortable[O]
	Lockable[O]
	Combinable[O]
	Search(cols ...SearchColumn) FilteredStage[O]
}

// SearchStage is a query with search columns. Keyword search does not combine with
// set operations.
type SearchStage[O any] interface {
	QueryStage[O]
	Groupable[O]
	Sortable[O]
	Lockable[O]
	Where(conds ...Condition) FilteredStage[O]
}

// FilteredStage is a query with both WHERE and search columns.
type FilteredStage[O any] interface {
	QueryStage[O]
	Groupable[O]
	Sortable[O]
	Lockable[O]
}

// GroupedStage is a query with GROUP BY. Grouped rows cannot be locked.
type GroupedStage[O any] interface {
	QueryStage[O]
	ResultSortable[O]
	Combinable[O]
	Having(conds ...Condition) HavingStage[O]
}

// HavingStage is a grouped query with HAVING.
type HavingStage[O any] interface {
	QueryStage[O]
	ResultSortable[O]
	Combinable[O]
}

// CompoundStage is a query combined with others by set operations.
type CompoundStage[O any] interface {
	QueryStage[O]
	ResultSortable[O]
	Combinable[O]
}

// OrderedResultStage is a grouped or combined query with ORDER BY, LIMIT or
// OFFSET. Unlike OrderedStage it cannot be locked.
type OrderedResultStage[O any] interface {
	QueryStage[O]
	ResultSortable[O]
}

// OrderedStage is a query with ORDER BY, LIMIT or OFFSET.
type OrderedStage[O any] interface {
	QueryStage[O]
	Sortable[O]
	Lockable[O]
}

// LockedStage is a query that locks the rows it reads.
type LockedStage[O any] interface {
	QueryStage[O]
	NoWait() LockedStage[O]
	SkipLocked() LockedStage[O]
}

type stagePhase uint8

const (
	phaseJoin stagePhase = iota
	phaseFilter
	phaseGroup
	phaseHaving
	phaseCompound
	phasePaged
	phaseLocked
)

type builder[O any] struct {
	spec      querySpec[O]
	phase     stagePhase
	hasWhere  bool
	hasSearch bool
	hasFrom   bool
	hasSelect bool
	err       error
}

// Select starts a query with its columns.
func Select[O any](cols ...BoundColumn[O]) SelectStage[O] {
	b := &builder[O]{}
	b.setSelect(cols)

	return selectBuilder[O]{b}
}

// SelectDistinct starts a SELECT DISTINCT query with its columns.
func SelectDistinct[O any](cols ...BoundColumn[O]) SelectStage[O] {
	b := &builder[O]{}
	b.spec.Distinct = true
	b.setSelect(cols)

	return selectBuilder[O]{b}
}

// SelectValue starts a query that reads one expression, such as an aggregate:
// its rows are the values themselves.
//
//	total, err := tsq.SelectValue(tsq.Sum(Order_Amount)).From(Orders).MustBuild().Get(ctx, db)
//
// The value must never be NULL; use SelectNullValue where it can be, for example
// for SUM over no rows.
func SelectValue[T any](expr ValueColumn[T]) SelectStage[T] {
	return Select(MapInto(expr, func(v *T) *T { return v }))
}

// SelectNullValue is SelectValue for an expression that can be NULL; its rows are
// sql.Null[T].
func SelectNullValue[T any](expr ValueColumn[T]) SelectStage[sql.Null[T]] {
	return Select(MapIntoNull(expr, func(v *sql.Null[T]) *sql.Null[T] { return v }))
}

type selectBuilder[O any] struct{ b *builder[O] }

func (s selectBuilder[O]) From(table Table) JoinStage[O] {
	n := s.b.next()
	n.setFrom(table)

	return joinBuilder[O]{n}
}

func (b *builder[O]) setSelect(cols []BoundColumn[O]) {
	if b.hasSelect {
		b.fail(errors.New("select is already set"))
		return
	}

	b.hasSelect = true
	b.spec.Selects = append(b.spec.Selects, cols...)
}

func (b *builder[O]) setFrom(table Table) {
	if b.hasFrom {
		b.fail(errors.New("from is already set"))
		return
	}

	b.hasFrom = true
	b.spec.From = table
}

func (b *builder[O]) next() *builder[O] {
	n := *b
	n.spec = b.spec.clone()

	return &n
}

func (b *builder[O]) fail(err error) {
	if b.err == nil {
		b.err = err
	}
}

// enter moves to phase, failing when the builder is already past it.
func (b *builder[O]) enter(method string, phase stagePhase) *builder[O] {
	n := b.next()
	if n.phase > phase {
		n.fail(fmt.Errorf("%s cannot follow the clauses already set", method))
	}

	n.phase = phase

	return n
}

type joinBuilder[O any] struct{ *builder[O] }

func (b *builder[O]) addJoin(kind joinType, table Table, on []Condition) JoinStage[O] {
	n := b.enter(string(kind), phaseJoin)
	n.spec.Joins = append(n.spec.Joins, join{kind: kind, table: table, on: on})

	return joinBuilder[O]{n}
}

func (b *builder[O]) Join(table Table, on ...Condition) JoinStage[O] {
	return b.addJoin(innerJoinType, table, on)
}

func (b *builder[O]) InnerJoin(table Table, on ...Condition) JoinStage[O] {
	return b.addJoin(innerJoinType, table, on)
}

func (b *builder[O]) LeftJoin(table Table, on ...Condition) JoinStage[O] {
	return b.addJoin(leftJoinType, table, on)
}

func (b *builder[O]) RightJoin(table Table, on ...Condition) JoinStage[O] {
	return b.addJoin(rightJoinType, table, on)
}

func (b *builder[O]) FullJoin(table Table, on ...Condition) JoinStage[O] {
	return b.addJoin(fullJoinType, table, on)
}

func (b *builder[O]) CrossJoin(table Table) JoinStage[O] {
	return b.addJoin(crossJoinType, table, nil)
}

func (b *builder[O]) Correlate(tables ...Table) JoinStage[O] {
	n := b.enter("Correlate", phaseJoin)
	if len(tables) == 0 {
		n.fail(errors.New("correlate requires at least one outer table"))
	}

	for _, t := range tables {
		if err := tableErr(t); err != nil {
			n.fail(err)
			continue
		}

		for _, existing := range n.spec.Correlated {
			if existing.TableName() == t.TableName() {
				n.fail(fmt.Errorf("correlated table %s is declared twice", t.TableName()))
			}
		}

		n.spec.Correlated = append(n.spec.Correlated, t)
	}

	return joinBuilder[O]{n}
}

func (b *builder[O]) where(conds []Condition) *builder[O] {
	n := b.enter("Where", phaseFilter)
	if n.hasWhere {
		n.fail(errors.New("where is already set"))
	}

	n.hasWhere = true
	n.spec.Filters = append(n.spec.Filters, conds...)

	return n
}

func (b *builder[O]) search(cols []SearchColumn) *builder[O] {
	n := b.enter("Search", phaseFilter)
	if n.hasSearch {
		n.fail(errors.New("search is already set"))
	}

	if len(cols) == 0 {
		n.fail(errors.New("search requires at least one column"))
	}

	n.hasSearch = true
	n.spec.KeywordSearch = append(n.spec.KeywordSearch, cols...)

	return n
}

func (j joinBuilder[O]) Where(conds ...Condition) WhereStage[O] {
	return whereBuilder[O]{j.where(conds)}
}

func (j joinBuilder[O]) Search(cols ...SearchColumn) SearchStage[O] {
	return searchBuilder[O]{j.search(cols)}
}

type whereBuilder[O any] struct{ *builder[O] }

// resultBuilder is a grouped or combined query: its ordering methods return a stage
// that cannot be locked.
type resultBuilder[O any] struct{ *builder[O] }

func (r resultBuilder[O]) OrderBy(orders ...OrderBy) OrderedResultStage[O] {
	return resultBuilder[O]{r.orderBy(orders)}
}

func (r resultBuilder[O]) Limit(limit int) OrderedResultStage[O] {
	return resultBuilder[O]{r.limit(limit)}
}

func (r resultBuilder[O]) Offset(offset int) OrderedResultStage[O] {
	return resultBuilder[O]{r.offset(offset)}
}

func (w whereBuilder[O]) Search(cols ...SearchColumn) FilteredStage[O] {
	return w.search(cols)
}

type searchBuilder[O any] struct{ *builder[O] }

func (s searchBuilder[O]) Where(conds ...Condition) FilteredStage[O] {
	return s.where(conds)
}

func (b *builder[O]) GroupBy(cols ...SQLColumn) GroupedStage[O] {
	n := b.enter("GroupBy", phaseGroup)

	switch {
	case len(cols) == 0:
		n.fail(errors.New("group by requires at least one column"))
	case len(n.spec.GroupBy) > 0:
		n.fail(errors.New("group by is already set"))
	}

	n.spec.GroupBy = append(n.spec.GroupBy, cols...)

	return resultBuilder[O]{n}
}

func (b *builder[O]) Having(conds ...Condition) HavingStage[O] {
	n := b.enter("Having", phaseHaving)
	if len(n.spec.GroupBy) == 0 {
		n.fail(errors.New("having requires group by"))
	}

	n.spec.Having = append(n.spec.Having, conds...)

	return resultBuilder[O]{n}
}

func (b *builder[O]) setOp(op setOperationType, other QueryStage[O]) CompoundStage[O] {
	n := b.enter(string(op), phaseCompound)

	spec, err := stageSpec(other)
	if err != nil {
		n.fail(err)
		return resultBuilder[O]{n}
	}

	// The operand's own ORDER BY, LIMIT, OFFSET and lock would not be rendered: a
	// set operation writes each operand as a bare SELECT.
	if len(spec.OrderBys) > 0 || spec.Limit != nil || spec.Offset != nil || spec.Lock.strength != "" {
		n.fail(fmt.Errorf("%s operands cannot order, limit or lock", op))
	}

	n.spec.SetOps = append(n.spec.SetOps, setOperation[O]{op: op, spec: spec})

	return resultBuilder[O]{n}
}

func (b *builder[O]) Union(other QueryStage[O]) CompoundStage[O] { return b.setOp(unionType, other) }

func (b *builder[O]) UnionAll(other QueryStage[O]) CompoundStage[O] {
	return b.setOp(unionAllType, other)
}

func (b *builder[O]) Intersect(other QueryStage[O]) CompoundStage[O] {
	return b.setOp(intersectType, other)
}

func (b *builder[O]) IntersectAll(other QueryStage[O]) CompoundStage[O] {
	return b.setOp(intersectAllType, other)
}

func (b *builder[O]) Except(other QueryStage[O]) CompoundStage[O] {
	return b.setOp(exceptType, other)
}

func (b *builder[O]) ExceptAll(other QueryStage[O]) CompoundStage[O] {
	return b.setOp(exceptAllType, other)
}

func (b *builder[O]) OrderBy(orders ...OrderBy) OrderedStage[O] { return b.orderBy(orders) }

func (b *builder[O]) Limit(limit int) OrderedStage[O] { return b.limit(limit) }

func (b *builder[O]) Offset(offset int) OrderedStage[O] { return b.offset(offset) }

func (b *builder[O]) orderBy(orders []OrderBy) *builder[O] {
	n := b.enter("OrderBy", phasePaged)

	switch {
	case len(orders) == 0:
		n.fail(errors.New("order by requires at least one term"))
	case len(n.spec.OrderBys) > 0:
		n.fail(errors.New("order by is already set"))
	}

	for _, o := range orders {
		if o.direction != orderAsc && o.direction != orderDesc {
			n.fail(fmt.Errorf("invalid order direction %q; use Asc() or Desc()", o.direction))
		}
	}

	n.spec.OrderBys = append(n.spec.OrderBys, orders...)

	return n
}

func (b *builder[O]) limit(limit int) *builder[O] {
	n := b.enter("Limit", phasePaged)

	switch {
	case limit < 0:
		n.fail(fmt.Errorf("invalid limit: %d", limit))
	case n.spec.Limit != nil:
		n.fail(errors.New("limit is already set"))
	}

	n.spec.Limit = &limit

	return n
}

func (b *builder[O]) offset(offset int) *builder[O] {
	n := b.enter("Offset", phasePaged)

	switch {
	case offset < 0:
		n.fail(fmt.Errorf("invalid offset: %d", offset))
	case n.spec.Offset != nil:
		n.fail(errors.New("offset is already set"))
	}

	n.spec.Offset = &offset

	return n
}

func (b *builder[O]) lock(strength queryLockStrength) LockedStage[O] {
	n := b.enter(string(strength), phaseLocked)
	if n.spec.Lock.strength != "" {
		n.fail(errors.New("row lock is already set"))
	}

	// The stage types already keep a lock off grouped and combined rows; this
	// catches a caller who asserts past them.
	if len(n.spec.GroupBy) > 0 || len(n.spec.SetOps) > 0 {
		n.fail(errors.New("grouped or combined rows cannot be locked"))
	}

	n.spec.Lock = queryLock{strength: strength}

	return n
}

func (b *builder[O]) ForUpdate() LockedStage[O] { return b.lock(queryLockStrengthUpdate) }

func (b *builder[O]) ForShare() LockedStage[O] { return b.lock(queryLockStrengthShare) }

func (b *builder[O]) wait(mode queryLockWaitMode) LockedStage[O] {
	n := b.enter(string(mode), phaseLocked)

	switch {
	case n.spec.Lock.strength == "":
		n.fail(fmt.Errorf("%s requires ForUpdate or ForShare", mode))
	case n.spec.Lock.waitMode != "":
		n.fail(errors.New("lock wait mode is already set"))
	}

	n.spec.Lock.waitMode = mode

	return n
}

func (b *builder[O]) NoWait() LockedStage[O] { return b.wait(queryLockWaitNoWait) }

func (b *builder[O]) SkipLocked() LockedStage[O] { return b.wait(queryLockWaitSkipLocked) }

// specOf returns the validated spec of a finished builder.
func (b *builder[O]) specOf() (querySpec[O], error) {
	if b == nil {
		return querySpec[O]{}, errors.New("query builder cannot be nil")
	}

	if b.err != nil {
		return querySpec[O]{}, b.err
	}

	if err := b.spec.validate(nil); err != nil {
		return querySpec[O]{}, err
	}

	return b.spec.clone(), nil
}

// stageSpec extracts the spec of a stage built by this package.
func stageSpec[O any](stage QueryStage[O]) (querySpec[O], error) {
	if isNilValue(stage) {
		return querySpec[O]{}, errors.New("query stage cannot be nil")
	}

	provider, ok := stage.(interface {
		specOf() (querySpec[O], error)
	})
	if !ok {
		return querySpec[O]{}, errors.New("query stage must come from tsq.Select or tsq.From")
	}

	return provider.specOf()
}

// Build validates the query and returns it.
func (b *builder[O]) subquery() exprInfo {
	q, err := b.Build()
	if err != nil {
		return exprInfo{err: err}
	}

	return q.subquery()
}

func (b *builder[O]) operand() exprInfo {
	q, err := b.Build()
	if err != nil {
		return exprInfo{err: err}
	}

	return q.operand()
}

func (b *builder[O]) setOperand(negated bool) exprInfo {
	q, err := b.Build()
	if err != nil {
		return exprInfo{err: err}
	}

	return q.setOperand(negated)
}

func (*builder[O]) valueOfType(O)  {}
func (*builder[O]) needsTsqVal()   {}
func (*builder[O]) needsTsqVals()  {}
func (*builder[O]) valuesOfType(O) {}

func (b *builder[O]) Build() (*Query[O], error) {
	spec, err := b.specOf()
	if err != nil {
		return nil, err
	}

	return &Query[O]{spec: spec, scanErr: spec.checkScanTargets(), partial: partialColumns(spec.Selects)}, nil
}

// MustBuild is Build for package-level queries whose shape is fixed; it panics on
// an invalid query.
func (b *builder[O]) MustBuild() *Query[O] {
	q, err := b.Build()
	if err != nil {
		panic(err)
	}

	return q
}

func (b *builder[O]) Get(ctx context.Context, db Executor, args ...Arg) (*O, error) {
	q, err := b.Build()
	if err != nil {
		return nil, err
	}

	return q.Get(ctx, db, args...)
}

func (b *builder[O]) Find(ctx context.Context, db Executor, args ...Arg) (*O, error) {
	q, err := b.Build()
	if err != nil {
		return nil, err
	}

	return q.Find(ctx, db, args...)
}

func (b *builder[O]) Exists(ctx context.Context, db Executor, args ...Arg) (bool, error) {
	q, err := b.Build()
	if err != nil {
		return false, err
	}

	return q.Exists(ctx, db, args...)
}

func (b *builder[O]) Count(ctx context.Context, db Executor, args ...Arg) (int64, error) {
	q, err := b.Build()
	if err != nil {
		return 0, err
	}

	return q.Count(ctx, db, args...)
}

func (b *builder[O]) List(ctx context.Context, db Executor, args ...Arg) ([]*O, error) {
	q, err := b.Build()
	if err != nil {
		return nil, err
	}

	return q.List(ctx, db, args...)
}

func (b *builder[O]) Page(ctx context.Context, db Executor, p Paging, args ...Arg) (*Page[O], error) {
	q, err := b.Build()
	if err != nil {
		return nil, err
	}

	return q.Page(ctx, db, p, args...)
}
