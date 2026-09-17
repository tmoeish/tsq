package tsq

import (
	"context"
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
type QueryStage[O any] interface {
	Build() (*Query[O], error)
	MustBuild() *Query[O]
	Get(ctx context.Context, db Executor, args ...Arg) (*O, error)
	Find(ctx context.Context, db Executor, args ...Arg) (*O, error)
	Exists(ctx context.Context, db Executor, args ...Arg) (bool, error)
	Count(ctx context.Context, db Executor, args ...Arg) (int64, error)
	List(ctx context.Context, db Executor, args ...Arg) ([]*O, error)
	Page(ctx context.Context, db Executor, page *PageRequest, args ...Arg) (*PageResponse[O], error)
}

// SelectStage is a query with columns but no FROM table yet.
type SelectStage[O any] interface {
	From(table Table) JoinStage[O]
}

// FromStage is a query with a FROM table but no columns yet.
type FromStage[O any] interface {
	Select(cols ...BoundColumn[O]) JoinStage[O]
}

// JoinStage is a query that can still take joins.
type JoinStage[O any] interface {
	QueryStage[O]
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
	// Search sets the columns Page matches PageRequest.Keyword against.
	Search(cols ...SearchColumn) SearchStage[O]
	GroupBy(cols ...SQLColumn) GroupedStage[O]
	OrderBy(orders ...OrderBy) PagedStage[O]
	Limit(limit int) PagedStage[O]
	Offset(offset int) PagedStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// WhereStage is a query with a WHERE clause.
type WhereStage[O any] interface {
	QueryStage[O]
	Search(cols ...SearchColumn) FilteredStage[O]
	GroupBy(cols ...SQLColumn) GroupedStage[O]
	OrderBy(orders ...OrderBy) PagedStage[O]
	Limit(limit int) PagedStage[O]
	Offset(offset int) PagedStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// SearchStage is a query with search columns.
type SearchStage[O any] interface {
	QueryStage[O]
	Where(conds ...Condition) FilteredStage[O]
	GroupBy(cols ...SQLColumn) GroupedStage[O]
	OrderBy(orders ...OrderBy) PagedStage[O]
	Limit(limit int) PagedStage[O]
	Offset(offset int) PagedStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
}

// FilteredStage is a query with both WHERE and search columns.
type FilteredStage[O any] interface {
	QueryStage[O]
	GroupBy(cols ...SQLColumn) GroupedStage[O]
	OrderBy(orders ...OrderBy) PagedStage[O]
	Limit(limit int) PagedStage[O]
	Offset(offset int) PagedStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
}

// GroupedStage is a query with GROUP BY.
type GroupedStage[O any] interface {
	QueryStage[O]
	Having(conds ...Condition) HavingStage[O]
	OrderBy(orders ...OrderBy) PagedStage[O]
	Limit(limit int) PagedStage[O]
	Offset(offset int) PagedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// HavingStage is a grouped query with HAVING.
type HavingStage[O any] interface {
	QueryStage[O]
	OrderBy(orders ...OrderBy) PagedStage[O]
	Limit(limit int) PagedStage[O]
	Offset(offset int) PagedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// CompoundStage is a query combined with others by set operations.
type CompoundStage[O any] interface {
	QueryStage[O]
	OrderBy(orders ...OrderBy) PagedStage[O]
	Limit(limit int) PagedStage[O]
	Offset(offset int) PagedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// PagedStage is a query with ORDER BY, LIMIT or OFFSET.
type PagedStage[O any] interface {
	QueryStage[O]
	OrderBy(orders ...OrderBy) PagedStage[O]
	Limit(limit int) PagedStage[O]
	Offset(offset int) PagedStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
}

// LockedStage is a query that locks the rows it reads. Row locks only mean
// something inside a transaction.
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

// From starts a query with its FROM table.
func From[O any](table Table) FromStage[O] {
	b := &builder[O]{}
	b.setFrom(table)

	return fromBuilder[O]{b}
}

type selectBuilder[O any] struct{ b *builder[O] }

func (s selectBuilder[O]) From(table Table) JoinStage[O] {
	n := s.b.next()
	n.setFrom(table)

	return joinBuilder[O]{n}
}

type fromBuilder[O any] struct{ b *builder[O] }

func (f fromBuilder[O]) Select(cols ...BoundColumn[O]) JoinStage[O] {
	n := f.b.next()
	n.setSelect(cols)

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
			if existing.Name() == t.Name() {
				n.fail(fmt.Errorf("correlated table %s is declared twice", t.Name()))
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

	return n
}

func (b *builder[O]) Having(conds ...Condition) HavingStage[O] {
	n := b.enter("Having", phaseHaving)
	if len(n.spec.GroupBy) == 0 {
		n.fail(errors.New("having requires group by"))
	}

	n.spec.Having = append(n.spec.Having, conds...)

	return n
}

func (b *builder[O]) setOp(op setOperationType, other QueryStage[O]) CompoundStage[O] {
	n := b.enter(string(op), phaseCompound)

	spec, err := stageSpec(other)
	if err != nil {
		n.fail(err)
		return n
	}

	if len(n.spec.OrderBys) > 0 || n.spec.Limit != nil || n.spec.Lock.strength != "" {
		n.fail(fmt.Errorf("%s operands cannot order, limit or lock", op))
	}

	n.spec.SetOps = append(n.spec.SetOps, setOperation[O]{op: op, spec: spec})

	return n
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

func (b *builder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	n := b.enter("OrderBy", phasePaged)

	switch {
	case len(orders) == 0:
		n.fail(errors.New("order by requires at least one term"))
	case len(n.spec.OrderBys) > 0:
		n.fail(errors.New("order by is already set"))
	}

	for _, o := range orders {
		if o.direction != ASC && o.direction != DESC {
			n.fail(fmt.Errorf("invalid order direction %q; use Asc() or Desc()", o.direction))
		}
	}

	n.spec.OrderBys = append(n.spec.OrderBys, orders...)

	return n
}

func (b *builder[O]) Limit(limit int) PagedStage[O] {
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

func (b *builder[O]) Offset(offset int) PagedStage[O] {
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
func (b *builder[O]) Build() (*Query[O], error) {
	spec, err := b.specOf()
	if err != nil {
		return nil, err
	}

	return &Query[O]{spec: spec}, nil
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

func (b *builder[O]) Page(ctx context.Context, db Executor, page *PageRequest, args ...Arg) (*PageResponse[O], error) {
	q, err := b.Build()
	if err != nil {
		return nil, err
	}

	return q.Page(ctx, db, page, args...)
}
