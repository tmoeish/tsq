package tsq

import "errors"

type joinType string

const (
	leftJoinType  joinType = "LEFT JOIN"
	innerJoinType joinType = "INNER JOIN"
	rightJoinType joinType = "RIGHT JOIN"
	fullJoinType  joinType = "FULL JOIN"
	crossJoinType joinType = "CROSS JOIN"
)

type setOperationType string

const (
	unionType        setOperationType = "UNION"
	unionAllType     setOperationType = "UNION ALL"
	intersectType    setOperationType = "INTERSECT"
	intersectAllType setOperationType = "INTERSECT ALL"
	exceptType       setOperationType = "EXCEPT"
	exceptAllType    setOperationType = "EXCEPT ALL"
)

type queryLockStrength string

const (
	queryLockStrengthUpdate queryLockStrength = "FOR UPDATE"
	queryLockStrengthShare  queryLockStrength = "FOR SHARE"
)

type queryLockWaitMode string

const (
	queryLockWaitNoWait     queryLockWaitMode = "NOWAIT"
	queryLockWaitSkipLocked queryLockWaitMode = "SKIP LOCKED"
)

type queryLock struct {
	strength queryLockStrength
	waitMode queryLockWaitMode
}

func (l queryLock) clause() string {
	if l.strength == "" {
		return ""
	}

	if l.waitMode == "" {
		return string(l.strength)
	}

	return string(l.strength) + " " + string(l.waitMode)
}

// builderPhase tracks which clauses may still be appended to a query builder.
type builderPhase string

const (
	builderPhaseUnset      builderPhase = "uninitialized"
	builderPhaseNeedFrom   builderPhase = "selected"
	builderPhaseNeedSelect builderPhase = "from-only"
	builderPhaseBase       builderPhase = "query"
	builderPhaseWhere      builderPhase = "query-with-where"
	builderPhaseSearch     builderPhase = "query-with-search"
	builderPhaseFiltered   builderPhase = "query-with-filters"
	builderPhaseGrouped    builderPhase = "grouped-query"
	builderPhaseHaving     builderPhase = "query-with-having"
	builderPhaseLocked     builderPhase = "query-with-lock"
	builderPhaseCompound   builderPhase = "compound-query"
)

type queryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// QueryStage is a buildable query state that can participate in CTEs and set operations.
type QueryStage[O Owner] interface {
	Build() (*Query[O], error)
}

// SelectStage is the result of Select(...) before From(...) is attached.
type SelectStage[O Owner] interface {
	From(table Table) *queryBuilder[O]
}

// FromStage is the result of From(...) before Select(...) is attached.
type FromStage[O Owner] interface {
	Select(cols ...BoundColumn[O]) *queryBuilder[O]
}

// WhereStage is the query state after Where(...).
type WhereStage[O Owner] interface {
	QueryStage[O]
	Search(cols ...SearchColumn) FilteredStage[O]
	GroupBy(cols ...SQLColumn) GroupedStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// SearchStage is the query state after Search(...).
type SearchStage[O Owner] interface {
	QueryStage[O]
	Where(conds ...Condition) FilteredStage[O]
	GroupBy(cols ...SQLColumn) GroupedStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// FilteredStage is the query state after both Where(...) and Search(...).
type FilteredStage[O Owner] interface {
	QueryStage[O]
	GroupBy(cols ...SQLColumn) GroupedStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// GroupedStage is the query state after GroupBy(...).
type GroupedStage[O Owner] interface {
	QueryStage[O]
	Having(conds ...Condition) HavingStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// HavingStage is the query state after Having(...).
type HavingStage[O Owner] interface {
	QueryStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// CompoundStage is the query state after one or more set operations.
type CompoundStage[O Owner] interface {
	QueryStage[O]
	ForUpdate() LockedStage[O]
	ForShare() LockedStage[O]
	Union(other QueryStage[O]) CompoundStage[O]
	UnionAll(other QueryStage[O]) CompoundStage[O]
	Intersect(other QueryStage[O]) CompoundStage[O]
	IntersectAll(other QueryStage[O]) CompoundStage[O]
	Except(other QueryStage[O]) CompoundStage[O]
	ExceptAll(other QueryStage[O]) CompoundStage[O]
}

// LockedStage is the query state after ForUpdate()/ForShare().
type LockedStage[O Owner] interface {
	QueryStage[O]
	NoWait() LockedStage[O]
	SkipLocked() LockedStage[O]
}

type selectBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

type fromBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

type whereQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

type searchQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

type filteredQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

type groupedQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

type havingQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

type compoundQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

type lockedQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// queryBuilderCore stores the shared mutable state for every staged builder wrapper.
type queryBuilderCore[O Owner] struct {
	spec     querySpec[O]
	buildErr error
	phase    builderPhase
}

// join represents any type of JOIN operation.
type join struct {
	joinType joinType
	table    Table
	on       []Condition
}

type setOperation[O Owner] struct {
	op   setOperationType
	spec querySpec[O]
}

var errQueryBuilderNil = errors.New("query builder cannot be nil")

// Select creates a new state-machine builder with the specified columns.
func Select[O Owner](cols ...BoundColumn[O]) SelectStage[O] {
	core := newQueryBuilderCore[O](builderPhaseNeedFrom)
	core.setSelect(cols...)

	return &selectBuilder[O]{queryBuilderCore: core}
}

// From creates a new state-machine builder with the specified base table.
func From[O Owner](table Table) FromStage[O] {
	core := newQueryBuilderCore[O](builderPhaseNeedSelect)
	core.setFrom(table)

	return &fromBuilder[O]{queryBuilderCore: core}
}
