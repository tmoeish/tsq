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
	builderPhaseKwSearch   builderPhase = "query-with-kw-search"
	builderPhaseFiltered   builderPhase = "query-with-filters"
	builderPhaseGrouped    builderPhase = "grouped-query"
	builderPhaseHaving     builderPhase = "query-with-having"
	builderPhaseLocked     builderPhase = "query-with-lock"
	builderPhaseCompound   builderPhase = "compound-query"
)

// QueryBuilder is the main fluent builder once both SELECT and FROM are known.
type QueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// SelectBuilder represents a query after Select(...) and before From(...).
type SelectBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// FromBuilder represents a query after From(...) and before Select(...).
type FromBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// WhereQueryBuilder represents a query with WHERE set and before Search/GroupBy/Build.
type WhereQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// SearchQueryBuilder represents a query with Search set and before Where/GroupBy/Build.
type SearchQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// FilteredQueryBuilder represents a query with both WHERE and Search set.
type FilteredQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// GroupedQueryBuilder represents a query after GroupBy(...) and before Having/Build.
type GroupedQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// HavingQueryBuilder represents a grouped query with HAVING applied.
type HavingQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// CompoundQueryBuilder represents a query with one or more set operations.
type CompoundQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// LockedQueryBuilder represents a query with a row-lock clause.
type LockedQueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// queryBuilderCore stores the shared mutable state for every staged builder wrapper.
type queryBuilderCore[O Owner] struct {
	spec     QuerySpec[O]
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
	spec QuerySpec[O]
}

var errQueryBuilderNil = errors.New("query builder cannot be nil")

type completeQueryStage[O Owner] interface {
	core() *queryBuilderCore[O]
	completeQueryStage()
}

// Select creates a new state-machine builder with the specified columns.
func Select[O Owner](cols ...BoundColumn[O]) *SelectBuilder[O] {
	core := newQueryBuilderCore[O](builderPhaseNeedFrom)
	core.setSelect(cols...)

	return &SelectBuilder[O]{queryBuilderCore: core}
}

// From creates a new state-machine builder with the specified base table.
func From[O Owner](table Table) *FromBuilder[O] {
	core := newQueryBuilderCore[O](builderPhaseNeedSelect)
	core.setFrom(table)

	return &FromBuilder[O]{queryBuilderCore: core}
}
