package tsq

import (
	"errors"
	"fmt"
)

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

func newQueryBuilderCore[O Owner](phase builderPhase) *queryBuilderCore[O] {
	return &queryBuilderCore[O]{
		spec: QuerySpec[O]{
			Selects: make([]BoundColumn[O], 0),
			Joins:   make([]join, 0),
			GroupBy: make([]SQLColumn, 0),
			Having:  make([]Condition, 0),
			SetOps:  make([]setOperation[O], 0),
		},
		phase: phase,
	}
}

func ensureQueryBuilderCore[O Owner](core *queryBuilderCore[O], defaultPhase builderPhase) *queryBuilderCore[O] {
	if core == nil {
		return newQueryBuilderCore[O](defaultPhase)
	}

	core = &queryBuilderCore[O]{
		spec:     cloneQuerySpec(core.spec),
		buildErr: core.buildErr,
		phase:    core.phase,
	}

	if core.spec.Selects == nil {
		core.spec.Selects = make([]BoundColumn[O], 0)
	}

	if core.spec.Joins == nil {
		core.spec.Joins = make([]join, 0)
	}

	if core.spec.GroupBy == nil {
		core.spec.GroupBy = make([]SQLColumn, 0)
	}

	if core.spec.Having == nil {
		core.spec.Having = make([]Condition, 0)
	}

	if core.spec.SetOps == nil {
		core.spec.SetOps = make([]setOperation[O], 0)
	}

	if core.phase == builderPhaseUnset {
		core.phase = defaultPhase
	}

	return core
}

func (qb *QueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *SelectBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *FromBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *WhereQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *SearchQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *FilteredQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *GroupedQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *HavingQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *CompoundQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *LockedQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		panic(errQueryBuilderNil)
	}

	return qb.queryBuilderCore
}

func (qb *QueryBuilder[O]) completeQueryStage()         {}
func (qb *WhereQueryBuilder[O]) completeQueryStage()    {}
func (qb *SearchQueryBuilder[O]) completeQueryStage()   {}
func (qb *FilteredQueryBuilder[O]) completeQueryStage() {}
func (qb *GroupedQueryBuilder[O]) completeQueryStage()  {}
func (qb *HavingQueryBuilder[O]) completeQueryStage()   {}
func (qb *CompoundQueryBuilder[O]) completeQueryStage() {}

// Select creates a new state-machine builder with the specified owner-constrained columns.
func Select[O Owner](cols ...BoundColumn[O]) *SelectBuilder[O] {
	core := newQueryBuilderCore[O](builderPhaseNeedFrom)
	core.spec.Selects = make([]BoundColumn[O], 0, len(cols))
	core.addSelectColumns(cols...)

	return &SelectBuilder[O]{queryBuilderCore: core}
}

// From creates a new state-machine builder with the specified base table.
func From[O Owner](table Table) *FromBuilder[O] {
	core := newQueryBuilderCore[O](builderPhaseNeedSelect)
	core.setFrom(table)

	return &FromBuilder[O]{queryBuilderCore: core}
}

func (core *queryBuilderCore[O]) setBuildError(err error) {
	if core == nil || err == nil || core.buildErr != nil {
		return
	}

	core.buildErr = err
}

func (core *queryBuilderCore[O]) failTransition(method string) {
	core.setBuildError(fmt.Errorf("%s is not available in %s state", method, core.phase))
}

func (core *queryBuilderCore[O]) addSelectColumns(cols ...BoundColumn[O]) {
	for _, col := range cols {
		if err := validateBoundColumn(col); err != nil {
			core.setBuildError(err)
			continue
		}

		core.spec.Selects = append(core.spec.Selects, col)
	}
}

func (core *queryBuilderCore[O]) appendColumn(target *[]SQLColumn, col SQLColumn) {
	if _, err := validateColumnInput(col); err != nil {
		core.setBuildError(err)
		return
	}

	*target = append(*target, col)
}

func (core *queryBuilderCore[O]) appendSearchColumn(target *[]SearchColumn, col SearchColumn) {
	if err := validateSearchColumn(col); err != nil {
		core.setBuildError(err)
		return
	}

	*target = append(*target, col)
}

func (core *queryBuilderCore[O]) appendCondition(target *[]Condition, cond Condition) {
	if _, _, _, err := validateConditionInput(cond); err != nil {
		core.setBuildError(err)
		return
	}

	*target = append(*target, cond)
}

func (core *queryBuilderCore[O]) setFrom(table Table) {
	if core.buildErr != nil {
		return
	}

	if core.phase != builderPhaseNeedFrom && core.phase != builderPhaseNeedSelect {
		core.failTransition("From()")
		return
	}

	if err := validateTableInput(table, "from table"); err != nil {
		core.setBuildError(err)
		return
	}

	core.spec.From = table
	if core.phase == builderPhaseNeedFrom {
		core.phase = builderPhaseBase
	}
}

func (core *queryBuilderCore[O]) setSelect(cols ...BoundColumn[O]) {
	if core.buildErr != nil {
		return
	}

	if core.phase != builderPhaseNeedSelect && core.phase != builderPhaseNeedFrom {
		core.failTransition("Select()")
		return
	}

	core.spec.Selects = make([]BoundColumn[O], 0, len(cols))
	core.addSelectColumns(cols...)

	if core.phase == builderPhaseNeedSelect {
		core.phase = builderPhaseBase
	}
}

func (core *queryBuilderCore[O]) addJoin(joinType joinType, table Table, conds ...Condition) {
	if core.buildErr != nil {
		return
	}

	if core.phase != builderPhaseBase {
		core.failTransition(string(joinType))
		return
	}

	if err := validateTableInput(table, "join table"); err != nil {
		core.setBuildError(err)
		return
	}

	on := make([]Condition, 0, len(conds))
	for _, cond := range conds {
		if _, _, _, err := validateConditionInput(cond); err != nil {
			core.setBuildError(err)
			return
		}

		on = append(on, cond)
	}

	core.spec.Joins = append(core.spec.Joins, join{
		joinType: joinType,
		table:    table,
		on:       on,
	})
}

func (core *queryBuilderCore[O]) addCrossJoin(table Table) {
	if core.buildErr != nil {
		return
	}

	if core.phase != builderPhaseBase {
		core.failTransition("CrossJoin()")
		return
	}

	if err := validateTableInput(table, "cross join table"); err != nil {
		core.setBuildError(err)
		return
	}

	core.spec.Joins = append(core.spec.Joins, join{
		joinType: crossJoinType,
		table:    table,
	})
}

func (core *queryBuilderCore[O]) setWhere(conds ...Condition) {
	if core.buildErr != nil {
		return
	}

	switch core.phase {
	case builderPhaseBase, builderPhaseKwSearch:
	default:
		core.failTransition("Where()")
		return
	}

	filters := make([]Condition, 0, len(conds))
	for _, cond := range conds {
		core.appendCondition(&filters, cond)
	}

	core.spec.Filters = filters
	if core.phase == builderPhaseKwSearch {
		core.phase = builderPhaseFiltered
		return
	}

	core.phase = builderPhaseWhere
}

func (core *queryBuilderCore[O]) setSearch(cols ...SearchColumn) {
	if core.buildErr != nil {
		return
	}

	switch core.phase {
	case builderPhaseBase, builderPhaseWhere:
	default:
		core.failTransition("Search()")
		return
	}

	core.spec.KeywordSearch = make([]SearchColumn, 0, len(cols))
	for _, col := range cols {
		core.appendSearchColumn(&core.spec.KeywordSearch, col)
	}

	if core.phase == builderPhaseWhere {
		core.phase = builderPhaseFiltered
		return
	}

	core.phase = builderPhaseKwSearch
}

func (core *queryBuilderCore[O]) setGroupBy(cols ...SQLColumn) {
	if core.buildErr != nil {
		return
	}

	switch core.phase {
	case builderPhaseBase, builderPhaseWhere, builderPhaseKwSearch, builderPhaseFiltered:
	default:
		core.failTransition("GroupBy()")
		return
	}

	core.spec.GroupBy = make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		core.appendColumn(&core.spec.GroupBy, col)
	}

	core.phase = builderPhaseGrouped
}

func (core *queryBuilderCore[O]) setHaving(conds ...Condition) {
	if core.buildErr != nil {
		return
	}

	if core.phase != builderPhaseGrouped {
		core.failTransition("Having()")
		return
	}

	core.spec.Having = make([]Condition, 0, len(conds))
	for _, cond := range conds {
		core.appendCondition(&core.spec.Having, cond)
	}

	core.phase = builderPhaseHaving
}

func (core *queryBuilderCore[O]) isComplete() bool {
	switch core.phase {
	case builderPhaseBase,
		builderPhaseWhere,
		builderPhaseKwSearch,
		builderPhaseFiltered,
		builderPhaseGrouped,
		builderPhaseHaving,
		builderPhaseLocked,
		builderPhaseCompound:
		return true
	default:
		return false
	}
}

func (core *queryBuilderCore[O]) appendSetOperation(op setOperationType, other completeQueryStage[O]) {
	if core.buildErr != nil {
		return
	}

	if !core.isComplete() {
		core.failTransition(string(op))
		return
	}

	if other == nil || other.core() == nil {
		core.setBuildError(errors.New("set operation query builder cannot be nil"))
		return
	}

	otherCore := ensureQueryBuilderCore(other.core(), builderPhaseBase)
	if otherCore.buildErr != nil {
		core.setBuildError(otherCore.buildErr)
		return
	}

	if !otherCore.isComplete() {
		core.setBuildError(fmt.Errorf("set operation %s requires a complete query", op))
		return
	}

	if len(core.spec.Selects) == 0 || len(otherCore.spec.Selects) == 0 {
		core.setBuildError(errors.New("set operations require both queries to select at least one column"))
		return
	}

	if len(core.spec.Selects) != len(otherCore.spec.Selects) {
		core.setBuildError(fmt.Errorf(
			"set operation %s requires matching select column counts: left=%d right=%d",
			op,
			len(core.spec.Selects),
			len(otherCore.spec.Selects),
		))

		return
	}

	if len(core.spec.KeywordSearch) > 0 || len(otherCore.spec.KeywordSearch) > 0 {
		core.setBuildError(errors.New("set operations do not support keyword search"))
		return
	}

	core.spec.SetOps = append(core.spec.SetOps, setOperation[O]{
		op:   op,
		spec: cloneQuerySpec(otherCore.spec),
	})
	core.phase = builderPhaseCompound
}

func (core *queryBuilderCore[O]) setLockStrength(strength queryLockStrength) {
	if core.buildErr != nil {
		return
	}

	if !core.isComplete() {
		core.failTransition(string(strength))
		return
	}

	core.spec.Lock = queryLock{strength: strength}
	core.phase = builderPhaseLocked
}

func (core *queryBuilderCore[O]) setLockWaitMode(mode queryLockWaitMode) {
	if core.buildErr != nil {
		return
	}

	if core.spec.Lock.strength == "" {
		core.setBuildError(errors.New("lock wait mode requires FOR UPDATE or FOR SHARE"))
		return
	}

	if core.spec.Lock.waitMode != "" {
		core.setBuildError(errors.New("lock wait mode is already set"))
		return
	}

	core.spec.Lock.waitMode = mode
	core.phase = builderPhaseLocked
}

// From sets the base table for a SELECT-first builder.
func (qb *SelectBuilder[O]) From(table Table) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseNeedFrom)
	core.setFrom(table)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// Select sets the projected columns for a FROM-first builder.
func (qb *FromBuilder[O]) Select(cols ...BoundColumn[O]) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseNeedSelect)
	core.setSelect(cols...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// Join adds an INNER JOIN clause.
func (qb *QueryBuilder[O]) Join(table Table, conds ...Condition) *QueryBuilder[O] {
	return qb.InnerJoin(table, conds...)
}

// LeftJoin adds a LEFT JOIN clause with ON conditions joined by AND.
func (qb *QueryBuilder[O]) LeftJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addJoin(leftJoinType, table, conds...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// InnerJoin adds an INNER JOIN clause with ON conditions joined by AND.
func (qb *QueryBuilder[O]) InnerJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addJoin(innerJoinType, table, conds...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// RightJoin adds a RIGHT JOIN clause with ON conditions joined by AND.
func (qb *QueryBuilder[O]) RightJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addJoin(rightJoinType, table, conds...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// FullJoin adds a FULL JOIN clause with ON conditions joined by AND.
func (qb *QueryBuilder[O]) FullJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addJoin(fullJoinType, table, conds...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// CrossJoin adds a CROSS JOIN clause.
func (qb *QueryBuilder[O]) CrossJoin(table Table) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addCrossJoin(table)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// Where sets the WHERE clause for the query.
func (qb *QueryBuilder[O]) Where(conds ...Condition) *WhereQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setWhere(conds...)

	return &WhereQueryBuilder[O]{queryBuilderCore: core}
}

// Search sets keyword-search columns for the query.
func (qb *QueryBuilder[O]) Search(cols ...SearchColumn) *SearchQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setSearch(cols...)

	return &SearchQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause for the query.
func (qb *QueryBuilder[O]) GroupBy(cols ...SQLColumn) *GroupedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setGroupBy(cols...)

	return &GroupedQueryBuilder[O]{queryBuilderCore: core}
}

// Where sets the WHERE clause after keyword-search columns are fixed.
func (qb *SearchQueryBuilder[O]) Where(conds ...Condition) *FilteredQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.setWhere(conds...)

	return &FilteredQueryBuilder[O]{queryBuilderCore: core}
}

// Search sets keyword-search columns after WHERE is fixed.
func (qb *WhereQueryBuilder[O]) Search(cols ...SearchColumn) *FilteredQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setSearch(cols...)

	return &FilteredQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after WHERE is fixed.
func (qb *WhereQueryBuilder[O]) GroupBy(cols ...SQLColumn) *GroupedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setGroupBy(cols...)

	return &GroupedQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after keyword search is fixed.
func (qb *SearchQueryBuilder[O]) GroupBy(cols ...SQLColumn) *GroupedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.setGroupBy(cols...)

	return &GroupedQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after all filters are fixed.
func (qb *FilteredQueryBuilder[O]) GroupBy(cols ...SQLColumn) *GroupedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setGroupBy(cols...)

	return &GroupedQueryBuilder[O]{queryBuilderCore: core}
}

// Having sets the HAVING clause for the grouped query.
func (qb *GroupedQueryBuilder[O]) Having(conds ...Condition) *HavingQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setHaving(conds...)

	return &HavingQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *QueryBuilder[O]) ForUpdate() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setLockStrength(queryLockStrengthUpdate)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *WhereQueryBuilder[O]) ForUpdate() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setLockStrength(queryLockStrengthUpdate)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *SearchQueryBuilder[O]) ForUpdate() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.setLockStrength(queryLockStrengthUpdate)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *FilteredQueryBuilder[O]) ForUpdate() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setLockStrength(queryLockStrengthUpdate)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *GroupedQueryBuilder[O]) ForUpdate() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setLockStrength(queryLockStrengthUpdate)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *HavingQueryBuilder[O]) ForUpdate() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.setLockStrength(queryLockStrengthUpdate)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *CompoundQueryBuilder[O]) ForUpdate() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.setLockStrength(queryLockStrengthUpdate)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *QueryBuilder[O]) ForShare() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setLockStrength(queryLockStrengthShare)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *WhereQueryBuilder[O]) ForShare() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setLockStrength(queryLockStrengthShare)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *SearchQueryBuilder[O]) ForShare() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.setLockStrength(queryLockStrengthShare)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *FilteredQueryBuilder[O]) ForShare() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setLockStrength(queryLockStrengthShare)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *GroupedQueryBuilder[O]) ForShare() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setLockStrength(queryLockStrengthShare)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *HavingQueryBuilder[O]) ForShare() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.setLockStrength(queryLockStrengthShare)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *CompoundQueryBuilder[O]) ForShare() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.setLockStrength(queryLockStrengthShare)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// NoWait adds NOWAIT to a locked query.
func (qb *LockedQueryBuilder[O]) NoWait() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseLocked)
	core.setLockWaitMode(queryLockWaitNoWait)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

// SkipLocked adds SKIP LOCKED to a locked query.
func (qb *LockedQueryBuilder[O]) SkipLocked() *LockedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseLocked)
	core.setLockWaitMode(queryLockWaitSkipLocked)

	return &LockedQueryBuilder[O]{queryBuilderCore: core}
}

func buildQuery[O Owner](core *queryBuilderCore[O]) (*Query[O], error) {
	if core == nil {
		return nil, errors.New("query builder cannot be nil")
	}

	core = ensureQueryBuilderCore(core, core.phase)
	if core.buildErr != nil {
		return nil, core.buildErr
	}

	plan, err := buildQueryPlan(core.spec)
	if err != nil {
		return nil, err
	}

	return &Query[O]{
		cntSQL:     plan.cntSQL,
		listSQL:    plan.listSQL,
		kwCntSQL:   plan.kwCntSQL,
		kwListSQL:  plan.kwListSQL,
		cntArgs:    plan.cntArgs,
		listArgs:   plan.listArgs,
		kwCntArgs:  plan.kwCntArgs,
		kwListArgs: plan.kwListArgs,

		cntArgState:    scanQueryArgState(plan.cntArgs),
		listArgState:   scanQueryArgState(plan.listArgs),
		kwCntArgState:  scanQueryArgState(plan.kwCntArgs),
		kwListArgState: scanQueryArgState(plan.kwListArgs),

		selectCols:   cloneBoundColumns(core.spec.Selects),
		selectTables: core.spec.selectTables(),
		kwCols:       cloneSearchColumns(core.spec.KeywordSearch),
		kwTables:     core.spec.keywordTables(),
		hasSetOps:    len(core.spec.SetOps) > 0,
	}, nil
}

func cloneBoundColumns[O Owner](cols []BoundColumn[O]) []BoundColumn[O] {
	if len(cols) == 0 {
		return nil
	}

	return append([]BoundColumn[O](nil), cols...)
}

func cloneSearchColumns(cols []SearchColumn) []SearchColumn {
	if len(cols) == 0 {
		return nil
	}

	return append([]SearchColumn(nil), cols...)
}

// Build compiles and validates the query shape.
//
// Build validates owner wiring, clause ordering, selected columns, and other
// dialect-independent structure. It intentionally does not reject dialect-
// specific capabilities that depend on the runtime executor, because the same
// built Query may later run against different registries or executors with
// different dialects. Capability checks that require the concrete executor
// dialect therefore happen during execution.
func (qb *QueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build compiles and validates the query shape.
//
// Build validates owner wiring, clause ordering, selected columns, and other
// dialect-independent structure. It intentionally does not reject dialect-
// specific capabilities that depend on the runtime executor, because the same
// built Query may later run against different registries or executors with
// different dialects. Capability checks that require the concrete executor
// dialect therefore happen during execution.
func (qb *WhereQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build compiles and validates the query shape.
//
// Build validates owner wiring, clause ordering, selected columns, and other
// dialect-independent structure. It intentionally does not reject dialect-
// specific capabilities that depend on the runtime executor, because the same
// built Query may later run against different registries or executors with
// different dialects. Capability checks that require the concrete executor
// dialect therefore happen during execution.
func (qb *SearchQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build compiles and validates the query shape.
//
// Build validates owner wiring, clause ordering, selected columns, and other
// dialect-independent structure. It intentionally does not reject dialect-
// specific capabilities that depend on the runtime executor, because the same
// built Query may later run against different registries or executors with
// different dialects. Capability checks that require the concrete executor
// dialect therefore happen during execution.
func (qb *FilteredQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build compiles and validates the query shape.
//
// Build validates owner wiring, clause ordering, selected columns, and other
// dialect-independent structure. It intentionally does not reject dialect-
// specific capabilities that depend on the runtime executor, because the same
// built Query may later run against different registries or executors with
// different dialects. Capability checks that require the concrete executor
// dialect therefore happen during execution.
func (qb *GroupedQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build compiles and validates the query shape.
//
// Build validates owner wiring, clause ordering, selected columns, and other
// dialect-independent structure. It intentionally does not reject dialect-
// specific capabilities that depend on the runtime executor, because the same
// built Query may later run against different registries or executors with
// different dialects. Capability checks that require the concrete executor
// dialect therefore happen during execution.
func (qb *HavingQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build compiles and validates the query shape.
//
// Build validates owner wiring, clause ordering, selected columns, and other
// dialect-independent structure. It intentionally does not reject dialect-
// specific capabilities that depend on the runtime executor, because the same
// built Query may later run against different registries or executors with
// different dialects. Capability checks that require the concrete executor
// dialect therefore happen during execution.
func (qb *CompoundQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build compiles and validates the locked query shape.
func (qb *LockedQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Union appends a UNION clause to the current query.
func (qb *QueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(unionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *WhereQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(unionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *SearchQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(unionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *FilteredQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(unionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *GroupedQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(unionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *HavingQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(unionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *CompoundQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(unionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *QueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(unionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *WhereQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(unionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *SearchQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(unionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *FilteredQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(unionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *GroupedQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(unionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *HavingQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(unionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *CompoundQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(unionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *QueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(intersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *WhereQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(intersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *SearchQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(intersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *FilteredQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(intersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *GroupedQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(intersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *HavingQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(intersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *CompoundQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(intersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *QueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(intersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *WhereQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(intersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *SearchQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(intersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *FilteredQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(intersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *GroupedQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(intersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *HavingQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(intersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *CompoundQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(intersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *QueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(exceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *WhereQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(exceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *SearchQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(exceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *FilteredQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(exceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *GroupedQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(exceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *HavingQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(exceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *CompoundQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(exceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *QueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(exceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *WhereQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(exceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *SearchQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(exceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *FilteredQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(exceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *GroupedQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(exceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *HavingQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(exceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *CompoundQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(exceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}
