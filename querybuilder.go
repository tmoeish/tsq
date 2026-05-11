package tsq

import "github.com/juju/errors"

// JoinType represents different types of SQL joins.
type JoinType string

const (
	LeftJoinType  JoinType = "LEFT JOIN"
	InnerJoinType JoinType = "INNER JOIN"
	RightJoinType JoinType = "RIGHT JOIN"
	FullJoinType  JoinType = "FULL JOIN"
	CrossJoinType JoinType = "CROSS JOIN"
)

// SetOperationType represents SQL compound query operators.
type SetOperationType string

const (
	// UnionType combines distinct rows from both queries.
	UnionType SetOperationType = "UNION"
	// UnionAllType combines all rows from both queries, preserving duplicates.
	UnionAllType SetOperationType = "UNION ALL"
	// IntersectType keeps only rows present in both queries.
	IntersectType SetOperationType = "INTERSECT"
	// IntersectAllType keeps rows present in both queries, preserving duplicate counts.
	IntersectAllType SetOperationType = "INTERSECT ALL"
	// ExceptType removes rows present in the right query from the left query.
	ExceptType SetOperationType = "EXCEPT"
	// ExceptAllType removes rows present in the right query from the left query while preserving duplicate counts.
	ExceptAllType SetOperationType = "EXCEPT ALL"
)

// builderPhase 定义了查询构建过程中的各种状态。
// 架构意图：TSQ 使用有限状态机（FSM）来管理查询构建。
// 这种设计可以在编译期（通过不同的返回类型）或运行期（通过状态检查）防止生成无效的 SQL（如在 WHERE 之后调用 JOIN）。
type builderPhase string

const (
	builderPhaseUnset      builderPhase = "uninitialized"
	builderPhaseNeedFrom   builderPhase = "selected"    // 已 Select，等待 From
	builderPhaseNeedSelect builderPhase = "from-only"   // 已 From，等待 Select
	builderPhaseBase       builderPhase = "query"       // 基础查询，可进行 Join/Where/GroupBy
	builderPhaseWhere      builderPhase = "query-with-where"
	builderPhaseKwSearch   builderPhase = "query-with-kw-search"
	builderPhaseFiltered   builderPhase = "query-with-filters"
	builderPhaseGrouped    builderPhase = "grouped-query"
	builderPhaseHaving     builderPhase = "query-with-having"
	builderPhaseCompound   builderPhase = "compound-query" // 集合操作后的状态
)

// QueryBuilder 是一个通用的构建器包装器。
// TSQ 提供了多个特定阶段的构建器（如 SelectBuilder, WhereQueryBuilder），
// 它们在底层共享同一个 queryBuilderCore，但暴露出的方法集不同，从而实现了流式 API 的引导。
type QueryBuilder[O Owner] struct {
	*queryBuilderCore[O]
}

// queryBuilderCore 存储了查询构建的所有状态和配置。
// 架构意图：它是所有构建器变体的单一状态来源，确保了状态转移的原子性和一致性。
type queryBuilderCore[O Owner] struct {
	spec     QuerySpec[O] // 查询定义的详细规范
	buildErr error        // 构建过程中遇到的第一个错误，采取“错误冒泡”策略
	phase    builderPhase // 当前状态机所处的阶段
}

// join represents any type of JOIN operation.
type join struct {
	joinType JoinType
	table    Table
	on       []Condition
}

type setOperation[O Owner] struct {
	op   SetOperationType
	spec QuerySpec[O]
}

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

func ensureQueryBuilderCore[O Owner](
	core *queryBuilderCore[O],
	defaultPhase builderPhase,
	nilReceiver bool,
) *queryBuilderCore[O] {
	if core == nil {
		core = newQueryBuilderCore[O](defaultPhase)
		if nilReceiver {
			core.buildErr = errors.New("query builder cannot be nil")
		}

		return core
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
		return nil
	}

	return qb.queryBuilderCore
}

func (qb *SelectBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		return nil
	}

	return qb.queryBuilderCore
}

func (qb *FromBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		return nil
	}

	return qb.queryBuilderCore
}

func (qb *WhereQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		return nil
	}

	return qb.queryBuilderCore
}

func (qb *SearchQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		return nil
	}

	return qb.queryBuilderCore
}

func (qb *FilteredQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		return nil
	}

	return qb.queryBuilderCore
}

func (qb *GroupedQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		return nil
	}

	return qb.queryBuilderCore
}

func (qb *HavingQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		return nil
	}

	return qb.queryBuilderCore
}

func (qb *CompoundQueryBuilder[O]) core() *queryBuilderCore[O] {
	if qb == nil {
		return nil
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
	core.setBuildError(errors.Errorf("%s is not available in %s state", method, core.phase))
}

func (core *queryBuilderCore[O]) addSelectColumns(cols ...BoundColumn[O]) {
	for _, col := range cols {
		if err := validateBoundColumn(col); err != nil {
			core.setBuildError(errors.Trace(err))
			continue
		}

		core.spec.Selects = append(core.spec.Selects, col)
	}
}

func (core *queryBuilderCore[O]) appendColumn(target *[]SQLColumn, col SQLColumn) {
	if _, err := validateColumnInput(col); err != nil {
		core.setBuildError(errors.Trace(err))
		return
	}

	*target = append(*target, col)
}

func (core *queryBuilderCore[O]) appendSearchColumn(target *[]SearchColumn, col SearchColumn) {
	if err := validateSearchColumn(col); err != nil {
		core.setBuildError(errors.Trace(err))
		return
	}

	*target = append(*target, col)
}

func (core *queryBuilderCore[O]) appendCondition(target *[]Condition, cond Condition) {
	if _, _, _, err := validateConditionInput(cond); err != nil {
		core.setBuildError(errors.Trace(err))
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
		core.setBuildError(errors.Trace(err))
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

func (core *queryBuilderCore[O]) addJoin(joinType JoinType, table Table, conds ...Condition) {
	if core.buildErr != nil {
		return
	}

	if core.phase != builderPhaseBase {
		core.failTransition(string(joinType))
		return
	}

	if err := validateTableInput(table, "join table"); err != nil {
		core.setBuildError(errors.Trace(err))
		return
	}

	on := make([]Condition, 0, len(conds))
	for _, cond := range conds {
		if _, _, _, err := validateConditionInput(cond); err != nil {
			core.setBuildError(errors.Trace(err))
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
		core.setBuildError(errors.Trace(err))
		return
	}

	core.spec.Joins = append(core.spec.Joins, join{
		joinType: CrossJoinType,
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
		builderPhaseCompound:
		return true
	default:
		return false
	}
}

func (core *queryBuilderCore[O]) appendSetOperation(op SetOperationType, other completeQueryStage[O]) {
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

	otherCore := ensureQueryBuilderCore(other.core(), builderPhaseBase, false)
	if otherCore.buildErr != nil {
		core.setBuildError(errors.Trace(otherCore.buildErr))
		return
	}

	if !otherCore.isComplete() {
		core.setBuildError(errors.Errorf("set operation %s requires a complete query", op))
		return
	}

	if len(core.spec.Selects) == 0 || len(otherCore.spec.Selects) == 0 {
		core.setBuildError(errors.New("set operations require both queries to select at least one column"))
		return
	}

	if len(core.spec.Selects) != len(otherCore.spec.Selects) {
		core.setBuildError(errors.Errorf(
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

// From sets the base table for a SELECT-first builder.
func (qb *SelectBuilder[O]) From(table Table) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseNeedFrom, qb == nil)
	core.setFrom(table)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// Select sets the projected columns for a FROM-first builder.
func (qb *FromBuilder[O]) Select(cols ...BoundColumn[O]) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseNeedSelect, qb == nil)
	core.setSelect(cols...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// Join adds an INNER JOIN clause.
func (qb *QueryBuilder[O]) Join(table Table, conds ...Condition) *QueryBuilder[O] {
	return qb.InnerJoin(table, conds...)
}

// LeftJoin adds a LEFT JOIN clause with ON conditions joined by AND.
func (qb *QueryBuilder[O]) LeftJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.addJoin(LeftJoinType, table, conds...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// InnerJoin adds an INNER JOIN clause with ON conditions joined by AND.
func (qb *QueryBuilder[O]) InnerJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.addJoin(InnerJoinType, table, conds...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// RightJoin adds a RIGHT JOIN clause with ON conditions joined by AND.
func (qb *QueryBuilder[O]) RightJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.addJoin(RightJoinType, table, conds...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// FullJoin adds a FULL JOIN clause with ON conditions joined by AND.
func (qb *QueryBuilder[O]) FullJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.addJoin(FullJoinType, table, conds...)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// CrossJoin adds a CROSS JOIN clause.
func (qb *QueryBuilder[O]) CrossJoin(table Table) *QueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.addCrossJoin(table)

	return &QueryBuilder[O]{queryBuilderCore: core}
}

// Where sets the WHERE clause for the query.
func (qb *QueryBuilder[O]) Where(conds ...Condition) *WhereQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.setWhere(conds...)

	return &WhereQueryBuilder[O]{queryBuilderCore: core}
}

// Search sets keyword-search columns for the query.
func (qb *QueryBuilder[O]) Search(cols ...SearchColumn) *SearchQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.setSearch(cols...)

	return &SearchQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause for the query.
func (qb *QueryBuilder[O]) GroupBy(cols ...SQLColumn) *GroupedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.setGroupBy(cols...)

	return &GroupedQueryBuilder[O]{queryBuilderCore: core}
}

// Where sets the WHERE clause after keyword-search columns are fixed.
func (qb *SearchQueryBuilder[O]) Where(conds ...Condition) *FilteredQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch, qb == nil)
	core.setWhere(conds...)

	return &FilteredQueryBuilder[O]{queryBuilderCore: core}
}

// Search sets keyword-search columns after WHERE is fixed.
func (qb *WhereQueryBuilder[O]) Search(cols ...SearchColumn) *FilteredQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere, qb == nil)
	core.setSearch(cols...)

	return &FilteredQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after WHERE is fixed.
func (qb *WhereQueryBuilder[O]) GroupBy(cols ...SQLColumn) *GroupedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere, qb == nil)
	core.setGroupBy(cols...)

	return &GroupedQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after keyword search is fixed.
func (qb *SearchQueryBuilder[O]) GroupBy(cols ...SQLColumn) *GroupedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch, qb == nil)
	core.setGroupBy(cols...)

	return &GroupedQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after all filters are fixed.
func (qb *FilteredQueryBuilder[O]) GroupBy(cols ...SQLColumn) *GroupedQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered, qb == nil)
	core.setGroupBy(cols...)

	return &GroupedQueryBuilder[O]{queryBuilderCore: core}
}

// Having sets the HAVING clause for the grouped query.
func (qb *GroupedQueryBuilder[O]) Having(conds ...Condition) *HavingQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped, qb == nil)
	core.setHaving(conds...)

	return &HavingQueryBuilder[O]{queryBuilderCore: core}
}

func buildQuery[O Owner](core *queryBuilderCore[O]) (*Query[O], error) {
	if core == nil {
		return nil, errors.New("query builder cannot be nil")
	}

	core = ensureQueryBuilderCore(core, core.phase, false)
	if core.buildErr != nil {
		return nil, errors.Trace(core.buildErr)
	}

	plan, err := buildQueryPlan(core.spec)
	if err != nil {
		return nil, errors.Trace(err)
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

// Build builds and validates the query.
func (qb *QueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build builds and validates the query.
func (qb *WhereQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build builds and validates the query.
func (qb *SearchQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build builds and validates the query.
func (qb *FilteredQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build builds and validates the query.
func (qb *GroupedQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build builds and validates the query.
func (qb *HavingQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build builds and validates the query.
func (qb *CompoundQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Union appends a UNION clause to the current query.
func (qb *QueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.appendSetOperation(UnionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *WhereQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere, qb == nil)
	core.appendSetOperation(UnionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *SearchQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch, qb == nil)
	core.appendSetOperation(UnionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *FilteredQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered, qb == nil)
	core.appendSetOperation(UnionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *GroupedQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped, qb == nil)
	core.appendSetOperation(UnionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *HavingQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving, qb == nil)
	core.appendSetOperation(UnionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *CompoundQueryBuilder[O]) Union(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound, qb == nil)
	core.appendSetOperation(UnionType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *QueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.appendSetOperation(UnionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *WhereQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere, qb == nil)
	core.appendSetOperation(UnionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *SearchQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch, qb == nil)
	core.appendSetOperation(UnionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *FilteredQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered, qb == nil)
	core.appendSetOperation(UnionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *GroupedQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped, qb == nil)
	core.appendSetOperation(UnionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *HavingQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving, qb == nil)
	core.appendSetOperation(UnionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *CompoundQueryBuilder[O]) UnionAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound, qb == nil)
	core.appendSetOperation(UnionAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *QueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.appendSetOperation(IntersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *WhereQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere, qb == nil)
	core.appendSetOperation(IntersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *SearchQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch, qb == nil)
	core.appendSetOperation(IntersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *FilteredQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered, qb == nil)
	core.appendSetOperation(IntersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *GroupedQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped, qb == nil)
	core.appendSetOperation(IntersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *HavingQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving, qb == nil)
	core.appendSetOperation(IntersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *CompoundQueryBuilder[O]) Intersect(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound, qb == nil)
	core.appendSetOperation(IntersectType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *QueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.appendSetOperation(IntersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *WhereQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere, qb == nil)
	core.appendSetOperation(IntersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *SearchQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch, qb == nil)
	core.appendSetOperation(IntersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *FilteredQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered, qb == nil)
	core.appendSetOperation(IntersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *GroupedQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped, qb == nil)
	core.appendSetOperation(IntersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *HavingQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving, qb == nil)
	core.appendSetOperation(IntersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *CompoundQueryBuilder[O]) IntersectAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound, qb == nil)
	core.appendSetOperation(IntersectAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *QueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.appendSetOperation(ExceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *WhereQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere, qb == nil)
	core.appendSetOperation(ExceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *SearchQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch, qb == nil)
	core.appendSetOperation(ExceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *FilteredQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered, qb == nil)
	core.appendSetOperation(ExceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *GroupedQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped, qb == nil)
	core.appendSetOperation(ExceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *HavingQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving, qb == nil)
	core.appendSetOperation(ExceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *CompoundQueryBuilder[O]) Except(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound, qb == nil)
	core.appendSetOperation(ExceptType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *QueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase, qb == nil)
	core.appendSetOperation(ExceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *WhereQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere, qb == nil)
	core.appendSetOperation(ExceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *SearchQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch, qb == nil)
	core.appendSetOperation(ExceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *FilteredQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered, qb == nil)
	core.appendSetOperation(ExceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *GroupedQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped, qb == nil)
	core.appendSetOperation(ExceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *HavingQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving, qb == nil)
	core.appendSetOperation(ExceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *CompoundQueryBuilder[O]) ExceptAll(other completeQueryStage[O]) *CompoundQueryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound, qb == nil)
	core.appendSetOperation(ExceptAllType, other)

	return &CompoundQueryBuilder[O]{queryBuilderCore: core}
}
