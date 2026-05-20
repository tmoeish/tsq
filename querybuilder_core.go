package tsq

import (
	"errors"
	"fmt"
)

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

	item := join{joinType: joinType, table: table, on: make([]Condition, 0, len(conds))}
	for _, cond := range conds {
		if _, _, _, err := validateConditionInput(cond); err != nil {
			core.setBuildError(err)
			return
		}

		item.on = append(item.on, cond)
	}

	core.spec.Joins = append(core.spec.Joins, item)
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

	core.spec.Joins = append(core.spec.Joins, join{joinType: crossJoinType, table: table})
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
	if len(core.spec.KeywordSearch) > 0 {
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

	if len(core.spec.Filters) > 0 {
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
