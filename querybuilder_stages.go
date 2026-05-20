package tsq

// From sets the base table for a SELECT-first builder.
func (qb *selectBuilder[O]) From(table Table) *queryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseNeedFrom)
	core.setFrom(table)

	return &queryBuilder[O]{queryBuilderCore: core}
}

// Select sets the projected columns for a FROM-first builder.
func (qb *fromBuilder[O]) Select(cols ...BoundColumn[O]) *queryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseNeedSelect)
	core.setSelect(cols...)

	return &queryBuilder[O]{queryBuilderCore: core}
}

// Join adds an INNER JOIN clause.
func (qb *queryBuilder[O]) Join(table Table, conds ...Condition) *queryBuilder[O] {
	return qb.InnerJoin(table, conds...)
}

// LeftJoin adds a LEFT JOIN clause with ON conditions joined by AND.
func (qb *queryBuilder[O]) LeftJoin(table Table, conds ...Condition) *queryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addJoin(leftJoinType, table, conds...)

	return &queryBuilder[O]{queryBuilderCore: core}
}

// InnerJoin adds an INNER JOIN clause with ON conditions joined by AND.
func (qb *queryBuilder[O]) InnerJoin(table Table, conds ...Condition) *queryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addJoin(innerJoinType, table, conds...)

	return &queryBuilder[O]{queryBuilderCore: core}
}

// RightJoin adds a RIGHT JOIN clause with ON conditions joined by AND.
func (qb *queryBuilder[O]) RightJoin(table Table, conds ...Condition) *queryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addJoin(rightJoinType, table, conds...)

	return &queryBuilder[O]{queryBuilderCore: core}
}

// FullJoin adds a FULL JOIN clause with ON conditions joined by AND.
func (qb *queryBuilder[O]) FullJoin(table Table, conds ...Condition) *queryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addJoin(fullJoinType, table, conds...)

	return &queryBuilder[O]{queryBuilderCore: core}
}

// CrossJoin adds a CROSS JOIN clause.
func (qb *queryBuilder[O]) CrossJoin(table Table) *queryBuilder[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.addCrossJoin(table)

	return &queryBuilder[O]{queryBuilderCore: core}
}

// Where sets the WHERE clause for the query.
func (qb *queryBuilder[O]) Where(conds ...Condition) WhereStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setWhere(conds...)

	return &whereQueryBuilder[O]{queryBuilderCore: core}
}

// Search sets keyword-search columns for the query.
func (qb *queryBuilder[O]) Search(cols ...SearchColumn) SearchStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setSearch(cols...)

	return &searchQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause for the query.
func (qb *queryBuilder[O]) GroupBy(cols ...SQLColumn) GroupedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setGroupBy(cols...)

	return &groupedQueryBuilder[O]{queryBuilderCore: core}
}

// Where sets the WHERE clause after keyword-search columns are fixed.
func (qb *searchQueryBuilder[O]) Where(conds ...Condition) FilteredStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseSearch)
	core.setWhere(conds...)

	return &filteredQueryBuilder[O]{queryBuilderCore: core}
}

// Search sets keyword-search columns after WHERE is fixed.
func (qb *whereQueryBuilder[O]) Search(cols ...SearchColumn) FilteredStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setSearch(cols...)

	return &filteredQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after WHERE is fixed.
func (qb *whereQueryBuilder[O]) GroupBy(cols ...SQLColumn) GroupedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setGroupBy(cols...)

	return &groupedQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after keyword search is fixed.
func (qb *searchQueryBuilder[O]) GroupBy(cols ...SQLColumn) GroupedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseSearch)
	core.setGroupBy(cols...)

	return &groupedQueryBuilder[O]{queryBuilderCore: core}
}

// GroupBy sets the GROUP BY clause after all filters are fixed.
func (qb *filteredQueryBuilder[O]) GroupBy(cols ...SQLColumn) GroupedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setGroupBy(cols...)

	return &groupedQueryBuilder[O]{queryBuilderCore: core}
}

// Having sets the HAVING clause for the grouped query.
func (qb *groupedQueryBuilder[O]) Having(conds ...Condition) HavingStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setHaving(conds...)

	return &havingQueryBuilder[O]{queryBuilderCore: core}
}

// Build compiles and validates the query shape.
//
// Build validates owner wiring, clause ordering, selected columns, and other
// dialect-independent structure. It intentionally does not reject dialect-
// specific capabilities that depend on the runtime executor, because the same
// built Query may later run against different registries or executors with
// different dialects. Capability checks that require the concrete executor
// dialect therefore happen during execution.
func (qb *queryBuilder[O]) Build() (*Query[O], error) {
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
func (qb *whereQueryBuilder[O]) Build() (*Query[O], error) {
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
func (qb *searchQueryBuilder[O]) Build() (*Query[O], error) {
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
func (qb *filteredQueryBuilder[O]) Build() (*Query[O], error) {
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
func (qb *groupedQueryBuilder[O]) Build() (*Query[O], error) {
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
func (qb *havingQueryBuilder[O]) Build() (*Query[O], error) {
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
func (qb *compoundQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// Build compiles and validates the locked query shape.
func (qb *lockedQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}
