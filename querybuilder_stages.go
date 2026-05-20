package tsq

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
