package tsq

// Union appends a UNION clause to the current query.
func (qb *QueryBuilder[O]) Union(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(unionType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *whereQueryBuilder[O]) Union(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(unionType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *searchQueryBuilder[O]) Union(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(unionType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *filteredQueryBuilder[O]) Union(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(unionType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *groupedQueryBuilder[O]) Union(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(unionType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *havingQueryBuilder[O]) Union(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(unionType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Union appends a UNION clause to the current query.
func (qb *compoundQueryBuilder[O]) Union(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(unionType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *QueryBuilder[O]) UnionAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(unionAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *whereQueryBuilder[O]) UnionAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(unionAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *searchQueryBuilder[O]) UnionAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(unionAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *filteredQueryBuilder[O]) UnionAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(unionAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *groupedQueryBuilder[O]) UnionAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(unionAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *havingQueryBuilder[O]) UnionAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(unionAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *compoundQueryBuilder[O]) UnionAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(unionAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *QueryBuilder[O]) Intersect(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(intersectType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *whereQueryBuilder[O]) Intersect(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(intersectType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *searchQueryBuilder[O]) Intersect(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(intersectType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *filteredQueryBuilder[O]) Intersect(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(intersectType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *groupedQueryBuilder[O]) Intersect(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(intersectType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *havingQueryBuilder[O]) Intersect(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(intersectType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *compoundQueryBuilder[O]) Intersect(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(intersectType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *QueryBuilder[O]) IntersectAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(intersectAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *whereQueryBuilder[O]) IntersectAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(intersectAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *searchQueryBuilder[O]) IntersectAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(intersectAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *filteredQueryBuilder[O]) IntersectAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(intersectAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *groupedQueryBuilder[O]) IntersectAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(intersectAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *havingQueryBuilder[O]) IntersectAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(intersectAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *compoundQueryBuilder[O]) IntersectAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(intersectAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *QueryBuilder[O]) Except(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(exceptType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *whereQueryBuilder[O]) Except(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(exceptType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *searchQueryBuilder[O]) Except(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(exceptType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *filteredQueryBuilder[O]) Except(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(exceptType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *groupedQueryBuilder[O]) Except(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(exceptType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *havingQueryBuilder[O]) Except(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(exceptType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// Except appends an EXCEPT clause to the current query.
func (qb *compoundQueryBuilder[O]) Except(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(exceptType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *QueryBuilder[O]) ExceptAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.appendSetOperation(exceptAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *whereQueryBuilder[O]) ExceptAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.appendSetOperation(exceptAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *searchQueryBuilder[O]) ExceptAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.appendSetOperation(exceptAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *filteredQueryBuilder[O]) ExceptAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.appendSetOperation(exceptAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *groupedQueryBuilder[O]) ExceptAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.appendSetOperation(exceptAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *havingQueryBuilder[O]) ExceptAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.appendSetOperation(exceptAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *compoundQueryBuilder[O]) ExceptAll(other QueryStage[O]) CompoundStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.appendSetOperation(exceptAllType, other)

	return &compoundQueryBuilder[O]{queryBuilderCore: core}
}
