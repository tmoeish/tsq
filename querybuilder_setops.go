package tsq

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
