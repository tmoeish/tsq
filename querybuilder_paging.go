package tsq

// ============================================================================
// ORDER BY / LIMIT / OFFSET
//
// Every complete stage can enter the paged stage, because SQL puts these clauses
// after whatever filtering, grouping and set operations came before. From the paged
// stage only row locking may still follow, which matches the SQL clause order.
// ============================================================================

// OrderBy sorts the result rows. Build Asc()/Desc() terms from typed columns.
func (qb *queryBuilder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setOrderBy(orders...)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Limit caps the number of returned rows.
func (qb *queryBuilder[O]) Limit(limit int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setLimit(limit)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Offset skips rows before returning any. It requires a Limit: a bare OFFSET is a
// syntax error on MySQL and SQLite.
func (qb *queryBuilder[O]) Offset(offset int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setOffset(offset)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// OrderBy sorts the result rows. Build Asc()/Desc() terms from typed columns.
func (qb *whereQueryBuilder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setOrderBy(orders...)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Limit caps the number of returned rows.
func (qb *whereQueryBuilder[O]) Limit(limit int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setLimit(limit)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Offset skips rows before returning any. It requires a Limit: a bare OFFSET is a
// syntax error on MySQL and SQLite.
func (qb *whereQueryBuilder[O]) Offset(offset int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setOffset(offset)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// OrderBy sorts the result rows. Build Asc()/Desc() terms from typed columns.
func (qb *searchQueryBuilder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseSearch)
	core.setOrderBy(orders...)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Limit caps the number of returned rows.
func (qb *searchQueryBuilder[O]) Limit(limit int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseSearch)
	core.setLimit(limit)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Offset skips rows before returning any. It requires a Limit: a bare OFFSET is a
// syntax error on MySQL and SQLite.
func (qb *searchQueryBuilder[O]) Offset(offset int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseSearch)
	core.setOffset(offset)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// OrderBy sorts the result rows. Build Asc()/Desc() terms from typed columns.
func (qb *filteredQueryBuilder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setOrderBy(orders...)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Limit caps the number of returned rows.
func (qb *filteredQueryBuilder[O]) Limit(limit int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setLimit(limit)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Offset skips rows before returning any. It requires a Limit: a bare OFFSET is a
// syntax error on MySQL and SQLite.
func (qb *filteredQueryBuilder[O]) Offset(offset int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setOffset(offset)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// OrderBy sorts the result rows. Build Asc()/Desc() terms from typed columns.
func (qb *groupedQueryBuilder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setOrderBy(orders...)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Limit caps the number of returned rows.
func (qb *groupedQueryBuilder[O]) Limit(limit int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setLimit(limit)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Offset skips rows before returning any. It requires a Limit: a bare OFFSET is a
// syntax error on MySQL and SQLite.
func (qb *groupedQueryBuilder[O]) Offset(offset int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setOffset(offset)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// OrderBy sorts the result rows. Build Asc()/Desc() terms from typed columns.
func (qb *havingQueryBuilder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.setOrderBy(orders...)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Limit caps the number of returned rows.
func (qb *havingQueryBuilder[O]) Limit(limit int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.setLimit(limit)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Offset skips rows before returning any. It requires a Limit: a bare OFFSET is a
// syntax error on MySQL and SQLite.
func (qb *havingQueryBuilder[O]) Offset(offset int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.setOffset(offset)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// OrderBy sorts the result rows. Build Asc()/Desc() terms from typed columns.
func (qb *compoundQueryBuilder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.setOrderBy(orders...)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Limit caps the number of returned rows.
func (qb *compoundQueryBuilder[O]) Limit(limit int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.setLimit(limit)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Offset skips rows before returning any. It requires a Limit: a bare OFFSET is a
// syntax error on MySQL and SQLite.
func (qb *compoundQueryBuilder[O]) Offset(offset int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.setOffset(offset)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// OrderBy sorts the result rows. Build Asc()/Desc() terms from typed columns.
func (qb *pagedQueryBuilder[O]) OrderBy(orders ...OrderBy) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhasePaged)
	core.setOrderBy(orders...)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Limit caps the number of returned rows.
func (qb *pagedQueryBuilder[O]) Limit(limit int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhasePaged)
	core.setLimit(limit)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Offset skips rows before returning any. It requires a Limit: a bare OFFSET is a
// syntax error on MySQL and SQLite.
func (qb *pagedQueryBuilder[O]) Offset(offset int) PagedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhasePaged)
	core.setOffset(offset)

	return &pagedQueryBuilder[O]{queryBuilderCore: core}
}

// Build compiles and validates the ordered/limited query shape.
func (qb *pagedQueryBuilder[O]) Build() (*Query[O], error) {
	return buildQuery(qb.core())
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *pagedQueryBuilder[O]) ForUpdate() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhasePaged)
	core.setLockStrength(queryLockStrengthUpdate)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *pagedQueryBuilder[O]) ForShare() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhasePaged)
	core.setLockStrength(queryLockStrengthShare)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}
