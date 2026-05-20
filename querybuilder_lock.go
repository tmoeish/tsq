package tsq

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
