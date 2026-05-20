package tsq

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *queryBuilder[O]) ForUpdate() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setLockStrength(queryLockStrengthUpdate)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *whereQueryBuilder[O]) ForUpdate() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setLockStrength(queryLockStrengthUpdate)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *searchQueryBuilder[O]) ForUpdate() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.setLockStrength(queryLockStrengthUpdate)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *filteredQueryBuilder[O]) ForUpdate() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setLockStrength(queryLockStrengthUpdate)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *groupedQueryBuilder[O]) ForUpdate() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setLockStrength(queryLockStrengthUpdate)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *havingQueryBuilder[O]) ForUpdate() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.setLockStrength(queryLockStrengthUpdate)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForUpdate adds a FOR UPDATE row-lock clause to the query.
func (qb *compoundQueryBuilder[O]) ForUpdate() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.setLockStrength(queryLockStrengthUpdate)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *queryBuilder[O]) ForShare() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseBase)
	core.setLockStrength(queryLockStrengthShare)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *whereQueryBuilder[O]) ForShare() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseWhere)
	core.setLockStrength(queryLockStrengthShare)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *searchQueryBuilder[O]) ForShare() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseKwSearch)
	core.setLockStrength(queryLockStrengthShare)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *filteredQueryBuilder[O]) ForShare() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseFiltered)
	core.setLockStrength(queryLockStrengthShare)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *groupedQueryBuilder[O]) ForShare() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseGrouped)
	core.setLockStrength(queryLockStrengthShare)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *havingQueryBuilder[O]) ForShare() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseHaving)
	core.setLockStrength(queryLockStrengthShare)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// ForShare adds a FOR SHARE row-lock clause to the query.
func (qb *compoundQueryBuilder[O]) ForShare() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseCompound)
	core.setLockStrength(queryLockStrengthShare)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// NoWait adds NOWAIT to a locked query.
func (qb *lockedQueryBuilder[O]) NoWait() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseLocked)
	core.setLockWaitMode(queryLockWaitNoWait)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}

// SkipLocked adds SKIP LOCKED to a locked query.
func (qb *lockedQueryBuilder[O]) SkipLocked() LockedStage[O] {
	core := ensureQueryBuilderCore(qb.core(), builderPhaseLocked)
	core.setLockWaitMode(queryLockWaitSkipLocked)

	return &lockedQueryBuilder[O]{queryBuilderCore: core}
}
