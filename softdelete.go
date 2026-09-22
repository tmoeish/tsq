package tsq

import "context"

// SoftDeleteTableOf is the descriptor of a table with a deleted_at column. It is
// a TableOf whose deletes are soft: Delete stamps the tombstone, queries leave
// deleted rows out, and only the Hard methods remove a row. A table without
// deleted_at is a plain TableOf, which has no Delete, so every statement that
// removes data says Hard.
//
// Generated code embeds it in place of *TableOf when the struct declares
// //tsq:managed deleted_at.
type SoftDeleteTableOf[R any, K comparable] struct {
	*TableOf[R, K]
}

// SoftDeleteTable is a table whose rows are R and whose deletes are soft: a
// *SoftDeleteTableOf, or a generated table struct that embeds one. DeleteFrom
// takes one; a table without deleted_at goes to HardDeleteFrom.
type SoftDeleteTable[R any] interface {
	RowTable[R]
	// needsDeletedAtOrHardDeleteFrom is what the compiler reports when a table
	// without deleted_at is passed to DeleteFrom.
	needsDeletedAtOrHardDeleteFrom()
}

// NewSoftDeleteTable starts the declaration of a soft-delete table named name.
// Bind its columns to the embedded TableOf; the table is unusable until Define
// completes it.
func NewSoftDeleteTable[R any, K comparable](name string) *SoftDeleteTableOf[R, K] {
	t := NewTable[R, K](name)
	t.def.softDelete = true

	return &SoftDeleteTableOf[R, K]{TableOf: t}
}

// Define completes the table with deletedAt as its tombstone column and returns
// it. A definition error is reported by every query and write that uses the
// table.
func (t *SoftDeleteTableOf[R, K]) Define(spec TableSpec[R, K], deletedAt BoundColumn[R]) *SoftDeleteTableOf[R, K] {
	t.define(spec, deletedAt)

	return t
}

// WithDeleted returns the table without its live-row filter, and changes nothing
// else: queries, joins, UpdateTable and Update reach deleted rows, and Delete and
// DeleteFrom stamp them again. A delete through it is still soft; removing a row
// is HardDelete.
func (t *SoftDeleteTableOf[R, K]) WithDeleted() *SoftDeleteTableOf[R, K] {
	return &SoftDeleteTableOf[R, K]{TableOf: t.withDeleted()}
}

// As returns the table under an alias, for joining it more than once; see
// TableOf.As.
func (t *SoftDeleteTableOf[R, K]) As(alias string) *SoftDeleteTableOf[R, K] {
	return &SoftDeleteTableOf[R, K]{TableOf: t.TableOf.As(alias)}
}

func (t *SoftDeleteTableOf[R, K]) needsDeletedAtOrHardDeleteFrom() {}

// Delete soft-deletes row: an update that stamps the tombstone and refreshes
// updated_at, so the version check applies. Only a live row matches, unless the
// table is WithDeleted. Remove the row with HardDelete.
func (t *SoftDeleteTableOf[R, K]) Delete(ctx context.Context, db Executor, row *R) error {
	return t.BatchDelete(ctx, db, []*R{row}, WithBatchSize(1))
}

// BatchDelete soft-deletes rows as Delete does, in as few statements as the batch
// size allows.
func (t *SoftDeleteTableOf[R, K]) BatchDelete(ctx context.Context, db Executor, rows []*R, options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpDelete), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		return t.setTombstone(ctx, db, rows, config, true)
	})
}

// BatchDeleteByPK soft-deletes the rows whose primary key is in keys. Rows
// already deleted keep their tombstone, unless the table is WithDeleted. It does
// not check versions, but it increments them.
func (t *SoftDeleteTableOf[R, K]) BatchDeleteByPK(ctx context.Context, db Executor, keys []K, options ...BatchOption) error {
	return t.deleteByPK(ctx, db, keys, options, true)
}

// Restore clears the tombstone of a soft-deleted row, refreshing updated_at and
// incrementing version. Only a deleted row matches; on a table with a version
// column a row that is not deleted, or changed since it was loaded, fails with
// OptimisticLockError.
func (t *SoftDeleteTableOf[R, K]) Restore(ctx context.Context, db Executor, row *R) error {
	return t.BatchRestore(ctx, db, []*R{row}, WithBatchSize(1))
}

// BatchRestore restores rows in as few statements as the batch size allows.
func (t *SoftDeleteTableOf[R, K]) BatchRestore(ctx context.Context, db Executor, rows []*R, options ...BatchOption) error {
	return traceExecutor(ctx, db, t.traceInfo(TraceOpUpdate), func(ctx context.Context) error {
		config, err := newBatchConfig(options, false)
		if err != nil {
			return err
		}

		return t.setTombstone(ctx, db, rows, config, false)
	})
}
