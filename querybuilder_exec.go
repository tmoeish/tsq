package tsq

import "context"

func (core *queryBuilderCore[O]) build() (*Query[O], error) {
	return buildQuery(core)
}

// MustBuild compiles the query or panics if validation fails.
//
// MustBuild is intended for TSQ framework and generated-code use where TSQ
// controls and validates the query shape. User code should call Build and
// check the returned error.
func (core *queryBuilderCore[O]) MustBuild() *Query[O] {
	query, err := core.build()
	if err != nil {
		panic(err)
	}

	return query
}

// Get builds and executes the query, returning one row or nil when no row matches.
func (core *queryBuilderCore[O]) Get(ctx context.Context, tx SQLExecutor, args ...any) (*O, error) {
	query, err := core.build()
	if err != nil {
		return nil, err
	}

	return query.Get(ctx, tx, args...)
}

// GetOrErr builds and executes the query, returning one row or sql.ErrNoRows.
func (core *queryBuilderCore[O]) GetOrErr(ctx context.Context, tx SQLExecutor, args ...any) (*O, error) {
	query, err := core.build()
	if err != nil {
		return nil, err
	}

	return query.GetOrErr(ctx, tx, args...)
}

// Load builds and executes the query, scanning one row into holder.
func (core *queryBuilderCore[O]) Load(ctx context.Context, tx SQLExecutor, holder *O, args ...any) error {
	query, err := core.build()
	if err != nil {
		return err
	}

	return query.Load(ctx, tx, holder, args...)
}

// Exists builds and executes the query, reporting whether any rows match.
func (core *queryBuilderCore[O]) Exists(ctx context.Context, tx SQLExecutor, args ...any) (bool, error) {
	query, err := core.build()
	if err != nil {
		return false, err
	}

	return query.Exists(ctx, tx, args...)
}

// Count builds and executes the count query.
func (core *queryBuilderCore[O]) Count(ctx context.Context, tx SQLExecutor, args ...any) (int, error) {
	query, err := core.build()
	if err != nil {
		return 0, err
	}

	return query.Count(ctx, tx, args...)
}

// List builds and executes the list query.
func (core *queryBuilderCore[O]) List(ctx context.Context, tx SQLExecutor, args ...any) ([]*O, error) {
	query, err := core.build()
	if err != nil {
		return nil, err
	}

	return query.List(ctx, tx, args...)
}

// Page builds and executes the paginated query.
func (core *queryBuilderCore[O]) Page(
	ctx context.Context,
	tx SQLExecutor,
	page *PageRequest,
	args ...any,
) (*PageResponse[O], error) {
	query, err := core.build()
	if err != nil {
		return nil, err
	}

	return query.Page(ctx, tx, page, args...)
}
