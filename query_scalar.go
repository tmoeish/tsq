package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// prepareQueryExecution handles common steps for all scalar query methods:
// validation, SQL rendering, debug printing, and argument merging.
// Returns (sqlText, finalArgs, error).
func (q *Query[O]) prepareQueryExecution(
	ctx context.Context,
	tx SQLExecutor,
	methodName string,
	args ...any,
) (string, []any, error) {
	if err := validateQuery(q); err != nil {
		return "", nil, err
	}

	resolvedSQL, finalArgs, err := resolveQueryWithState(q.listSQL, q.listArgs, args, "", q.listArgState)
	if err != nil {
		return "", nil, err
	}

	if err := validateOperationalExecutorForSQL(tx, resolvedSQL); err != nil {
		return "", nil, err
	}

	sqlText := renderSQLForExecutor(tx, resolvedSQL)

	logSQLForExecutor(ctx, tx, methodName, sqlText, finalArgs)

	return sqlText, finalArgs, nil
}

func queryScalar[T any](ctx context.Context, tx SQLExecutor, sqlText string, args ...any) (T, error) {
	var result sql.Null[T]
	if err := tx.QueryRowContext(ctx, sqlText, args...).Scan(&result); err != nil {
		var zero T
		return zero, err
	}

	return result.V, nil
}

func queryInt64(ctx context.Context, tx SQLExecutor, sqlText string, args ...any) (int64, error) {
	return queryScalar[int64](ctx, tx, sqlText, args...)
}

func (q *Query[O]) validateScalarSelection[T any](selected TypedColumn[O, T]) error {
	if err := validateQuery(q); err != nil {
		return err
	}

	if isNilValue(selected) {
		return errors.New("scalar selected column cannot be nil")
	}

	if len(q.selectCols) != 1 {
		return fmt.Errorf("scalar query must select exactly one column, got %d", len(q.selectCols))
	}

	actual, ok := q.selectCols[0].(typedColumnInternal[T])
	if !ok {
		return errors.New("scalar selected column type does not match the query column type")
	}

	if !sameSubqueryColumn(selected, actual) {
		return fmt.Errorf(
			"scalar query selected %s but expected %s",
			describeSubqueryColumn(actual),
			describeSubqueryColumn(selected),
		)
	}

	return nil
}

func (q *Query[O]) scalarValue[T any](ctx context.Context, tx SQLExecutor, args ...any) (T, error) {
	var zero T

	sqlText, finalArgs, err := q.prepareQueryExecution(ctx, tx, "scalar", args...)
	if err != nil {
		return zero, err
	}

	result, err := queryScalar[T](ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return zero, fmt.Errorf("failed to execute scalar query: %w", err)
	}

	return result, nil
}

// Scalar executes a single-column query and returns the selected column's Go type.
func (q *Query[O]) Scalar[T any](
	ctx context.Context,
	tx SQLExecutor,
	selected TypedColumn[O, T],
	args ...any,
) (T, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (T, error) {
		if err := q.validateScalarSelection(selected); err != nil {
			var zero T
			return zero, err
		}

		return q.scalarValue[T](ctx, tx, args...)
	})
}

// Count executes the count query and returns the number of matching records.
func (q *Query[O]) Count(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int64, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (int64, error) {
		return q.count64(ctx, tx, args...)
	})
}

func (q *Query[O]) count64(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int64, error) {
	if err := validateQuery(q); err != nil {
		return 0, err
	}

	resolvedSQL, finalArgs, err := resolveQueryWithState(q.cntSQL, q.cntArgs, args, "", q.cntArgState)
	if err != nil {
		return 0, err
	}

	if err := validateOperationalExecutorForSQL(tx, resolvedSQL); err != nil {
		return 0, err
	}

	sqlText := renderSQLForExecutor(tx, resolvedSQL)

	logSQLForExecutor(ctx, tx, "count", sqlText, finalArgs)

	count, err := queryInt64(ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", "failed to execute count query", err)
	}

	return count, nil
}

// Exists reports whether any record matches the query conditions.
//
// It runs the same single-row read as Find rather than counting: COUNT made the
// database visit every matching row to answer a question that the first row
// settles.
func (q *Query[O]) Exists(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (bool, error) {
	row, err := q.Find(ctx, tx, args...)
	if err != nil {
		return false, err
	}

	return row != nil, nil
}
