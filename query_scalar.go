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

// QueryInt executes the query and returns a single integer result.
// Deprecated: use Query.Scalar with an integer selected column.
func (q *Query[O]) QueryInt(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int64, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (int64, error) {
		return q.scalarValue[int64](ctx, tx, args...)
	})
}

// QueryFloat executes the query and returns a single float result.
// Deprecated: use Query.Scalar with a floating-point selected column.
func (q *Query[O]) QueryFloat(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (float64, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (float64, error) {
		return q.scalarValue[float64](ctx, tx, args...)
	})
}

// QueryString executes the query and returns a single string result.
// Deprecated: use Query.Scalar with a string selected column.
func (q *Query[O]) QueryString(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (string, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (string, error) {
		return q.scalarValue[string](ctx, tx, args...)
	})
}

// Count executes the count query and returns the number of matching records.
// The result is truncated to int; use Count64 when an int64 is required.
func (q *Query[O]) Count(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (int, error) {
		return q.count(ctx, tx, args...)
	})
}

// Count64 executes the count query and returns the number of matching records as int64.
// This avoids truncation on large result sets or 32-bit platforms.
func (q *Query[O]) Count64(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int64, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (int64, error) {
		return q.count64(ctx, tx, args...)
	})
}

func (q *Query[O]) count(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int, error) {
	n, err := q.count64(ctx, tx, args...)
	return int(n), err
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

// Exists reports whether any records match the query conditions.
func (q *Query[O]) Exists(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (bool, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (bool, error) {
		return q.exist(ctx, tx, args...)
	})
}

func (q *Query[O]) exist(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (bool, error) {
	if err := validateQuery(q); err != nil {
		return false, err
	}

	resolvedSQL, finalArgs, err := resolveQueryWithState(q.cntSQL, q.cntArgs, args, "", q.cntArgState)
	if err != nil {
		return false, err
	}

	if err := validateOperationalExecutorForSQL(tx, resolvedSQL); err != nil {
		return false, err
	}

	sqlText := renderSQLForExecutor(tx, resolvedSQL)

	logSQLForExecutor(ctx, tx, "exist", sqlText, finalArgs)

	count, err := queryInt64(ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", "failed to check record existence", err)
	}

	return count > 0, nil
}
