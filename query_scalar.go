package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
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

	if ctx.Value(printSQL) != nil {
		slog.Info(methodName, "sql", sqlText, "args", compactJSON(finalArgs))
	}

	return sqlText, finalArgs, nil
}

func queryInt64(ctx context.Context, tx SQLExecutor, sqlText string, args ...any) (int64, error) {
	var result sql.NullInt64
	if err := tx.QueryRowContext(ctx, sqlText, args...).Scan(&result); err != nil {
		return 0, err
	}

	return result.Int64, nil
}

func queryFloat64(ctx context.Context, tx SQLExecutor, sqlText string, args ...any) (float64, error) {
	var result sql.NullFloat64
	if err := tx.QueryRowContext(ctx, sqlText, args...).Scan(&result); err != nil {
		return 0, err
	}

	return result.Float64, nil
}

func queryString(ctx context.Context, tx SQLExecutor, sqlText string, args ...any) (string, error) {
	var result sql.NullString
	if err := tx.QueryRowContext(ctx, sqlText, args...).Scan(&result); err != nil {
		return "", err
	}

	return result.String, nil
}

// QueryInt executes the query and returns a single integer result.
func (q *Query[O]) QueryInt(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int64, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (int64, error) {
		return q.queryInt(ctx, tx, args...)
	})
}

func (q *Query[O]) queryInt(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int64, error) {
	sqlText, finalArgs, err := q.prepareQueryExecution(ctx, tx, "queryInt", args...)
	if err != nil {
		return 0, err
	}

	result, err := queryInt64(ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", "failed to execute select query", err)
	}

	return result, nil
}

// QueryFloat executes the query and returns a single float result.
func (q *Query[O]) QueryFloat(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (float64, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (float64, error) {
		return q.queryFloat(ctx, tx, args...)
	})
}

func (q *Query[O]) queryFloat(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (float64, error) {
	sqlText, finalArgs, err := q.prepareQueryExecution(ctx, tx, "queryFloat", args...)
	if err != nil {
		return 0, err
	}

	result, err := queryFloat64(ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", "failed to execute select query", err)
	}

	return result, nil
}

// QueryString executes the query and returns a single string result.
func (q *Query[O]) QueryString(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (string, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (string, error) {
		return q.queryStr(ctx, tx, args...)
	})
}

func (q *Query[O]) queryStr(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (string, error) {
	sqlText, finalArgs, err := q.prepareQueryExecution(ctx, tx, "queryStr", args...)
	if err != nil {
		return "", err
	}

	result, err := queryString(ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return "", fmt.Errorf("%s: %w", "failed to execute select query", err)
	}

	return result, nil
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

	if ctx.Value(printSQL) != nil {
		slog.Info("count", "sql", sqlText, "args", compactJSON(finalArgs))
	}

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

	if ctx.Value(printSQL) != nil {
		slog.Info("exist", "sql", sqlText, "args", compactJSON(finalArgs))
	}

	count, err := queryInt64(ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", "failed to check record existence", err)
	}

	return count > 0, nil
}
