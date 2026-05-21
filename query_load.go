package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// Page executes a paginated query with the given page parameters
func Page[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	page *PageRequest,
	q *Query[O],
	args ...any,
) (*PageResponse[O], error) {
	return trace1(ctx, func(ctx context.Context) (*PageResponse[O], error) {
		return pageFn(ctx, tx, page, q, args...)
	})
}

func pageFn[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	page *PageRequest,
	q *Query[O],
	args ...any,
) (*PageResponse[O], error) {
	if err := validateQuery(q); err != nil {
		return nil, err
	}

	page = normalizePageReq(page)

	cntSQL, listSQL, err := q.buildPageSQLs(page)
	if err != nil {
		return nil, err
	}

	queryBaseArgs := q.listArgs
	countBaseArgs := q.cntArgs
	queryArgState := q.listArgState
	countArgState := q.cntArgState

	if len(q.kwCols) > 0 && len(page.Keyword) > 0 {
		queryBaseArgs = q.kwListArgs
		countBaseArgs = q.kwCntArgs
		queryArgState = q.kwListArgState
		countArgState = q.kwCntArgState
	}

	resolvedListSQL, finalArgs, err := resolveQueryWithState(listSQL, queryBaseArgs, args, page.Keyword, queryArgState)
	if err != nil {
		return nil, err
	}

	resolvedCntSQL, countArgs, err := resolveQueryWithState(cntSQL, countBaseArgs, args, page.Keyword, countArgState)
	if err != nil {
		return nil, err
	}

	if err := validateOperationalExecutorForSQL(tx, resolvedCntSQL, resolvedListSQL); err != nil {
		return nil, err
	}

	renderedCntSQL := renderSQLForExecutor(tx, resolvedCntSQL)
	renderedListSQL := renderSQLForExecutor(tx, resolvedListSQL)

	if err := validateScanDestForType(q.selectCols, renderedListSQL, finalArgs); err != nil {
		return nil, err
	}

	argsWithLimit := make([]any, 0, len(finalArgs)+2)
	argsWithLimit = append(argsWithLimit, finalArgs...)
	argsWithLimit = append(argsWithLimit, page.Size, page.Offset())

	if ctx.Value(printSQL) != nil {
		slog.Info("count", "sql", renderedCntSQL, "args", CompactJSON(countArgs))
		slog.Info("list", "sql", renderedListSQL, "args", CompactJSON(argsWithLimit))
	}

	count, err := queryInt64(ctx, tx, renderedCntSQL, countArgs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to execute count query", err)
	}

	rows, err := tx.QueryContext(ctx, renderedListSQL, argsWithLimit...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to execute paginated query", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("Failed to close rows", "error", closeErr)
		}
	}()

	list := make([]*O, 0, page.Size)

	for rows.Next() {
		r := new(O)

		dest, err := buildScanDest(q.selectCols, r)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", "failed to execute paginated query", err)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("%s: %w", "failed to execute paginated query", err)
		}

		list = append(list, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to execute paginated query", err)
	}

	return NewPageResponse(page, count, list), nil
}

// List executes q and returns all matching rows.
func List[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	qb *Query[O],
	args ...any,
) ([]*O, error) {
	return trace1(ctx, func(ctx context.Context) ([]*O, error) {
		return listFn(ctx, tx, qb, args...)
	})
}

func listFn[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	q *Query[O],
	args ...any,
) ([]*O, error) {
	if err := validateQuery(q); err != nil {
		return nil, err
	}

	resolvedSQL, finalArgs, err := resolveQueryWithState(q.listSQL, q.listArgs, args, "", q.listArgState)
	if err != nil {
		return nil, err
	}

	if err := validateOperationalExecutorForSQL(tx, resolvedSQL); err != nil {
		return nil, err
	}

	sqlText := renderSQLForExecutor(tx, resolvedSQL)

	if err := validateScanDestForType(q.selectCols, sqlText, finalArgs); err != nil {
		return nil, err
	}

	if ctx.Value(printSQL) != nil {
		slog.Info("list", "sql", sqlText, "args", CompactJSON(finalArgs))
	}

	rows, err := tx.QueryContext(ctx, sqlText, finalArgs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to execute list query", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("Failed to close rows", "error", closeErr)
		}
	}()

	var list []*O

	for rows.Next() {
		r := new(O)

		dest, err := buildScanDest(q.selectCols, r)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", "failed to execute list query", err)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("%s: %w", "failed to execute list query", err)
		}

		list = append(list, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to execute list query", err)
	}

	return list, nil
}

// GetOrErr executes q and returns one row or sql.ErrNoRows.
func GetOrErr[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	qb *Query[O],
	args ...any,
) (*O, error) {
	return trace1(ctx, func(ctx context.Context) (*O, error) {
		return getOrErrFn(ctx, tx, qb, args...)
	})
}

func getOrErrFn[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	qb *Query[O],
	args ...any,
) (*O, error) {
	if err := validateQuery(qb); err != nil {
		return nil, err
	}

	resolvedSQL, finalArgs, err := resolveQueryWithState(qb.listSQL, qb.listArgs, args, "", qb.listArgState)
	if err != nil {
		return nil, err
	}

	if err := validateOperationalExecutorForSQL(tx, resolvedSQL); err != nil {
		return nil, err
	}

	sqlText := renderSQLForExecutor(tx, resolvedSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("getOrErr", "sql", sqlText, "args", CompactJSON(finalArgs))
	}

	r := new(O)

	dest, err := buildScanDest(qb.selectCols, r)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to execute select query", err)
	}

	row := tx.QueryRowContext(ctx, sqlText, finalArgs...)

	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}

		return nil, fmt.Errorf("%s: %w", "failed to execute select query", err)
	}

	return r, nil
}

// Get executes q and returns one row or nil when no row matches.
func Get[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	qb *Query[O],
	args ...any,
) (*O, error) {
	row, err := GetOrErr(ctx, tx, qb, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	return row, nil
}

// Load executes q and scans one row into holder.
func (q *Query[O]) Load(
	ctx context.Context,
	tx SQLExecutor,
	holder *O,
	args ...any,
) error {
	return trace(ctx, func(ctx context.Context) error {
		if err := validateQuery(q); err != nil {
			return err
		}

		resolvedSQL, finalArgs, err := resolveQueryWithState(q.listSQL, q.listArgs, args, "", q.listArgState)
		if err != nil {
			return err
		}

		if err := validateOperationalExecutorForSQL(tx, resolvedSQL); err != nil {
			return err
		}

		sqlText := renderSQLForExecutor(tx, resolvedSQL)

		if ctx.Value(printSQL) != nil {
			slog.Info("load", "sql", sqlText, "args", CompactJSON(finalArgs))
		}

		dest, err := buildScanDest(q.selectCols, holder)
		if err != nil {
			return fmt.Errorf("%s: %w", "failed to execute select query", err)
		}

		row := tx.QueryRowContext(ctx, sqlText, finalArgs...)
		if err := row.Scan(dest...); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return sql.ErrNoRows
			}

			return fmt.Errorf("%s: %w", "failed to execute select query", err)
		}

		return nil
	})
}

func (q *Query[O]) buildPageSQLs(page *PageRequest) (string, string, error) {
	if err := validateQuery(q); err != nil {
		return "", "", err
	}

	page = normalizePageReq(page)

	var cntQuery, listQuery string
	if len(q.kwCols) > 0 && len(page.Keyword) > 0 {
		cntQuery = q.kwCntSQL
		listQuery = q.kwListSQL
	} else {
		cntQuery = q.cntSQL
		listQuery = q.listSQL
	}

	allowedFields := make(map[string]string)
	ambiguousFields := make(map[string]struct{})
	registerSortableField := func(key, qualifiedName string) {
		if key == "" {
			return
		}

		if _, ok := ambiguousFields[key]; ok {
			return
		}

		if existing, ok := allowedFields[key]; ok {
			if existing != qualifiedName {
				delete(allowedFields, key)
				ambiguousFields[key] = struct{}{}
			}

			return
		}

		allowedFields[key] = qualifiedName
	}

	for _, f := range q.selectCols {
		sortExpr := rawColumnQualifiedName(f)
		if q.hasSetOps {
			sortExpr = rawIdentifier(f.OutputName())
		}

		registerSortableField(f.OutputName(), sortExpr)

		if f.JSONFieldName() != "" && f.JSONFieldName() != "-" {
			jsonSortExpr := rawColumnQualifiedName(f)
			if q.hasSetOps {
				jsonSortExpr = rawIdentifier(f.JSONFieldName())
			}

			registerSortableField(f.JSONFieldName(), jsonSortExpr)
		}
	}

	if len(page.OrderBy) != 0 {
		orderbys := splitCommaValues(page.OrderBy)
		if len(orderbys) == 0 {
			return "", "", errors.New("order by fields cannot be empty")
		}

		orders, err := normalizeSortOrders(splitCommaValues(page.Order), len(orderbys))
		if err != nil {
			return "", "", err
		}

		var fullNames []string

		for i, ob := range orderbys {
			ob = strings.TrimSpace(ob)
			if _, ok := ambiguousFields[ob]; ok {
				return "", "", newErrAmbiguousSortField(ob)
			}

			fullName, ok := allowedFields[ob]
			if !ok {
				return "", "", newErrUnknownSortField(ob)
			}

			fullNames = append(fullNames, fullName+" "+string(orders[i]))
		}

		listQuery += "\nORDER BY " + strings.Join(fullNames, ", ")
	}

	bodySQL, lockClause := splitTrailingQueryLockClause(listQuery)

	listQuery = bodySQL + "\nLIMIT ? OFFSET ?"
	if lockClause != "" {
		listQuery += "\n" + lockClause
	}

	return cntQuery, listQuery, nil
}

func splitCommaValues(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		result = append(result, part)
	}

	return result
}
