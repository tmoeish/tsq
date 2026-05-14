package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgconn"
	"github.com/lib/pq"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// ================================================
// 资源清理模式
// ================================================
//
// 本包中所有 defer 块遵循统一的错误处理模式：
//   - 总是检查 Close() 返回的错误
//   - 在 slog.Warn 中记录任何错误
//   - 不会由于关闭错误而返回不同的错误
//
// 示例：
//
//	defer func() {
//	    if closeErr := rows.Close(); closeErr != nil {
//	        slog.Warn("Failed to close rows", "error", closeErr)
//	    }
//	}()
//
// 这种方法确保：
//   1. 资源总是被清理，即使出现错误
//   2. 关闭错误被记录以便调试
//   3. 主要的操作错误不会被掩盖

// ================================================
// 错误类型定义
// ================================================

// ErrUnknownSortField represents an error when an unknown sort field is encountered
type ErrUnknownSortField struct {
	field string
}

// newErrUnknownSortField constructs an ErrUnknownSortField.
func newErrUnknownSortField(field string) *ErrUnknownSortField {
	return &ErrUnknownSortField{field: field}
}

// Error implements error.
func (e *ErrUnknownSortField) Error() string {
	return fmt.Sprintf("unknown sort field: %s", e.field)
}

// Is reports whether target is an *ErrUnknownSortField for the same field.
// An *ErrUnknownSortField with an empty field matches any ErrUnknownSortField,
// enabling both type-level and value-level errors.Is checks.
func (e *ErrUnknownSortField) Is(target error) bool {
	other, ok := target.(*ErrUnknownSortField)
	if !ok {
		return false
	}

	return other.field == "" || e.field == other.field
}

// ErrAmbiguousSortField represents an error when a sort field matches multiple selected columns
type ErrAmbiguousSortField struct {
	field string
}

// newErrAmbiguousSortField constructs an ErrAmbiguousSortField.
func newErrAmbiguousSortField(field string) *ErrAmbiguousSortField {
	return &ErrAmbiguousSortField{field: field}
}

// Error implements error.
func (e *ErrAmbiguousSortField) Error() string {
	return fmt.Sprintf("ambiguous sort field: %s", e.field)
}

// Is reports whether target is an *ErrAmbiguousSortField for the same field.
// An *ErrAmbiguousSortField with an empty field matches any ErrAmbiguousSortField,
// enabling both type-level and value-level errors.Is checks.
func (e *ErrAmbiguousSortField) Is(target error) bool {
	other, ok := target.(*ErrAmbiguousSortField)
	if !ok {
		return false
	}

	return other.field == "" || e.field == other.field
}

// ErrOrderCountMismatch represents an error when order by and order count mismatch
type ErrOrderCountMismatch struct {
	orderBys int
	orders   int
}

// newErrOrderCountMismatch constructs an ErrOrderCountMismatch.
func newErrOrderCountMismatch(orderbys, orders int) *ErrOrderCountMismatch {
	return &ErrOrderCountMismatch{orderBys: orderbys, orders: orders}
}

// Error implements error.
func (e *ErrOrderCountMismatch) Error() string {
	return fmt.Sprintf(
		"ORDER BY fields count(%d) and ORDER directions count(%d) mismatch",
		e.orderBys, e.orders,
	)
}

// Is reports whether target is an *ErrOrderCountMismatch with the same counts.
// An *ErrOrderCountMismatch with zero orderBys and zero orders matches any
// ErrOrderCountMismatch, enabling type-level errors.Is checks.
func (e *ErrOrderCountMismatch) Is(target error) bool {
	other, ok := target.(*ErrOrderCountMismatch)
	if !ok {
		return false
	}

	return (other.orderBys == 0 && other.orders == 0) ||
		(e.orderBys == other.orderBys && e.orders == other.orders)
}

// ================================================
// 查询结构体定义
// ================================================

// Query 代表一个已编译的 SQL 查询，它包含了多种查询变体（计数、列表、搜索）。
// 架构意图：Query 是 Build() 调用的最终产物。它是不可变的且线程安全的。
// 它解耦了“查询定义”（SQL 构建逻辑）和“查询执行”（数据库交互）。
//
// 核心字段说明：
// - cntSQL: 用于 COUNT(*) 查询的 SQL。
// - listSQL: 用于获取记录列表的 SQL。
// - baseArgs: 在 Build() 时确定的参数，包含普通值和标记位（Markers）。
type Query[O Owner] struct {
	// SQL 语句模板。
	cntSQL    string // COUNT 查询
	listSQL   string // 主 SELECT 查询
	kwCntSQL  string // 关键词搜索 COUNT 查询
	kwListSQL string // 关键词搜索 SELECT 查询

	// 基础参数列表。可能包含延迟绑定的标记（externalArgMarker 等）。
	cntArgs    []any
	listArgs   []any
	kwCntArgs  []any
	kwListArgs []any

	cntArgState    queryArgState
	listArgState   queryArgState
	kwCntArgState  queryArgState
	kwListArgState queryArgState

	// 元数据。
	selectCols   []BoundColumn[O] // 选中的列，用于 Scan 映射。
	selectTables map[string]Table // 查询涉及的所有表。
	kwCols       []SearchColumn   // 关键词搜索涉及的列。
	kwTables     map[string]Table
	hasSetOps    bool // 是否包含集合操作（UNION 等），影响别名处理。
}

type externalSliceArgMarker struct{}

type queryArgState struct {
	initialized         bool
	hasExternalArg      bool
	hasExternalSliceArg bool
	hasKeywordArg       bool
}

func (s queryArgState) hasDeferredArgs() bool {
	return s.hasExternalArg || s.hasExternalSliceArg || s.hasKeywordArg
}

const slicePlaceholderCacheMax = 128

var slicePlaceholderCache = buildSlicePlaceholderCache(slicePlaceholderCacheMax)

var builtInIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Identifier length limits by SQL dialect.
const (
	// MaxIdentifierLengthMySQL = 64  // Actual is 64, but we allow 63 for compatibility
	MaxIdentifierLengthMySQL = 64
	// MaxIdentifierLengthPostgreSQL is PostgreSQL's maximum identifier length.
	MaxIdentifierLengthPostgreSQL = 63
	// MaxIdentifierLengthOracleSQL is Oracle's maximum identifier length.
	MaxIdentifierLengthOracleSQL = 30
	// MaxIdentifierLengthSQLite is zero because SQLite has no practical identifier limit.
	MaxIdentifierLengthSQLite = 0 // SQLite has no practical limit, 0 means unlimited
)

// ================================================
// SQL 访问方法
// ================================================

// Build once and reuse Query values on hot paths instead of rebuilding the same shape.

// CountSQL returns the COUNT query SQL statement.
func (q *Query[O]) CountSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.cntSQL)
}

// ListSQL returns the main SELECT query SQL statement
func (q *Query[O]) ListSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.listSQL)
}

// KeywordCountSQL returns the keyword-search COUNT query SQL statement.
func (q *Query[O]) KeywordCountSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.kwCntSQL)
}

// KeywordListSQL returns the keyword-search SELECT query SQL statement.
func (q *Query[O]) KeywordListSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.kwListSQL)
}

// ================================================
// 查询构建器方法
// ================================================

// ================================================
// 基础查询执行方法
// ================================================

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
		slog.Info(methodName, "sql", sqlText, "args", CompactJSON(finalArgs))
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

// QueryInt executes the query and returns a single integer result
func (q *Query[O]) QueryInt(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int64, error) {
	return Trace1(ctx, func(ctx context.Context) (int64, error) {
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

// QueryFloat executes the query and returns a single float result
func (q *Query[O]) QueryFloat(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (float64, error) {
	return Trace1(ctx, func(ctx context.Context) (float64, error) {
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
	return Trace1(ctx, func(ctx context.Context) (string, error) {
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
	return Trace1(ctx, func(ctx context.Context) (int, error) {
		return q.count(ctx, tx, args...)
	})
}

// Count64 executes the count query and returns the number of matching records
// as int64, avoiding truncation on large result sets or 32-bit platforms.
func (q *Query[O]) Count64(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (int64, error) {
	return Trace1(ctx, func(ctx context.Context) (int64, error) {
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
		slog.Info("count", "sql", sqlText, "args", CompactJSON(finalArgs))
	}

	count, err := queryInt64(ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", "failed to execute count query", err)
	}

	return count, nil
}

// Exists checks if any records match the query conditions
func (q *Query[O]) Exists(
	ctx context.Context,
	tx SQLExecutor,
	args ...any,
) (bool, error) {
	return Trace1(ctx, func(ctx context.Context) (bool, error) {
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
		slog.Info("exist", "sql", sqlText, "args", CompactJSON(finalArgs))
	}

	count, err := queryInt64(ctx, tx, sqlText, finalArgs...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", "failed to check record existence", err)
	}

	return count > 0, nil
}

// ================================================
// 分页查询方法
// ================================================

// Page executes a paginated query with the given page parameters
func Page[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	page *PageReq,
	q *Query[O],
	args ...any,
) (*PageResp[O], error) {
	return Trace1(ctx, func(ctx context.Context) (*PageResp[O], error) {
		return pageFn(ctx, tx, page, q, args...)
	})
}

func pageFn[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	page *PageReq,
	q *Query[O],
	args ...any,
) (*PageResp[O], error) {
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

	// Add LIMIT parameters
	argsWithLimit := make([]any, 0, len(finalArgs)+2)
	argsWithLimit = append(argsWithLimit, finalArgs...)
	argsWithLimit = append(argsWithLimit, page.Size, page.Offset())

	if ctx.Value(printSQL) != nil {
		slog.Info("count", "sql", renderedCntSQL, "args", CompactJSON(countArgs))
		slog.Info("list", "sql", renderedListSQL, "args", CompactJSON(argsWithLimit))
	}

	// Execute count query
	count, err := queryInt64(ctx, tx, renderedCntSQL, countArgs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to execute count query", err)
	}

	// Execute list query
	rows, err := tx.QueryContext(ctx, renderedListSQL, argsWithLimit...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to execute paginated query", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("Failed to close rows", "error", closeErr)
		}
	}()

	// Scan results
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

	return NewPageResp(page, count, list), nil
}

// List executes q and returns all matching rows.
func List[O Owner](
	ctx context.Context,
	tx SQLExecutor,
	qb *Query[O],
	args ...any,
) ([]*O, error) {
	return Trace1(ctx, func(ctx context.Context) ([]*O, error) {
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
	return Trace1(ctx, func(ctx context.Context) (*O, error) {
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
	return Trace(ctx, func(ctx context.Context) error {
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

func (q *Query[O]) buildPageSQLs(page *PageReq) (string, string, error) {
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

	// 排序字段白名单校验
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

	// LIMIT 参数化
	listQuery += "\nLIMIT ? OFFSET ?"

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

// ================================================
// 批量操作支持
// ================================================

// ChunkedOptions 分块执行通用配置选项。
type ChunkedOptions struct {
	ChunkSize int // 每块处理的数量，默认 1000
}

// DefaultChunkedOptions 返回默认的分块执行配置。
func DefaultChunkedOptions() *ChunkedOptions {
	return &ChunkedOptions{
		ChunkSize: 1000,
	}
}

// ChunkedInsertOptions 分块插入配置选项。
type ChunkedInsertOptions struct {
	ChunkSize    int  // 每块处理的数量，默认 1000
	IgnoreErrors bool // 是否忽略重复键插入错误并继续处理后续数据
}

// DefaultChunkedInsertOptions 返回默认的分块插入配置。
func DefaultChunkedInsertOptions() *ChunkedInsertOptions {
	return &ChunkedInsertOptions{
		ChunkSize:    DefaultChunkedOptions().ChunkSize,
		IgnoreErrors: false,
	}
}

// ChunkedInsert inserts items in chunks using the provided executor.
//
// Transaction boundaries are intentionally caller-controlled. Passing a plain
// *sql.DB or non-transactional executor allows partial progress across chunks;
// passing a *sql.Tx makes the whole chunked operation participate in that
// transaction. TSQ does not open an implicit outer transaction for this helper.
func ChunkedInsert[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedInsertOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return chunkedInsertFn(ctx, tx, items, options...)
	})
}

func chunkedInsertFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	opts, err := normalizeChunkedInsertOptions(options...)
	if err != nil {
		return err
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(items))

		batch := items[i:end]
		if err := chunkedInsertChunk(ctx, sqlMutationExecutor{exec: tx}, batch, opts); err != nil {
			return fmt.Errorf("chunked insert failed at index %d"+": %w", i, err)
		}
	}

	return nil
}

func chunkedInsertChunk[T Table](
	ctx context.Context,
	tx mutationExecutor,
	items []T,
	opts *ChunkedInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	batch := make([]Table, 0, len(items))
	for itemIdx, item := range items {
		if isNilValue(item) {
			return fmt.Errorf("item at index %d is nil", itemIdx)
		}

		batch = append(batch, item)
	}

	if opts.IgnoreErrors {
		for itemIdx, item := range batch {
			if err := tx.Insert(ctx, item); err != nil {
				if isDuplicateKeyError(err) {
					slog.Debug("Ignored duplicate key error in batch insert", "error", err)
					continue
				}

				return fmt.Errorf("chunked insert failed at item %d"+": %w", itemIdx, err)
			}
		}

		return nil
	}

	if err := tx.Insert(ctx, batch...); err != nil {
		return fmt.Errorf("%s: %w", "chunked insert batch failed", err)
	}

	return nil
}

// ChunkedUpdate updates items in chunks using the provided executor.
//
// Transaction boundaries are intentionally caller-controlled. Passing a plain
// *sql.DB or non-transactional executor allows partial progress across chunks;
// passing a *sql.Tx makes the whole chunked operation participate in that
// transaction. TSQ does not open an implicit outer transaction for this helper.
func ChunkedUpdate[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return chunkedUpdateFn(ctx, tx, items, options...)
	})
}

func chunkedUpdateFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return err
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(items))

		batch := items[i:end]
		if err := chunkedUpdateChunk(ctx, sqlMutationExecutor{exec: tx}, batch); err != nil {
			return fmt.Errorf("chunked update failed at index %d"+": %w", i, err)
		}
	}

	return nil
}

func chunkedUpdateChunk[T Table](
	ctx context.Context,
	tx mutationExecutor,
	items []T,
) error {
	batch := make([]Table, 0, len(items))
	for itemIdx, item := range items {
		if isNilValue(item) {
			return fmt.Errorf("item at index %d is nil", itemIdx)
		}

		batch = append(batch, item)
	}

	if len(batch) == 0 {
		return nil
	}

	if _, err := tx.Update(ctx, batch...); err != nil {
		return fmt.Errorf("%s: %w", "chunked update batch failed", err)
	}

	return nil
}

// ChunkedDelete deletes items in chunks using the provided executor.
//
// Transaction boundaries are intentionally caller-controlled. Passing a plain
// *sql.DB or non-transactional executor allows partial progress across chunks;
// passing a *sql.Tx makes the whole chunked operation participate in that
// transaction. TSQ does not open an implicit outer transaction for this helper.
func ChunkedDelete[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return chunkedDeleteFn(ctx, tx, items, options...)
	})
}

func chunkedDeleteFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return err
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(items))

		batch := items[i:end]
		if err := chunkedDeleteChunk(ctx, sqlMutationExecutor{exec: tx}, batch); err != nil {
			return fmt.Errorf("chunked delete failed at index %d"+": %w", i, err)
		}
	}

	return nil
}

func chunkedDeleteChunk[T Table](
	ctx context.Context,
	tx mutationExecutor,
	items []T,
) error {
	batch := make([]Table, 0, len(items))
	for itemIdx, item := range items {
		if isNilValue(item) {
			return fmt.Errorf("item at index %d is nil", itemIdx)
		}

		batch = append(batch, item)
	}

	if len(batch) == 0 {
		return nil
	}

	if _, err := tx.Delete(ctx, batch...); err != nil {
		return fmt.Errorf("%s: %w", "chunked delete batch failed", err)
	}

	return nil
}

// ChunkedDeleteByIDs deletes rows by primary-key values in chunks.
//
// Transaction boundaries are intentionally caller-controlled. Passing a plain
// *sql.DB or non-transactional executor allows partial progress across chunks;
// passing a *sql.Tx makes the whole chunked operation participate in that
// transaction. TSQ does not open an implicit outer transaction for this helper.
func ChunkedDeleteByIDs(
	ctx context.Context,
	tx SQLExecutor,
	tableName string,
	idColumn string,
	ids []any,
	options ...*ChunkedOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return chunkedDeleteByIDsFn(ctx, tx, tableName, idColumn, ids, options...)
	})
}

func chunkedDeleteByIDsFn(
	ctx context.Context,
	tx SQLExecutor,
	tableName string,
	idColumn string,
	ids []any,
	options ...*ChunkedOptions,
) error {
	if len(ids) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	if err := validateIDValues(ids); err != nil {
		return err
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return err
	}

	// 批量处理
	for i := 0; i < len(ids); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(ids))

		batch := ids[i:end]
		if err := chunkedDeleteByIDsChunk(ctx, tx, tableName, idColumn, batch); err != nil {
			return fmt.Errorf("chunked delete by IDs failed at index %d"+": %w", i, err)
		}
	}

	return nil
}

func chunkedDeleteByIDsChunk(
	ctx context.Context,
	tx SQLExecutor,
	tableName string,
	idColumn string,
	ids []any,
) error {
	if len(ids) == 0 {
		return nil
	}

	// 构建 IN 查询
	placeholders := make([]string, len(ids))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	sqlStr, err := buildDeleteByIDsSQL(tableName, idColumn, len(placeholders))
	if err != nil {
		return err
	}

	sqlText := renderSQLForExecutor(tx, sqlStr)

	if err := validateOperationalExecutorForSQL(tx, sqlStr); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, sqlText, ids...)
	if err != nil {
		return fmt.Errorf("chunked delete by IDs failed: %s"+": %w", sqlText, err)
	}

	return nil
}

func buildDeleteByIDsSQL(tableName, idColumn string, placeholderCount int) (string, error) {
	if placeholderCount <= 0 {
		return "", errors.New("placeholder count must be greater than 0")
	}

	quotedTable, err := quoteBuiltInIdentifier(tableName)
	if err != nil {
		return "", err
	}

	quotedColumn, err := quoteBuiltInIdentifier(idColumn)
	if err != nil {
		return "", err
	}

	placeholders := make([]string, placeholderCount)
	for i := range placeholders {
		placeholders[i] = "?"
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		quotedTable,
		quotedColumn,
		strings.Join(placeholders, ","),
	), nil
}

func quoteBuiltInIdentifier(name string) (string, error) {
	if !builtInIdentifierPattern.MatchString(name) {
		return "", fmt.Errorf("invalid SQL identifier: %s", name)
	}

	// Warn if identifier is very long (>50 chars) as it may exceed limits in some databases
	if len(name) > 50 {
		slog.Warn("identifier is unusually long", "identifier", name, "length", len(name))
	}

	return rawIdentifier(name), nil
}

// ================================================
// 批量操作辅助函数
// ================================================

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1062
	}

	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique ||
			sqliteErr.ExtendedCode == sqlite3.ErrConstraintPrimaryKey
	}

	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code) == "23505"
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}

	return false
}

func normalizePageReq(page *PageReq) *PageReq {
	if page == nil {
		page = &PageReq{}
	}

	normalized := *page
	if normalized.Page <= 0 {
		normalized.Page = 1
	}

	if normalized.Size <= 0 {
		normalized.Size = DefaultPageSize
	}

	if normalized.Size > MaxPageSize {
		normalized.Size = MaxPageSize
	}

	return &normalized
}

func normalizeChunkedInsertOptions(options ...*ChunkedInsertOptions) (*ChunkedInsertOptions, error) {
	if len(options) > 1 {
		return nil, errors.New("expected at most one chunked insert options value")
	}

	opts := DefaultChunkedInsertOptions()

	if len(options) > 0 && options[0] != nil {
		copied := *options[0]
		opts = &copied
	}

	if err := validateChunkSize(opts.ChunkSize); err != nil {
		return nil, err
	}

	return opts, nil
}

func normalizeChunkedOptions(options ...*ChunkedOptions) (*ChunkedOptions, error) {
	if len(options) > 1 {
		return nil, errors.New("expected at most one chunked options value")
	}

	opts := DefaultChunkedOptions()

	if len(options) > 0 && options[0] != nil {
		copied := *options[0]
		opts = &copied
	}

	if err := validateChunkSize(opts.ChunkSize); err != nil {
		return nil, err
	}

	return opts, nil
}

func validateChunkSize(chunkSize int) error {
	if chunkSize <= 0 {
		return fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	return nil
}

func validateIDValues(ids []any) error {
	for i, id := range ids {
		if isNilValue(id) {
			return fmt.Errorf("id at index %d cannot be nil", i)
		}
	}

	return nil
}

// EscapeKeywordSearch escapes special characters in keyword search strings for use with LIKE clauses.
// This prevents SQL injection via LIKE wildcard characters (% and _).
//
// Example:
//
//	keyword := "100% cotton"
//	escaped := EscapeKeywordSearch(keyword)  // "100\% cotton"
//
// Note: When using this function, your SQL dialect may require you to specify the escape character
// in the LIKE clause. For example:
//
//	SELECT * FROM table WHERE column LIKE ? ESCAPE '\'
//
// Currently, TSQ keyword search does not apply escaping automatically. Users MUST call this function
// if their keywords contain % or _ characters to prevent unintended pattern matching or SQL injection.
func EscapeKeywordSearch(keyword string) string {
	// Escape backslash first to avoid double-escaping
	s := strings.ReplaceAll(keyword, "\\", "\\\\")
	// Escape LIKE wildcards
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")

	return s
}

func mergeQueryArgs(base, extra []any) ([]any, error) {
	_, args, err := resolveQueryWithState("", base, extra, "", scanQueryArgState(base))
	return args, err
}

func resolveQueryArgs(base, extra []any, keyword string) ([]any, error) {
	_, args, err := resolveQueryWithState("", base, extra, keyword, scanQueryArgState(base))
	return args, err
}

// resolveQuery 是 TSQ 参数绑定的核心算法。
// 架构意图：它负责将“编译期”确定的基础参数（base）与“执行期”提供的外部参数（extra）进行对齐。
//
// 工作原理：
//  1. 它是基础 SQL 模板的“二遍扫描”过程。
//  2. 遍历 baseArgs，这些参数是在 Build() 阶段收集的。
//  3. 遇到普通的绑定值，直接保留。
//  4. 遇到 externalArgMarker：从 extraArgs 中取出一个值替换，并在 SQL 中保留一个 "?"。
//  5. 遇到 externalSliceArgMarker：这是最复杂的部分。它从 extraArgs 中取出一个切片，
//     计算其长度 N，然后在 SQL 中将原来的一个 "?" 展开为 N 个 "?"（如 "?, ?, ?"），
//     并将切片中的所有元素平铺到结果参数列表中。
//  6. 遇到 keywordArgMarker：使用传入的 keyword 构造 LIKE 参数。
func resolveQuery(baseSQL string, base, extra []any, keyword string) (string, []any, error) {
	return resolveQueryWithState(baseSQL, base, extra, keyword, scanQueryArgState(base))
}

func resolveQueryWithState(
	baseSQL string,
	base,
	extra []any,
	keyword string,
	state queryArgState,
) (string, []any, error) {
	if !state.initialized {
		state = scanQueryArgState(base)
	}

	if !state.hasDeferredArgs() {
		if len(extra) == 0 {
			return baseSQL, base, nil
		}

		result := make([]any, 0, len(base)+len(extra))
		result = append(result, base...)
		result = append(result, extra...)

		return baseSQL, result, nil
	}

	if !state.hasExternalSliceArg {
		args, err := resolveQueryArgsOnly(base, extra, keyword)
		return baseSQL, args, err
	}

	result := make([]any, 0, len(base)+len(extra))
	extraIndex := 0
	like := ""
	cursor := 0

	var sqlBuilder strings.Builder
	hasSQL := baseSQL != ""

	for _, arg := range base {
		// 如果提供了 baseSQL，我们需要同步处理 SQL 中的问号占位符。
		if hasSQL {
			next := strings.Index(baseSQL[cursor:], "?")
			if next < 0 {
				sqlBuilder.WriteString(baseSQL[cursor:])
				hasSQL = false
			} else {
				sqlBuilder.WriteString(baseSQL[cursor : cursor+next])
				cursor += next + 1
			}
		}

		switch arg {
		case externalArgMarker:
			// 延迟绑定单个变量。
			if extraIndex >= len(extra) {
				return "", nil, errors.New("missing external query argument")
			}

			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, extra[extraIndex])
			extraIndex++
		case externalSliceArgMarker{}:
			// 延迟绑定切片变量（处理 IN 语句）。
			if extraIndex >= len(extra) {
				return "", nil, errors.New("missing external query argument")
			}

			values, err := flattenExternalSliceArg(extra[extraIndex])
			if err != nil {
				return "", nil, err
			}

			if hasSQL {
				// 关键点：动态展开占位符。
				sqlBuilder.WriteString(expandSlicePlaceholders(len(values)))
			}

			result = append(result, values...)
			extraIndex++

		case keywordArgMarker:
			// 处理 Search() 产生的关键词搜索标记。
			if keyword == "" {
				return "", nil, errors.New("missing keyword query argument")
			}

			if like == "" {
				like = "%" + keyword + "%"
			}

			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, like)
		default:
			// 正常的绑定变量（Build 阶段已确定）。
			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, arg)
		}
	}

	// 将剩余的 extra 参数（如果有的话，如 LIMIT/OFFSET）追加到末尾。
	result = append(result, extra[extraIndex:]...)

	if hasSQL {
		sqlBuilder.WriteString(baseSQL[cursor:])
		return sqlBuilder.String(), result, nil
	}

	return baseSQL, result, nil
}

func resolveQueryArgsOnly(base, extra []any, keyword string) ([]any, error) {
	result := make([]any, 0, len(base)+len(extra))
	extraIndex := 0
	like := ""

	for _, arg := range base {
		switch arg {
		case externalArgMarker:
			if extraIndex >= len(extra) {
				return nil, errors.New("missing external query argument")
			}

			result = append(result, extra[extraIndex])
			extraIndex++
		case keywordArgMarker:
			if keyword == "" {
				return nil, errors.New("missing keyword query argument")
			}

			if like == "" {
				like = "%" + keyword + "%"
			}

			result = append(result, like)
		default:
			result = append(result, arg)
		}
	}

	result = append(result, extra[extraIndex:]...)

	return result, nil
}

func flattenExternalSliceArg(arg any) ([]any, error) {
	if isNilValue(arg) {
		return nil, nil
	}

	switch v := arg.(type) {
	case []any:
		return validateAnySlice(v)
	case []int:
		return boxSlice(v), nil
	case []int64:
		return boxSlice(v), nil
	case []string:
		return boxSlice(v), nil
	case []bool:
		return boxSlice(v), nil
	case []float64:
		return boxSlice(v), nil
	case []float32:
		return boxSlice(v), nil
	case *[]any:
		if v == nil {
			return nil, nil
		}

		return validateAnySlice(*v)
	case *[]int:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]int64:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]string:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]bool:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]float64:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]float32:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	}

	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, nil
		}

		v = v.Elem()
	}

	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("external IN query argument must be a slice or array, got %T", arg)
	}

	values := make([]any, 0, v.Len())
	for i := range v.Len() {
		value := v.Index(i).Interface()
		if err := validatePredicateValue(value); err != nil {
			return nil, err
		}

		values = append(values, value)
	}

	return values, nil
}

func expandSlicePlaceholders(size int) string {
	if size <= slicePlaceholderCacheMax {
		return slicePlaceholderCache[size]
	}

	var builder strings.Builder
	builder.Grow(size*3 - 2)

	for i := range size {
		if i > 0 {
			builder.WriteString(", ")
		}

		builder.WriteByte('?')
	}

	return builder.String()
}

func scanQueryArgState(args []any) queryArgState {
	state := queryArgState{initialized: true}

	for _, arg := range args {
		switch arg {
		case externalArgMarker:
			state.hasExternalArg = true
		case externalSliceArgMarker{}:
			state.hasExternalSliceArg = true
		case keywordArgMarker:
			state.hasKeywordArg = true
		}
	}

	return state
}

func buildSlicePlaceholderCache(max int) []string {
	cache := make([]string, max+1)
	cache[0] = "NULL"
	cache[1] = "?"

	for size := 2; size <= max; size++ {
		var builder strings.Builder
		builder.Grow(size*3 - 2)

		for i := 0; i < size; i++ {
			if i > 0 {
				builder.WriteString(", ")
			}

			builder.WriteByte('?')
		}

		cache[size] = builder.String()
	}

	return cache
}

func validateAnySlice(values []any) ([]any, error) {
	if len(values) == 0 {
		return nil, nil
	}

	result := make([]any, 0, len(values))
	for _, value := range values {
		if err := validatePredicateValue(value); err != nil {
			return nil, err
		}

		result = append(result, value)
	}

	return result, nil
}

func boxSlice[T any](values []T) []any {
	if len(values) == 0 {
		return nil
	}

	result := make([]any, len(values))
	for i, value := range values {
		result[i] = value
	}

	return result
}

func (q *Query[O]) subquerySQL() string {
	if q == nil {
		return ""
	}

	return q.listSQL
}

func (q *Query[O]) subqueryArgs() []any {
	if q == nil {
		return nil
	}

	return slices.Clone(q.listArgs)
}

func (q *Query[O]) subquerySelectCount() int {
	if q == nil {
		return 0
	}

	return len(q.selectCols)
}

func validateQuery[O Owner](q *Query[O]) error {
	if q == nil {
		return errors.New("query cannot be nil")
	}

	if strings.TrimSpace(q.listSQL) == "" || strings.TrimSpace(q.cntSQL) == "" {
		return errors.New("query is not built")
	}

	if len(q.kwCols) > 0 &&
		(strings.TrimSpace(q.kwListSQL) == "" || strings.TrimSpace(q.kwCntSQL) == "") {
		return errors.New("keyword query is not built")
	}

	return nil
}

func validateExecutor(tx SQLExecutor) error {
	if tx == nil {
		return errSQLExecutorNil
	}

	value := reflect.ValueOf(tx)
	if value.IsValid() && value.Kind() == reflect.Pointer && value.IsNil() {
		return errSQLExecutorNil
	}

	return nil
}

func validateOperationalExecutor(tx SQLExecutor) error {
	if err := validateExecutor(tx); err != nil {
		return err
	}

	if engine, ok := tx.(*Engine); ok && engine.DB == nil {
		return errEngineDatabaseNil
	}

	return nil
}

func validateExecutorForSQL(tx SQLExecutor, rawSQLs ...string) error {
	if err := validateExecutor(tx); err != nil {
		return err
	}

	dialect := dialectForExecutor(tx)
	if dialect != nil {
		for _, rawSQL := range rawSQLs {
			for _, capability := range detectSQLCapabilities(rawSQL) {
				if err := validateDialectCapability(dialect, capability); err != nil {
					return err
				}
			}
		}

		return nil
	}

	for _, rawSQL := range rawSQLs {
		if containsIdentifierMarkersNeedingRender(rawSQL) || containsBindVarsNeedingDialect(rawSQL) {
			return errors.New("sql executor dialect cannot be determined")
		}
	}

	return nil
}

func detectSQLCapabilities(rawSQL string) []DialectCapability {
	upperSQL := strings.ToUpper(strings.TrimSpace(rawSQL))
	capabilities := make([]DialectCapability, 0, 4)

	if strings.HasPrefix(upperSQL, "WITH ") {
		capabilities = append(capabilities, DialectCapabilityCTE)
	}

	if strings.Contains(upperSQL, " FULL JOIN ") {
		capabilities = append(capabilities, DialectCapabilityFullOuterJoin)
	}

	if strings.Contains(upperSQL, " INTERSECT ") {
		capabilities = append(capabilities, DialectCapabilityIntersect)
	}

	if strings.Contains(upperSQL, " EXCEPT ") || strings.Contains(upperSQL, " MINUS ") {
		capabilities = append(capabilities, DialectCapabilityExcept)
	}

	return capabilities
}

func validateOperationalExecutorForSQL(tx SQLExecutor, rawSQLs ...string) error {
	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	return validateExecutorForSQL(tx, rawSQLs...)
}

func validateMutationItem(item Table) error {
	if isNilValue(item) {
		return errors.New("mutation item cannot be nil")
	}

	return nil
}

func validateScanHolder(holder any) error {
	if isNilValue(holder) {
		return errors.New("scan holder cannot be nil")
	}

	if reflect.ValueOf(holder).Kind() != reflect.Pointer {
		return errors.New("scan holder must be a pointer")
	}

	return nil
}

func buildScanDest[O Owner](cols []BoundColumn[O], holder *O) ([]any, error) {
	if isNilValue(holder) {
		return nil, errors.New("scan holder cannot be nil")
	}

	erased := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		erased = append(erased, col)
	}

	return buildScanDestWith(erased, func(pointerFunc scanPointer) (any, error) {
		return invokeFieldPointer(pointerFunc, holder)
	})
}

func buildScanDestWith(cols []SQLColumn, invoke func(scanPointer) (any, error)) ([]any, error) {
	dest := make([]any, len(cols))

	for i, col := range cols {
		pointerFunc := col.scanPointer()
		if pointerFunc == nil {
			return nil, fmt.Errorf("select column %s cannot be scanned: field pointer is nil", col.SQLExpr())
		}

		ptr, err := invoke(pointerFunc)
		if err != nil {
			return nil, fmt.Errorf("select column %s cannot be scanned"+": %w", col.SQLExpr(), err)
		}

		if ptr == nil {
			return nil, fmt.Errorf("select column %s cannot be scanned: field pointer returned nil", col.SQLExpr())
		}

		value := reflect.ValueOf(ptr)
		if value.IsValid() && value.Kind() == reflect.Pointer && value.IsNil() {
			return nil, fmt.Errorf("select column %s cannot be scanned: field pointer returned nil", col.SQLExpr())
		}

		dest[i] = ptr
	}

	return dest, nil
}

func validateScanDestForType[O Owner](cols []BoundColumn[O], sqlText string, args []any) error {
	holder := new(O)
	if _, err := buildScanDest(cols, holder); err != nil {
		return fmt.Errorf("build scan dest\n%s\n%v"+": %w",
			sqlText, CompactJSON(args), err)
	}

	return nil
}

func invokeFieldPointer[O Owner](pointerFunc scanPointer, holder *O) (ptr any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("field pointer panicked: %v", recovered)
		}
	}()

	return pointerFunc(holder), nil
}

// Insert inserts item using the table metadata on T.
func Insert[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return insertFn(ctx, tx, item)
	})
}

func insertFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	if err := validateMutationItem(item); err != nil {
		return err
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	return insertTables(ctx, tx, item)
}

// Update updates item using the table metadata on T.
func Update[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return updateFn(ctx, tx, item)
	})
}

func updateFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	if err := validateMutationItem(item); err != nil {
		return err
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	_, err := updateTables(ctx, tx, item)

	return err
}

// Delete deletes item using the table metadata on T.
func Delete[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return deleteFn(ctx, tx, item)
	})
}

func deleteFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	if err := validateMutationItem(item); err != nil {
		return err
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	_, err := deleteTables(ctx, tx, item)

	return err
}
