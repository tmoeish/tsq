package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/juju/errors"
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

func NewErrUnknownSortField(field string) *ErrUnknownSortField {
	return &ErrUnknownSortField{field: field}
}

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

func NewErrAmbiguousSortField(field string) *ErrAmbiguousSortField {
	return &ErrAmbiguousSortField{field: field}
}

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

func NewErrOrderCountMismatch(orderbys, orders int) *ErrOrderCountMismatch {
	return &ErrOrderCountMismatch{orderBys: orderbys, orders: orders}
}

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

// ErrIncompatibleTableRebind indicates an attempt to join columns from the same table
type ErrIncompatibleTableRebind struct {
	table1 string
	table2 string
}

func NewErrIncompatibleTableRebind(table1, table2 string) *ErrIncompatibleTableRebind {
	return &ErrIncompatibleTableRebind{
		table1: table1,
		table2: table2,
	}
}

func (e *ErrIncompatibleTableRebind) Error() string {
	if e.table1 == e.table2 {
		return "join columns must belong to different tables; both reference table " + e.table1
	}
	return "join column tables must not be identical; got " + e.table1 + " and " + e.table2
}

// ================================================
// 查询结构体定义
// ================================================

// Query represents a compiled SQL query with all its variations
type Query struct {
	// SQL statements
	cntSQL    string // COUNT query
	listSQL   string // Main SELECT query
	kwCntSQL  string // Keyword search COUNT query
	kwListSQL string // Keyword search SELECT query

	cntArgs    []any
	listArgs   []any
	kwCntArgs  []any
	kwListArgs []any

	// Metadata
	selectCols   []Column
	selectTables map[string]Table
	kwCols       []Column
	kwTables     map[string]Table
}

var builtInIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Identifier length limits by SQL dialect
const (
	// MaxIdentifierLengthMySQL = 64  // Actual is 64, but we allow 63 for compatibility
	MaxIdentifierLengthMySQL       = 64
	MaxIdentifierLengthPostgreSQL  = 63
	MaxIdentifierLengthOracleSQL   = 30
	MaxIdentifierLengthSQLite      = 0 // SQLite has no practical limit, 0 means unlimited
)

// ================================================
// SQL 访问方法
// ================================================

// 查询计划缓存考虑
//
// TSQ 本身不提供内置的查询计划缓存，但支持多种缓存策略：
//
// 1. 应用层缓存（推荐）
//    对于频繁重复的查询，在应用层缓存生成的 SQL：
//
//    type CachedQuery struct {
//        key  string
//        sql  string
//        args []any
//    }
//
//    // 或使用 sync.Map 进行无锁缓存
//    var queryCache sync.Map
//    q := queryCache.LoadOrStore(key, buildQuery()).(Query)
//
// 2. 数据库驱动缓存
//    许多驱动程序（如 github.com/jmoiron/sqlx）支持准备语句缓存。
//    只需确保对相同的 SQL 使用相同的参数类型。
//
// 3. 连接池准备缓存
//    使用支持准备语句缓存的连接池（如 pgx 连接池）。
//
// 性能建议：
// - 对于一次性查询，不需要缓存
// - 对于循环中的查询，在循环外构建一次
// - 对于参数不同的动态查询，使用数据库参数化
// - 避免在热路径中调用 Build() 多次

// CntSQL returns the COUNT query SQL statement
func (q *Query) CntSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.cntSQL)
}

// ListSQL returns the main SELECT query SQL statement
func (q *Query) ListSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.listSQL)
}

// KwCntSQL returns the keyword search COUNT query SQL statement
func (q *Query) KwCntSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.kwCntSQL)
}

// KwListSQL returns the keyword search SELECT query SQL statement
func (q *Query) KwListSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.kwListSQL)
}

// ================================================
// 查询构建器方法
// ================================================

// Build builds and validates the query
func (qb *QueryBuilder) Build() (*Query, error) {
	if qb == nil {
		return nil, errors.New("query builder cannot be nil")
	}

	if qb.buildErr != nil {
		return nil, errors.Trace(qb.buildErr)
	}

	plan, err := buildQueryPlan(qb.spec, qb.allowCartesianProduct)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Query{
		cntSQL:     plan.cntSQL,
		listSQL:    plan.listSQL,
		kwCntSQL:   plan.kwCntSQL,
		kwListSQL:  plan.kwListSQL,
		cntArgs:    plan.cntArgs,
		listArgs:   plan.listArgs,
		kwCntArgs:  plan.kwCntArgs,
		kwListArgs: plan.kwListArgs,

		selectCols:   slices.Clone(qb.spec.Selects),
		selectTables: qb.spec.selectTables(),
		kwCols:       slices.Clone(qb.spec.KeywordSearch),
		kwTables:     qb.spec.keywordTables(),
	}, nil
}

// ================================================
// 基础查询执行方法
// ================================================

// prepareQueryExecution handles common steps for all scalar query methods:
// validation, SQL rendering, debug printing, and argument merging.
// Returns (sqlText, finalArgs, error).
func (q *Query) prepareQueryExecution(
	ctx context.Context,
	tx SqlExecutor,
	methodName string,
	args ...any,
) (string, []any, error) {
	if err := validateQuery(q); err != nil {
		return "", nil, errors.Trace(err)
	}

	if err := validateOperationalExecutorForSQL(tx, q.listSQL); err != nil {
		return "", nil, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info(methodName, "sql", sqlText, "args", CompactJSON(args))
	}

	finalArgs, err := mergeQueryArgs(q.listArgs, args)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	return sqlText, finalArgs, nil
}

// QueryInt executes the query and returns a single integer result
func (q *Query) QueryInt(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (int64, error) {
	return Trace1(ctx, func(ctx context.Context) (int64, error) {
		return q.queryInt(ctx, tx, args...)
	})
}

func (q *Query) queryInt(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (int64, error) {
	sqlText, finalArgs, err := q.prepareQueryExecution(ctx, tx, "queryInt", args...)
	if err != nil {
		return 0, err
	}

	result, err := tx.WithContext(ctx).SelectInt(sqlText, finalArgs...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(finalArgs))
	}

	return result, nil
}

// QueryFloat executes the query and returns a single float result
func (q *Query) QueryFloat(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (float64, error) {
	return Trace1(ctx, func(ctx context.Context) (float64, error) {
		return q.queryFloat(ctx, tx, args...)
	})
}

func (q *Query) queryFloat(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (float64, error) {
	sqlText, finalArgs, err := q.prepareQueryExecution(ctx, tx, "queryFloat", args...)
	if err != nil {
		return 0, err
	}

	result, err := tx.WithContext(ctx).SelectFloat(sqlText, finalArgs...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(finalArgs))
	}

	return result, nil
}

// QueryStr executes the query and returns a single string result
func (q *Query) QueryStr(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (string, error) {
	return Trace1(ctx, func(ctx context.Context) (string, error) {
		return q.queryStr(ctx, tx, args...)
	})
}

func (q *Query) queryStr(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (string, error) {
	sqlText, finalArgs, err := q.prepareQueryExecution(ctx, tx, "queryStr", args...)
	if err != nil {
		return "", err
	}

	result, err := tx.WithContext(ctx).SelectStr(sqlText, finalArgs...)
	if err != nil {
		return "", errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(finalArgs))
	}

	return result, nil
}

// Count executes the count query and returns the number of matching records.
// The result is truncated to int; use Count64 when an int64 is required.
func (q *Query) Count(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (int, error) {
	return Trace1(ctx, func(ctx context.Context) (int, error) {
		return q.count(ctx, tx, args...)
	})
}

// Count64 executes the count query and returns the number of matching records
// as int64, avoiding truncation on large result sets or 32-bit platforms.
func (q *Query) Count64(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (int64, error) {
	return Trace1(ctx, func(ctx context.Context) (int64, error) {
		return q.count64(ctx, tx, args...)
	})
}

func (q *Query) count(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (int, error) {
	n, err := q.count64(ctx, tx, args...)
	return int(n), err
}

func (q *Query) count64(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (int64, error) {
	if err := validateQuery(q); err != nil {
		return 0, errors.Trace(err)
	}

	if err := validateOperationalExecutorForSQL(tx, q.cntSQL); err != nil {
		return 0, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.cntSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("count", "sql", sqlText, "args", CompactJSON(args))
	}

	finalArgs, err := mergeQueryArgs(q.cntArgs, args)
	if err != nil {
		return 0, errors.Trace(err)
	}

	count, err := tx.WithContext(ctx).SelectInt(sqlText, finalArgs...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(finalArgs))
	}

	return count, nil
}

// Exists checks if any records match the query conditions
func (q *Query) Exists(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (bool, error) {
	return Trace1(ctx, func(ctx context.Context) (bool, error) {
		return q.exist(ctx, tx, args...)
	})
}

func (q *Query) exist(
	ctx context.Context,
	tx SqlExecutor,
	args ...any,
) (bool, error) {
	if err := validateQuery(q); err != nil {
		return false, errors.Trace(err)
	}

	if err := validateOperationalExecutorForSQL(tx, q.cntSQL); err != nil {
		return false, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.cntSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("exist", "sql", sqlText, "args", CompactJSON(args))
	}

	finalArgs, err := mergeQueryArgs(q.cntArgs, args)
	if err != nil {
		return false, errors.Trace(err)
	}

	count, err := tx.WithContext(ctx).SelectInt(sqlText, finalArgs...)
	if err != nil {
		return false, errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(finalArgs))
	}

	return count > 0, nil
}

// ================================================
// 分页查询方法
// ================================================

// Page executes a paginated query with the given page parameters
func Page[T any](
	ctx context.Context,
	tx SqlExecutor,
	page *PageReq,
	q *Query,
	args ...any,
) (*PageResp[T], error) {
	return Trace1(ctx, func(ctx context.Context) (*PageResp[T], error) {
		return pageFn[T](ctx, tx, page, q, args...)
	})
}

func pageFn[T any](
	ctx context.Context,
	tx SqlExecutor,
	page *PageReq,
	q *Query,
	args ...any,
) (*PageResp[T], error) {
	if err := validateQuery(q); err != nil {
		return nil, errors.Trace(err)
	}

	page = normalizePageReq(page)

	cntSQL, listSQL, err := q.buildPageSQLs(page)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := validateOperationalExecutorForSQL(tx, cntSQL, listSQL); err != nil {
		return nil, errors.Trace(err)
	}

	renderedCntSQL := renderSQLForExecutor(tx, cntSQL)
	renderedListSQL := renderSQLForExecutor(tx, listSQL)

	if err := validateScanDestForType[T](q.selectCols, renderedListSQL, args); err != nil {
		return nil, errors.Trace(err)
	}

	queryBaseArgs := q.listArgs
	countBaseArgs := q.cntArgs

	if len(q.kwCols) > 0 && len(page.Keyword) > 0 {
		queryBaseArgs = q.kwListArgs
		countBaseArgs = q.kwCntArgs
	}

	// Add keyword search parameters if needed
	finalArgs, err := resolveQueryArgs(queryBaseArgs, args, page.Keyword)
	if err != nil {
		return nil, errors.Trace(err)
	}

	countArgs, err := resolveQueryArgs(countBaseArgs, args, page.Keyword)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Add LIMIT parameters
	argsWithLimit := append(finalArgs, page.Size, page.Offset())

	if ctx.Value(printSQL) != nil {
		slog.Info("count", "sql", renderedCntSQL, "args", CompactJSON(finalArgs))
		slog.Info("list", "sql", renderedListSQL, "args", CompactJSON(argsWithLimit))
	}

	// Execute count query
	count, err := tx.WithContext(ctx).SelectInt(renderedCntSQL, countArgs...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", renderedCntSQL, CompactJSON(countArgs))
	}

	// Execute list query
	rows, err := tx.WithContext(ctx).Query(renderedListSQL, argsWithLimit...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", renderedListSQL, CompactJSON(argsWithLimit))
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("Failed to close rows", "error", closeErr)
		}
	}()

	// Scan results
	list := make([]*T, 0, page.Size) // Pre-allocate with expected size

	for rows.Next() {
		r := new(T)

		dest, err := buildScanDest(q.selectCols, r)
		if err != nil {
			return nil, errors.Annotatef(err,
				"build scan dest\n%s\n%v",
				renderedListSQL, CompactJSON(argsWithLimit),
			)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Annotatef(err,
				"rows.Scan\n%s\n%v",
				renderedListSQL, CompactJSON(argsWithLimit),
			)
		}

		list = append(list, r)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", renderedListSQL, CompactJSON(argsWithLimit))
	}

	return NewResponse(page, count, list), nil
}

func List[T any](
	ctx context.Context,
	tx SqlExecutor,
	qb *Query,
	args ...any,
) ([]*T, error) {
	return Trace1(ctx, func(ctx context.Context) ([]*T, error) {
		return listFn[T](ctx, tx, qb, args...)
	})
}

func listFn[T any](
	ctx context.Context,
	tx SqlExecutor,
	q *Query,
	args ...any,
) ([]*T, error) {
	if err := validateQuery(q); err != nil {
		return nil, errors.Trace(err)
	}

	if err := validateOperationalExecutorForSQL(tx, q.listSQL); err != nil {
		return nil, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.listSQL)

	finalArgs, err := mergeQueryArgs(q.listArgs, args)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := validateScanDestForType[T](q.selectCols, sqlText, finalArgs); err != nil {
		return nil, errors.Trace(err)
	}

	if ctx.Value(printSQL) != nil {
		slog.Info("list", "sql", sqlText, "args", CompactJSON(args))
	}

	rows, err := tx.WithContext(ctx).Query(sqlText, finalArgs...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", sqlText, finalArgs)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("Failed to close rows", "error", closeErr)
		}
	}()

	var list []*T

	for rows.Next() {
		r := new(T)

		dest, err := buildScanDest(q.selectCols, r)
		if err != nil {
			return nil, errors.Annotatef(err,
				"build scan dest\n%s\n%v",
				sqlText, CompactJSON(args),
			)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Annotatef(err,
				"rows.Scan\n%s\n%v",
				sqlText, CompactJSON(finalArgs),
			)
		}

		list = append(list, r)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", sqlText, finalArgs)
	}

	return list, nil
}

func GetOrErr[T any](
	ctx context.Context,
	tx SqlExecutor,
	qb *Query,
	args ...any,
) (*T, error) {
	return Trace1(ctx, func(ctx context.Context) (*T, error) {
		return getOrErrFn[T](ctx, tx, qb, args...)
	})
}

func getOrErrFn[T any](
	ctx context.Context,
	tx SqlExecutor,
	qb *Query,
	args ...any,
) (*T, error) {
	if err := validateQuery(qb); err != nil {
		return nil, errors.Trace(err)
	}

	if err := validateOperationalExecutorForSQL(tx, qb.listSQL); err != nil {
		return nil, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, qb.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("getOrErr", "sql", sqlText, "args", CompactJSON(args))
	}

	r := new(T)

	dest, err := buildScanDest(qb.selectCols, r)
	if err != nil {
		return nil, errors.Annotatef(err,
			"build scan dest\n%s\n%v",
			sqlText, CompactJSON(args),
		)
	}

	finalArgs, err := mergeQueryArgs(qb.listArgs, args)
	if err != nil {
		return nil, errors.Trace(err)
	}

	row := tx.WithContext(ctx).QueryRow(sqlText, finalArgs...)

	if err := row.Scan(dest...); err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return nil, errors.Trace(sql.ErrNoRows)
		}

		return nil, errors.Annotatef(err,
			"row.Scan\n%s\n%v",
			sqlText, CompactJSON(finalArgs),
		)
	}

	return r, nil
}

func (q *Query) Load(
	ctx context.Context,
	tx SqlExecutor,
	holder any,
	args ...any,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return q.load(ctx, tx, holder, args...)
	})
}

func (q *Query) load(
	ctx context.Context,
	tx SqlExecutor,
	holder any,
	args ...any,
) error {
	if err := validateQuery(q); err != nil {
		return errors.Trace(err)
	}

	if err := validateOperationalExecutorForSQL(tx, q.listSQL); err != nil {
		return errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("load", "sql", sqlText, "args", CompactJSON(args))
	}

	dest, err := buildScanDest(q.selectCols, holder)
	if err != nil {
		return errors.Annotatef(err,
			"build scan dest\n%s\n%v",
			sqlText, CompactJSON(args),
		)
	}

	finalArgs, err := mergeQueryArgs(q.listArgs, args)
	if err != nil {
		return errors.Trace(err)
	}

	row := tx.WithContext(ctx).QueryRow(sqlText, finalArgs...)
	if err := row.Err(); err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return errors.Trace(sql.ErrNoRows)
		}

		return errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(finalArgs))
	}

	if err := row.Scan(dest...); err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return errors.Trace(sql.ErrNoRows)
		}

		return errors.Annotatef(err,
			"row.Scan\n%s\n%v",
			sqlText, CompactJSON(finalArgs),
		)
	}

	return nil
}

func (q *Query) buildPageSQLs(page *PageReq) (string, string, error) {
	if err := validateQuery(q); err != nil {
		return "", "", errors.Trace(err)
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
	registerSortableField := func(key string, qualifiedName string) {
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
		registerSortableField(f.Name(), rawColumnQualifiedName(f))

		if f.JSONFieldName() != "" && f.JSONFieldName() != "-" {
			registerSortableField(f.JSONFieldName(), rawColumnQualifiedName(f))
		}
	}

	if len(page.OrderBy) != 0 {
		orderbys := splitCommaValues(page.OrderBy)
		if len(orderbys) == 0 {
			return "", "", errors.New("order by fields cannot be empty")
		}

		orders, err := normalizeSortOrders(splitCommaValues(page.Order), len(orderbys))
		if err != nil {
			return "", "", errors.Trace(err)
		}

		var fullNames []string

		for i, ob := range orderbys {
			ob = strings.TrimSpace(ob)
			if _, ok := ambiguousFields[ob]; ok {
				return "", "", NewErrAmbiguousSortField(ob)
			}

			fullName, ok := allowedFields[ob]

			if !ok {
				return "", "", NewErrUnknownSortField(ob)
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

// ChunkedInsert 按块逐条插入数据。
func ChunkedInsert[T Table](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
	options ...*ChunkedInsertOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return chunkedInsertFn[T](ctx, tx, items, options...)
	})
}

func chunkedInsertFn[T Table](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
	options ...*ChunkedInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	opts, err := normalizeChunkedInsertOptions(options...)
	if err != nil {
		return errors.Trace(err)
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(items))

		batch := items[i:end]
		if err := chunkedInsertChunk(ctx, tx, batch, opts); err != nil {
			return errors.Annotatef(err, "chunked insert failed at index %d", i)
		}
	}

	return nil
}

func chunkedInsertChunk[T Table](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
	opts *ChunkedInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	// Insert items in chunks. For future optimization opportunities,
	// consider batching multiple inserts in a single transaction or using bulk insert features.
	for itemIdx, item := range items {
		if item == nil {
			return errors.Errorf("item at index %d is nil", itemIdx)
		}

		var err error
		if opts.IgnoreErrors {
			// 忽略错误模式：尝试插入，如果失败则跳过
			if err = tx.WithContext(ctx).Insert(item); err != nil {
				if isDuplicateKeyError(err) {
					slog.Debug("Ignored duplicate key error in batch insert", "error", err)
					continue
				}

				return errors.Annotatef(err, "chunked insert failed at item %d", itemIdx)
			}
		} else {
			if err = tx.WithContext(ctx).Insert(item); err != nil {
				return errors.Annotatef(err, "chunked insert failed at item %d", itemIdx)
			}
		}
	}

	return nil
}

// ChunkedUpdate 按块逐条更新数据。
func ChunkedUpdate[T any](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
	options ...*ChunkedOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return chunkedUpdateFn[T](ctx, tx, items, options...)
	})
}

func chunkedUpdateFn[T any](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
	options ...*ChunkedOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return errors.Trace(err)
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.ChunkSize {
		end := i + opts.ChunkSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		if err := chunkedUpdateChunk(ctx, tx, batch); err != nil {
			return errors.Annotatef(err, "chunked update failed at index %d", i)
		}
	}

	return nil
}

func chunkedUpdateChunk[T any](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
) error {
	for itemIdx, item := range items {
		if item == nil {
			return errors.Errorf("item at index %d is nil", itemIdx)
		}

		if _, err := tx.WithContext(ctx).Update(item); err != nil {
			return errors.Annotate(err, "chunked update item failed")
		}
	}

	return nil
}

// ChunkedDelete 按块逐条删除数据。
func ChunkedDelete[T any](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
	options ...*ChunkedOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return chunkedDeleteFn[T](ctx, tx, items, options...)
	})
}

func chunkedDeleteFn[T any](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
	options ...*ChunkedOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return errors.Trace(err)
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.ChunkSize {
		end := i + opts.ChunkSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		if err := chunkedDeleteChunk(ctx, tx, batch); err != nil {
			return errors.Annotatef(err, "chunked delete failed at index %d", i)
		}
	}

	return nil
}

func chunkedDeleteChunk[T any](
	ctx context.Context,
	tx SqlExecutor,
	items []*T,
) error {
	for itemIdx, item := range items {
		if item == nil {
			return errors.Errorf("item at index %d is nil", itemIdx)
		}

		if _, err := tx.WithContext(ctx).Delete(item); err != nil {
			return errors.Annotate(err, "chunked delete item failed")
		}
	}

	return nil
}

// ChunkedDeleteByIDs 按块根据 ID 列表删除数据。
func ChunkedDeleteByIDs(
	ctx context.Context,
	tx SqlExecutor,
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
	tx SqlExecutor,
	tableName string,
	idColumn string,
	ids []any,
	options ...*ChunkedOptions,
) error {
	if len(ids) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	if err := validateIDValues(ids); err != nil {
		return errors.Trace(err)
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return errors.Trace(err)
	}

	// 批量处理
	for i := 0; i < len(ids); i += opts.ChunkSize {
		end := i + opts.ChunkSize
		if end > len(ids) {
			end = len(ids)
		}

		batch := ids[i:end]
		if err := chunkedDeleteByIDsChunk(ctx, tx, tableName, idColumn, batch); err != nil {
			return errors.Annotatef(err, "chunked delete by IDs failed at index %d", i)
		}
	}

	return nil
}

func chunkedDeleteByIDsChunk(
	ctx context.Context,
	tx SqlExecutor,
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
		return errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, sqlStr)

	if err := validateOperationalExecutorForSQL(tx, sqlStr); err != nil {
		return errors.Trace(err)
	}

	_, err = tx.WithContext(ctx).Exec(sqlText, ids...)
	if err != nil {
		return errors.Annotatef(err, "chunked delete by IDs failed: %s", sqlText)
	}

	return nil
}

func buildDeleteByIDsSQL(tableName string, idColumn string, placeholderCount int) (string, error) {
	if placeholderCount <= 0 {
		return "", errors.New("placeholder count must be greater than 0")
	}

	quotedTable, err := quoteBuiltInIdentifier(tableName)
	if err != nil {
		return "", errors.Trace(err)
	}

	quotedColumn, err := quoteBuiltInIdentifier(idColumn)
	if err != nil {
		return "", errors.Trace(err)
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
		return "", errors.Errorf("invalid SQL identifier: %s", name)
	}

	// Warn if identifier is very long (>50 chars) as it may exceed limits in some databases
	if len(name) > 50 {
		slog.Warn("identifier is unusually long", "identifier", name, "length", len(name))
	}

	return rawIdentifier(name), nil
}

// ValidateIdentifierForDialect is a utility function to validate an identifier's form and length
// for a specific database dialect. This combines pattern validation (via quoteBuiltInIdentifier)
// with dialect-specific length validation (via ValidateIdentifierLength).
//
// Returns an error if:
// - The identifier is empty
// - The identifier doesn't match SQL identifier pattern [A-Za-z_][A-Za-z0-9_]*
// - The identifier exceeds the dialect's maximum length
//
// dialect should be one of: "mysql", "postgres", "oracle", "sqlite", or empty to skip length validation.
func ValidateIdentifierForDialect(identifier, dialect string) error {
	if identifier == "" {
		return errors.New("identifier cannot be empty")
	}

	if !builtInIdentifierPattern.MatchString(identifier) {
		return errors.Errorf("invalid SQL identifier: %s (must match pattern [A-Za-z_][A-Za-z0-9_]*)", identifier)
	}

	return ValidateIdentifierLength(identifier, dialect)
}

// ValidateIdentifierLength checks if an identifier conforms to length limits for a specific dialect.
// dialect should be one of: "mysql", "postgres", "oracle", "sqlite", or empty to skip validation.
func ValidateIdentifierLength(identifier string, dialect string) error {
	if identifier == "" {
		return errors.New("identifier cannot be empty")
	}

	var maxLen int
	switch strings.ToLower(dialect) {
	case "mysql":
		maxLen = MaxIdentifierLengthMySQL
	case "postgres", "postgresql":
		maxLen = MaxIdentifierLengthPostgreSQL
	case "oracle":
		maxLen = MaxIdentifierLengthOracleSQL
	case "sqlite", "":
		// SQLite has no practical limit
		return nil
	default:
		// Unknown dialect, skip validation
		return nil
	}

	if maxLen > 0 && len(identifier) > maxLen {
		return errors.Errorf("identifier %q exceeds %s maximum length of %d characters (got %d)", identifier, dialect, maxLen, len(identifier))
	}

	return nil
}

// ================================================
// 批量操作辅助函数
// ================================================

// isDuplicateKeyError 检查是否是重复键错误
// 这里提供一个简化的实现，实际项目中应该根据具体数据库类型进行判断
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// MySQL 重复键错误关键词
	mysqlKeywords := []string{
		"duplicate entry",
		"duplicate key",
		"unique constraint",
		"primary key",
	}

	// PostgreSQL 重复键错误关键词
	pgKeywords := []string{
		"duplicate key value",
		"unique_violation",
		"unique constraint",
	}

	// SQLite 重复键错误关键词
	sqliteKeywords := []string{
		"unique constraint failed",
		"primary key constraint failed",
	}

	allKeywords := append(mysqlKeywords, pgKeywords...)
	allKeywords = append(allKeywords, sqliteKeywords...)

	for _, keyword := range allKeywords {
		if strings.Contains(errStr, keyword) {
			return true
		}
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
		return nil, errors.Trace(err)
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
		return nil, errors.Trace(err)
	}

	return opts, nil
}

func validateChunkSize(chunkSize int) error {
	if chunkSize <= 0 {
		return errors.Errorf("invalid chunk size: %d", chunkSize)
	}

	return nil
}

func validateIDValues(ids []any) error {
	for i, id := range ids {
		if isNilValue(id) {
			return errors.Errorf("id at index %d cannot be nil", i)
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

func mergeQueryArgs(base []any, extra []any) ([]any, error) {
	return resolveQueryArgs(base, extra, "")
}

func resolveQueryArgs(base []any, extra []any, keyword string) ([]any, error) {
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

func queryArgs(q *Query) []any {
	if q == nil {
		return nil
	}

	return slices.Clone(q.listArgs)
}

func validateQuery(q *Query) error {
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

func validateExecutor(tx SqlExecutor) error {
	if tx == nil {
		return errors.New("sql executor cannot be nil")
	}

	value := reflect.ValueOf(tx)
	if value.IsValid() && value.Kind() == reflect.Ptr && value.IsNil() {
		return errors.New("sql executor cannot be nil")
	}

	return nil
}

func validateOperationalExecutor(tx SqlExecutor) error {
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	if dbMap, ok := tx.(*DbMap); ok && dbMap.Db == nil {
		return errors.New("db map database cannot be nil")
	}

	return nil
}

func validateExecutorForSQL(tx SqlExecutor, rawSQLs ...string) error {
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	dialect := dialectForExecutor(tx)
	if dialect != nil {
		if !dialectSupportsFullJoin(dialect) {
			for _, rawSQL := range rawSQLs {
				if strings.Contains(strings.ToUpper(rawSQL), " FULL JOIN ") {
					return errors.New("FULL JOIN is not supported by this SQL dialect")
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

func dialectSupportsFullJoin(dialect Dialect) bool {
	switch dialect.(type) {
	case MySQLDialect, *MySQLDialect, SqliteDialect, *SqliteDialect:
		return false
	default:
		return true
	}
}

func validateOperationalExecutorForSQL(tx SqlExecutor, rawSQLs ...string) error {
	if err := validateOperationalExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	return validateExecutorForSQL(tx, rawSQLs...)
}

func validateMutationItem(item any) error {
	if isNilValue(item) {
		return errors.New("mutation item cannot be nil")
	}

	return nil
}

func validateScanHolder(holder any) error {
	if isNilValue(holder) {
		return errors.New("scan holder cannot be nil")
	}

	if reflect.ValueOf(holder).Kind() != reflect.Ptr {
		return errors.New("scan holder must be a pointer")
	}

	return nil
}

func buildScanDest(cols []Column, holder any) ([]any, error) {
	if err := validateScanHolder(holder); err != nil {
		return nil, errors.Trace(err)
	}

	dest := make([]any, len(cols))

	for i, col := range cols {
		pointerFunc := col.FieldPointer()
		if pointerFunc == nil {
			return nil, errors.Errorf("select column %s cannot be scanned: field pointer is nil", col.QualifiedName())
		}

		ptr, err := invokeFieldPointer(pointerFunc, holder)
		if err != nil {
			return nil, errors.Annotatef(err, "select column %s cannot be scanned", col.QualifiedName())
		}

		if ptr == nil {
			return nil, errors.Errorf("select column %s cannot be scanned: field pointer returned nil", col.QualifiedName())
		}

		dest[i] = ptr
	}

	return dest, nil
}

func validateScanDestForType[T any](cols []Column, sqlText string, args []any) error {
	holder := new(T)
	if _, err := buildScanDest(cols, holder); err != nil {
		return errors.Annotatef(err,
			"build scan dest\n%s\n%v",
			sqlText, CompactJSON(args),
		)
	}

	return nil
}

func invokeFieldPointer(pointerFunc FieldPointer, holder any) (ptr any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Errorf("field pointer panicked: %v", recovered)
		}
	}()

	return pointerFunc(holder), nil
}

func Insert[T any](
	ctx context.Context,
	tx SqlExecutor,
	item *T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return insertFn(ctx, tx, item)
	})
}

func insertFn[T any](
	ctx context.Context,
	tx SqlExecutor,
	item *T,
) error {
	if err := validateMutationItem(item); err != nil {
		return errors.Trace(err)
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	return tx.WithContext(ctx).Insert(item)
}

func Update[T any](
	ctx context.Context,
	tx SqlExecutor,
	item *T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return updateFn(ctx, tx, item)
	})
}

func updateFn[T any](
	ctx context.Context,
	tx SqlExecutor,
	item *T,
) error {
	if err := validateMutationItem(item); err != nil {
		return errors.Trace(err)
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	_, err := tx.WithContext(ctx).Update(item)

	return errors.Trace(err)
}

func Delete[T any](
	ctx context.Context,
	tx SqlExecutor,
	item *T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return deleteFn(ctx, tx, item)
	})
}

func deleteFn[T any](
	ctx context.Context,
	tx SqlExecutor,
	item *T,
) error {
	if err := validateMutationItem(item); err != nil {
		return errors.Trace(err)
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	_, err := tx.WithContext(ctx).Delete(item)

	return errors.Trace(err)
}
