package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/juju/errors"
	"gopkg.in/gorp.v2"
)

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

	// Metadata
	selectCols   []Column
	selectTables map[string]Table
	kwCols       []Column
	kwTables     map[string]Table
}

var builtInIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ================================================
// SQL 访问方法
// ================================================

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

// MustBuild builds the query and panics on error
func (qb *QueryBuilder) MustBuild() *Query {
	q, err := qb.Build()
	if err != nil {
		panic(errors.ErrorStack(err))
	}

	return q
}

// Build builds and validates the query
func (qb *QueryBuilder) Build() (*Query, error) {
	if qb == nil {
		return nil, errors.New("query builder cannot be nil")
	}

	if qb.buildErr != nil {
		return nil, errors.Trace(qb.buildErr)
	}

	if len(qb.selectCols) == 0 {
		return nil, errors.Errorf("empty select fields: %+v", qb)
	}

	for _, join := range qb.joins {
		if join.joinType == FullJoinType {
			return nil, errors.New("FULL JOIN is not supported by TSQ's built-in dialects")
		}
	}

	if err := qb.validateJoinGraph(); err != nil {
		return nil, errors.Trace(err)
	}

	// Build all SQL variations
	cntSQL := qb.buildCntSQL()
	listSQL := qb.buildListSQL()
	kwCntSQL := qb.buildKwCntSQL()
	kwListSQL := qb.buildKwListSQL()

	return &Query{
		cntSQL:    cntSQL,
		listSQL:   listSQL,
		kwCntSQL:  kwCntSQL,
		kwListSQL: kwListSQL,

		selectCols:   slices.Clone(qb.selectCols),
		selectTables: maps.Clone(qb.selectTables),
		kwCols:       slices.Clone(qb.kwCols),
		kwTables:     maps.Clone(qb.kwTables),
	}, nil
}

// ================================================
// 基础查询执行方法
// ================================================

// QueryInt executes the query and returns a single integer result
func (q *Query) QueryInt(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int64, error) {
	return Trace1(ctx, func(ctx context.Context) (int64, error) {
		return q.queryInt(ctx, tx, args...)
	})
}

func (q *Query) queryInt(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int64, error) {
	if err := validateQuery(q); err != nil {
		return 0, errors.Trace(err)
	}
	if err := validateExecutorForSQL(tx, q.listSQL); err != nil {
		return 0, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("queryInt", "sql", sqlText, "args", CompactJSON(args))
	}

	result, err := tx.WithContext(ctx).SelectInt(sqlText, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(args))
	}

	return result, nil
}

// QueryFloat executes the query and returns a single float result
func (q *Query) QueryFloat(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (float64, error) {
	return Trace1(ctx, func(ctx context.Context) (float64, error) {
		return q.queryFloat(ctx, tx, args...)
	})
}

func (q *Query) queryFloat(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (float64, error) {
	if err := validateQuery(q); err != nil {
		return 0, errors.Trace(err)
	}
	if err := validateExecutorForSQL(tx, q.listSQL); err != nil {
		return 0, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("queryFloat", "sql", sqlText, "args", CompactJSON(args))
	}

	result, err := tx.WithContext(ctx).SelectFloat(sqlText, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(args))
	}

	return result, nil
}

// QueryStr executes the query and returns a single string result
func (q *Query) QueryStr(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (string, error) {
	return Trace1(ctx, func(ctx context.Context) (string, error) {
		return q.queryStr(ctx, tx, args...)
	})
}

func (q *Query) queryStr(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (string, error) {
	if err := validateQuery(q); err != nil {
		return "", errors.Trace(err)
	}
	if err := validateExecutorForSQL(tx, q.listSQL); err != nil {
		return "", errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("queryStr", "sql", sqlText, "args", CompactJSON(args))
	}

	result, err := tx.WithContext(ctx).SelectStr(sqlText, args...)
	if err != nil {
		return "", errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(args))
	}

	return result, nil
}

// Count executes the count query and returns the number of matching records
func (q *Query) Count(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int, error) {
	return Trace1(ctx, func(ctx context.Context) (int, error) {
		return q.count(ctx, tx, args...)
	})
}

func (q *Query) count(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int, error) {
	if err := validateQuery(q); err != nil {
		return 0, errors.Trace(err)
	}
	if err := validateExecutorForSQL(tx, q.cntSQL); err != nil {
		return 0, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.cntSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("count", "sql", sqlText, "args", CompactJSON(args))
	}

	count, err := tx.WithContext(ctx).SelectInt(sqlText, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(args))
	}

	return int(count), nil
}

// Exists checks if any records match the query conditions
func (q *Query) Exists(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (bool, error) {
	return Trace1(ctx, func(ctx context.Context) (bool, error) {
		return q.exist(ctx, tx, args...)
	})
}

func (q *Query) exist(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (bool, error) {
	if err := validateQuery(q); err != nil {
		return false, errors.Trace(err)
	}
	if err := validateExecutorForSQL(tx, q.cntSQL); err != nil {
		return false, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.cntSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("exist", "sql", sqlText, "args", CompactJSON(args))
	}

	count, err := tx.WithContext(ctx).SelectInt(sqlText, args...)
	if err != nil {
		return false, errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(args))
	}

	return count > 0, nil
}

// ================================================
// 分页查询方法
// ================================================

// Page executes a paginated query with the given page parameters
func Page[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
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
	tx gorp.SqlExecutor,
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

	if err := validateExecutorForSQL(tx, cntSQL, listSQL); err != nil {
		return nil, errors.Trace(err)
	}

	renderedCntSQL := renderSQLForExecutor(tx, cntSQL)
	renderedListSQL := renderSQLForExecutor(tx, listSQL)

	// Add keyword search parameters if needed
	finalArgs := args

	if len(q.kwCols) > 0 && len(page.Keyword) > 0 {
		like := "%" + page.Keyword + "%"
		for range len(q.kwCols) {
			finalArgs = append(finalArgs, like)
		}
	}

	// Add LIMIT parameters
	argsWithLimit := append(finalArgs, page.Size, page.Offset())

	if ctx.Value(printSQL) != nil {
		slog.Info("count", "sql", renderedCntSQL, "args", CompactJSON(finalArgs))
		slog.Info("list", "sql", renderedListSQL, "args", CompactJSON(argsWithLimit))
	}

	// Execute count query
	count, err := tx.WithContext(ctx).SelectInt(renderedCntSQL, finalArgs...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", renderedCntSQL, CompactJSON(finalArgs))
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
	tx gorp.SqlExecutor,
	qb *Query,
	args ...any,
) ([]*T, error) {
	return Trace1(ctx, func(ctx context.Context) ([]*T, error) {
		return listFn[T](ctx, tx, qb, args...)
	})
}

func listFn[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	q *Query,
	args ...any,
) ([]*T, error) {
	if err := validateQuery(q); err != nil {
		return nil, errors.Trace(err)
	}
	if err := validateExecutorForSQL(tx, q.listSQL); err != nil {
		return nil, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("list", "sql", sqlText, "args", CompactJSON(args))
	}

	rows, err := tx.WithContext(ctx).Query(sqlText, args...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", sqlText, args)
	}

	defer func() {
		_ = rows.Close()
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
				sqlText, CompactJSON(args),
			)
		}

		list = append(list, r)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", sqlText, args)
	}

	return list, nil
}

func GetOrErr[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	qb *Query,
	args ...any,
) (*T, error) {
	return Trace1(ctx, func(ctx context.Context) (*T, error) {
		return getOrErrFn[T](ctx, tx, qb, args...)
	})
}

func getOrErrFn[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	qb *Query,
	args ...any,
) (*T, error) {
	if err := validateQuery(qb); err != nil {
		return nil, errors.Trace(err)
	}
	if err := validateExecutorForSQL(tx, qb.listSQL); err != nil {
		return nil, errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, qb.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("getOrErr", "sql", sqlText, "args", CompactJSON(args))
	}

	row := tx.WithContext(ctx).QueryRow(sqlText, args...)

	r := new(T)
	dest, err := buildScanDest(qb.selectCols, r)
	if err != nil {
		return nil, errors.Annotatef(err,
			"build scan dest\n%s\n%v",
			sqlText, CompactJSON(args),
		)
	}

	if err := row.Scan(dest...); err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}

		return nil, errors.Annotatef(err,
			"row.Scan\n%s\n%v",
			sqlText, CompactJSON(args),
		)
	}

	return r, nil
}

func (q *Query) Load(
	ctx context.Context,
	tx gorp.SqlExecutor,
	holder any,
	args ...any,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return q.load(ctx, tx, holder, args...)
	})
}

func (q *Query) load(
	ctx context.Context,
	tx gorp.SqlExecutor,
	holder any,
	args ...any,
) error {
	if err := validateQuery(q); err != nil {
		return errors.Trace(err)
	}
	if err := validateExecutorForSQL(tx, q.listSQL); err != nil {
		return errors.Trace(err)
	}

	sqlText := renderSQLForExecutor(tx, q.listSQL)

	if ctx.Value(printSQL) != nil {
		slog.Info("load", "sql", sqlText, "args", CompactJSON(args))
	}

	row := tx.WithContext(ctx).QueryRow(sqlText, args...)
	if err := row.Err(); err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return sql.ErrNoRows
		}

		return errors.Annotatef(err, "\n%s\n%v", sqlText, CompactJSON(args))
	}

	dest, err := buildScanDest(q.selectCols, holder)
	if err != nil {
		return errors.Annotatef(err,
			"build scan dest\n%s\n%v",
			sqlText, CompactJSON(args),
		)
	}

	if err := row.Scan(dest...); err != nil {
		if errors.Is(errors.Cause(err), sql.ErrNoRows) {
			return sql.ErrNoRows
		}

		return errors.Annotatef(err,
			"row.Scan\n%s\n%v",
			sqlText, CompactJSON(args),
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
		if f.JSONFieldName() != "" {
			registerSortableField(f.JSONFieldName(), rawColumnQualifiedName(f))
		}
	}

	if len(page.OrderBy) != 0 {
		orderbys := splitCommaValues(page.OrderBy)
		orders := splitCommaValues(page.Order)

		if len(orderbys) == 0 {
			return "", "", errors.New("order by fields cannot be empty")
		}

		if len(orders) == 0 {
			orders = make([]string, len(orderbys))
			for i := range orders {
				orders[i] = "ASC"
			}
		}

		if len(orders) != len(orderbys) {
			return "", "", NewErrOrderCountMismatch(len(orderbys), len(orders))
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

			order := strings.ToUpper(strings.TrimSpace(orders[i]))
			if order != "ASC" && order != "DESC" {
				return "", "", errors.Errorf("invalid order: %s", orders[i])
			}

			fullNames = append(fullNames, fullName+" "+order)
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

// BatchOptions 批量操作通用配置选项
type BatchOptions struct {
	BatchSize int // 每批处理的数量，默认 1000
}

// DefaultBatchOptions 返回默认的批量操作配置
func DefaultBatchOptions() *BatchOptions {
	return &BatchOptions{
		BatchSize: 1000,
	}
}

// BatchInsertOptions 批量插入配置选项
type BatchInsertOptions struct {
	BatchSize    int  // 每批处理的数量，默认 1000
	IgnoreErrors bool // 是否忽略重复键插入错误并继续处理后续数据
}

// DefaultBatchInsertOptions 返回默认的批量插入配置
func DefaultBatchInsertOptions() *BatchInsertOptions {
	return &BatchInsertOptions{
		BatchSize:    DefaultBatchOptions().BatchSize,
		IgnoreErrors: false,
	}
}

// BatchInsert 批量插入数据
// T 必须是结构体类型，且应该有相应的数据库字段映射
func BatchInsert[T Table](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
	options ...*BatchInsertOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return batchInsertFn[T](ctx, tx, items, options...)
	})
}

func batchInsertFn[T Table](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
	options ...*BatchInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	opts, err := normalizeBatchInsertOptions(options...)
	if err != nil {
		return errors.Trace(err)
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		if err := batchInsertChunk(ctx, tx, batch, opts); err != nil {
			return errors.Annotatef(err, "batch insert chunk failed at index %d", i)
		}
	}

	return nil
}

// batchInsertChunk 插入一个批次的数据，使用 gorp 的标准插入
func batchInsertChunk[T Table](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
	opts *BatchInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	// 简化版本：使用 gorp 的标准插入，逐个插入
	// 在实际生产环境中，可以根据需要实现更高效的批量插入
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

				return errors.Annotatef(err, "batch insert failed at item %d", itemIdx)
			}
		} else {
			// 标准插入模式
			if err = tx.WithContext(ctx).Insert(item); err != nil {
				return errors.Annotatef(err, "batch insert failed at item %d", itemIdx)
			}
		}
	}

	return nil
}

// BatchUpdate 批量更新数据
// 注意：这是一个简化版本，每个条目单独更新
// 更高效的实现可能需要使用 CASE WHEN 语句或临时表
func BatchUpdate[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
	options ...*BatchOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return batchUpdateFn[T](ctx, tx, items, options...)
	})
}

func batchUpdateFn[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
	options ...*BatchOptions,
) error {
	if len(items) == 0 {
		return nil
	}
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	opts, err := normalizeBatchOptions(options...)
	if err != nil {
		return errors.Trace(err)
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		if err := batchUpdateChunk(ctx, tx, batch); err != nil {
			return errors.Annotatef(err, "batch update chunk failed at index %d", i)
		}
	}

	return nil
}

// batchUpdateChunk 更新一个批次的数据
func batchUpdateChunk[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
) error {
	for itemIdx, item := range items {
		if item == nil {
			return errors.Errorf("item at index %d is nil", itemIdx)
		}

		if _, err := tx.WithContext(ctx).Update(item); err != nil {
			return errors.Annotate(err, "batch update item failed")
		}
	}

	return nil
}

// BatchDelete 批量删除数据
func BatchDelete[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
	options ...*BatchOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return batchDeleteFn[T](ctx, tx, items, options...)
	})
}

func batchDeleteFn[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
	options ...*BatchOptions,
) error {
	if len(items) == 0 {
		return nil
	}
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	opts, err := normalizeBatchOptions(options...)
	if err != nil {
		return errors.Trace(err)
	}

	// 批量处理
	for i := 0; i < len(items); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		if err := batchDeleteChunk(ctx, tx, batch); err != nil {
			return errors.Annotatef(err, "batch delete chunk failed at index %d", i)
		}
	}

	return nil
}

// batchDeleteChunk 删除一个批次的数据
func batchDeleteChunk[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	items []*T,
) error {
	for itemIdx, item := range items {
		if item == nil {
			return errors.Errorf("item at index %d is nil", itemIdx)
		}

		if _, err := tx.WithContext(ctx).Delete(item); err != nil {
			return errors.Annotate(err, "batch delete item failed")
		}
	}

	return nil
}

// BatchDeleteByIDs 根据 ID 列表批量删除数据
func BatchDeleteByIDs(
	ctx context.Context,
	tx gorp.SqlExecutor,
	tableName string,
	idColumn string,
	ids []any,
	options ...*BatchOptions,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return batchDeleteByIDsFn(ctx, tx, tableName, idColumn, ids, options...)
	})
}

func batchDeleteByIDsFn(
	ctx context.Context,
	tx gorp.SqlExecutor,
	tableName string,
	idColumn string,
	ids []any,
	options ...*BatchOptions,
) error {
	if len(ids) == 0 {
		return nil
	}
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}
	if err := validateIDValues(ids); err != nil {
		return errors.Trace(err)
	}

	opts, err := normalizeBatchOptions(options...)
	if err != nil {
		return errors.Trace(err)
	}

	// 批量处理
	for i := 0; i < len(ids); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(ids) {
			end = len(ids)
		}

		batch := ids[i:end]
		if err := batchDeleteByIDsChunk(ctx, tx, tableName, idColumn, batch); err != nil {
			return errors.Annotatef(err, "batch delete by IDs chunk failed at index %d", i)
		}
	}

	return nil
}

// batchDeleteByIDsChunk 根据 ID 列表删除一个批次的数据
func batchDeleteByIDsChunk(
	ctx context.Context,
	tx gorp.SqlExecutor,
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

	if err := validateExecutorForSQL(tx, sqlStr); err != nil {
		return errors.Trace(err)
	}

	_, err = tx.WithContext(ctx).Exec(sqlText, ids...)
	if err != nil {
		return errors.Annotatef(err, "batch delete by IDs failed: %s", sqlText)
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

	return rawIdentifier(name), nil
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
	_ = normalized.Validate()

	return &normalized
}

func normalizeBatchInsertOptions(options ...*BatchInsertOptions) (*BatchInsertOptions, error) {
	if len(options) > 1 {
		return nil, errors.New("expected at most one batch insert options value")
	}

	opts := DefaultBatchInsertOptions()
	if len(options) > 0 && options[0] != nil {
		copied := *options[0]
		opts = &copied
	}

	if err := validateBatchSize(opts.BatchSize); err != nil {
		return nil, errors.Trace(err)
	}

	return opts, nil
}

func normalizeBatchOptions(options ...*BatchOptions) (*BatchOptions, error) {
	if len(options) > 1 {
		return nil, errors.New("expected at most one batch options value")
	}

	opts := DefaultBatchOptions()
	if len(options) > 0 && options[0] != nil {
		copied := *options[0]
		opts = &copied
	}

	if err := validateBatchSize(opts.BatchSize); err != nil {
		return nil, errors.Trace(err)
	}

	return opts, nil
}

func validateBatchSize(batchSize int) error {
	if batchSize <= 0 {
		return errors.Errorf("invalid batch size: %d", batchSize)
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

func validateExecutor(tx gorp.SqlExecutor) error {
	if tx == nil {
		return errors.New("sql executor cannot be nil")
	}

	value := reflect.ValueOf(tx)
	if value.IsValid() && value.Kind() == reflect.Ptr && value.IsNil() {
		return errors.New("sql executor cannot be nil")
	}

	return nil
}

func validateExecutorForSQL(tx gorp.SqlExecutor, rawSQLs ...string) error {
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}

	dialect := dialectForExecutor(tx)
	if dialect != nil {
		return nil
	}

	for _, rawSQL := range rawSQLs {
		if containsIdentifierMarkersNeedingRender(rawSQL) {
			return errors.New("sql executor dialect cannot be determined")
		}
	}

	return nil
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
	tx gorp.SqlExecutor,
	item *T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return insertFn(ctx, tx, item)
	})
}

func insertFn[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	item *T,
) error {
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}
	if err := validateMutationItem(item); err != nil {
		return errors.Trace(err)
	}

	return tx.WithContext(ctx).Insert(item)
}

func Update[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	item *T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return updateFn(ctx, tx, item)
	})
}

func updateFn[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	item *T,
) error {
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}
	if err := validateMutationItem(item); err != nil {
		return errors.Trace(err)
	}

	_, err := tx.WithContext(ctx).Update(item)
	return errors.Trace(err)
}

func Delete[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	item *T,
) error {
	return Trace(ctx, func(ctx context.Context) error {
		return deleteFn(ctx, tx, item)
	})
}

func deleteFn[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	item *T,
) error {
	if err := validateExecutor(tx); err != nil {
		return errors.Trace(err)
	}
	if err := validateMutationItem(item); err != nil {
		return errors.Trace(err)
	}

	_, err := tx.WithContext(ctx).Delete(item)
	return errors.Trace(err)
}
