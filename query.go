package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
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

// ================================================
// SQL 访问方法
// ================================================

// CntSQL returns the COUNT query SQL statement
func (q *Query) CntSQL() string {
	return q.cntSQL
}

// ListSQL returns the main SELECT query SQL statement
func (q *Query) ListSQL() string {
	return q.listSQL
}

// KwCntSQL returns the keyword search COUNT query SQL statement
func (q *Query) KwCntSQL() string {
	return q.kwCntSQL
}

// KwListSQL returns the keyword search SELECT query SQL statement
func (q *Query) KwListSQL() string {
	return q.kwListSQL
}

// ================================================
// 查询构建器方法
// ================================================

// MustBuild builds the query and panics on error
func (qb *QueryBuilder) MustBuild() *Query {
	q, err := qb.Build()
	if err != nil {
		logrus.WithField("module", "main").Fatal(errors.ErrorStack(err))
	}

	return q
}

// Build builds and validates the query
func (qb *QueryBuilder) Build() (*Query, error) {
	if len(qb.selectTables) == 0 {
		return nil, errors.Errorf("empty select fields: %+v", qb)
	}

	// Validate that all selected fields are available in condition tables
	if len(qb.conditionTables) > 0 {
		for _, col := range qb.selectCols {
			if _, ok := qb.conditionTables[col.Table().Table()]; !ok {
				// TODO: add alias support
				return nil, errors.Errorf("cannot select field: %s", col.QualifiedName())
			}
		}
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
	logrus.Tracef("QueryInt:\n%s\n%v", q.listSQL, args)

	result, err := tx.WithContext(ctx).SelectInt(q.listSQL, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", q.listSQL, args)
	}

	return result, nil
}

// QueryFloat executes the query and returns a single float result
func (q *Query) QueryFloat(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (float64, error) {
	logrus.Tracef("QueryFloat:\n%s\n%v", q.listSQL, args)

	result, err := tx.WithContext(ctx).SelectFloat(q.listSQL, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", q.listSQL, args)
	}

	return result, nil
}

// QueryStr executes the query and returns a single string result
func (q *Query) QueryStr(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (string, error) {
	logrus.Tracef("QueryStr:\n%s\n%v", q.listSQL, args)

	result, err := tx.WithContext(ctx).SelectStr(q.listSQL, args...)
	if err != nil {
		return "", errors.Annotatef(err, "\n%s\n%v", q.listSQL, args)
	}

	return result, nil
}

// Count executes the count query and returns the number of matching records
func (q *Query) Count(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int, error) {
	logrus.Tracef("Count:\n%s\n%v", q.cntSQL, args)

	count, err := tx.WithContext(ctx).SelectInt(q.cntSQL, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", q.cntSQL, args)
	}

	return int(count), nil
}

// Exists checks if any records match the query conditions
func (q *Query) Exists(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (bool, error) {
	logrus.Tracef("Exists:\n%s\n%v", q.cntSQL, args)

	count, err := tx.WithContext(ctx).SelectInt(q.cntSQL, args...)
	if err != nil {
		return false, errors.Annotatef(err, "\n%s\n%v", q.cntSQL, args)
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
	cntSQL, listSQL, err := q.buildPageSQLs(page)
	if err != nil {
		return nil, errors.Trace(err)
	}

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

	// Execute count query
	logrus.Tracef("Count SQL: %s, args: %v", cntSQL, finalArgs)

	count, err := tx.WithContext(ctx).SelectInt(cntSQL, finalArgs...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", cntSQL, finalArgs)
	}

	// Execute list query
	logrus.Tracef("List SQL: %s, args: %v", listSQL, argsWithLimit)

	rows, err := tx.WithContext(ctx).Query(listSQL, argsWithLimit...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", listSQL, argsWithLimit)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close rows")
		}
	}()

	if err := rows.Err(); err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", listSQL, argsWithLimit)
	}

	// Scan results
	list := make([]*T, 0, page.Size) // Pre-allocate with expected size

	for rows.Next() {
		r := new(T)
		dest := make([]any, len(q.selectCols))

		for i, col := range q.selectCols {
			dest[i] = col.FieldPointer()(r)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Annotate(err, "rows.Scan")
		}

		list = append(list, r)
	}

	return NewResponse(page, count, list), nil
}

func List[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	qb *Query,
	args ...any,
) ([]*T, error) {
	logrus.Tracef("List:\n%s\n%v", qb.listSQL, args)

	rows, err := tx.WithContext(ctx).Query(qb.listSQL, args...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", qb.listSQL, args)
	}

	if rows.Err() != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", qb.listSQL, args)
	}

	defer func() {
		_ = rows.Close()
	}()

	var list []*T

	for rows.Next() {
		r := new(T)
		dest := make([]any, len(qb.selectCols))

		for i, f := range qb.selectCols {
			dest[i] = f.FieldPointer()(r)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Annotate(err, "rows.Scan")
		}

		list = append(list, r)
	}

	return list, nil
}

func GetOrErr[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	qb *Query,
	args ...any,
) (*T, error) {
	logrus.Tracef("GetOrErr:\n%s\n%v", qb.listSQL, args)

	row := tx.WithContext(ctx).QueryRow(qb.listSQL, args...)
	if err := row.Err(); err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}

		return nil, errors.Annotatef(err, "\n%s\n%v", qb.listSQL, args)
	}

	r := new(T)
	dest := make([]any, len(qb.selectCols))

	for i, f := range qb.selectCols {
		dest[i] = f.FieldPointer()(r)
	}

	if err := row.Scan(dest...); err != nil {
		return nil, errors.Annotate(err, "row.Scan")
	}

	return r, nil
}

func (qb *Query) Load(
	ctx context.Context,
	tx gorp.SqlExecutor,
	holder any,
	args ...any,
) error {
	logrus.Tracef("Load:\n%s\n%v", qb.listSQL, args)

	row := tx.WithContext(ctx).QueryRow(qb.listSQL, args...)
	if err := row.Err(); err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return sql.ErrNoRows
		}

		return errors.Annotatef(err, "\n%s\n%v", qb.listSQL, args)
	}

	dest := make([]any, len(qb.selectCols))
	for i, f := range qb.selectCols {
		dest[i] = f.FieldPointer()(holder)
	}

	if err := row.Scan(dest...); err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return sql.ErrNoRows
		}

		return errors.Annotate(err, "row.Scan")
	}

	return nil
}

func (qb *Query) buildPageSQLs(page *PageReq) (string, string, error) {
	var cntQuery, listQuery string
	if len(qb.kwCols) > 0 && len(page.Keyword) > 0 {
		cntQuery = qb.kwCntSQL
		listQuery = qb.kwListSQL
	} else {
		cntQuery = qb.cntSQL
		listQuery = qb.listSQL
	}

	// 排序字段白名单校验
	allowedFields := make(map[string]string)
	for _, f := range qb.selectCols {
		allowedFields[f.Name()] = f.QualifiedName()
		if f.JSONFieldName() != "" {
			allowedFields[f.JSONFieldName()] = f.QualifiedName()
		}
	}

	if len(page.OrderBy) != 0 {
		orderbys := strings.Split(page.OrderBy, ",")
		orders := strings.Split(page.Order, ",")

		if len(orders) != len(orderbys) {
			return "", "", NewErrOrderCountMismatch(len(orderbys), len(orders))
		}

		var fullNames []string

		for i, ob := range orderbys {
			ob = strings.TrimSpace(ob)
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

// ================================================
// 批量操作支持
// ================================================

// BatchInsertOptions 批量插入配置选项
type BatchInsertOptions struct {
	BatchSize      int  // 每批插入的数量，默认 1000
	IgnoreErrors   bool // 是否忽略重复键错误，使用 INSERT IGNORE
	OnDuplicateKey bool // 是否使用 ON DUPLICATE KEY UPDATE
}

// DefaultBatchInsertOptions 返回默认的批量插入配置
func DefaultBatchInsertOptions() *BatchInsertOptions {
	return &BatchInsertOptions{
		BatchSize:      1000,
		IgnoreErrors:   false,
		OnDuplicateKey: false,
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
	if len(items) == 0 {
		return nil
	}

	opts := DefaultBatchInsertOptions()
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
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
					logrus.Tracef("Ignored duplicate key error in batch insert: %v", err)
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
	options ...*BatchInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	opts := DefaultBatchInsertOptions()
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
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
	for _, item := range items {
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
	options ...*BatchInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	opts := DefaultBatchInsertOptions()
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
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
	for _, item := range items {
		if _, err := tx.WithContext(ctx).Delete(item); err != nil {
			return errors.Annotate(err, "batch delete item failed")
		}
	}

	return nil
}

// BatchDeleteByIDs 根据 ID 列表批量删除数据
func BatchDeleteByIDs[T any](
	ctx context.Context,
	tx gorp.SqlExecutor,
	tableName string,
	idColumn string,
	ids []any,
	options ...*BatchInsertOptions,
) error {
	if len(ids) == 0 {
		return nil
	}

	opts := DefaultBatchInsertOptions()
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
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

	sql := fmt.Sprintf(
		"DELETE FROM `%s` WHERE `%s` IN (%s)",
		tableName, idColumn, strings.Join(placeholders, ","),
	)

	logrus.Tracef("BatchDeleteByIDs SQL: %s, args: %v", sql, ids)

	_, err := tx.WithContext(ctx).Exec(sql, ids...)
	if err != nil {
		return errors.Annotatef(err, "batch delete by IDs failed: %s", sql)
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
