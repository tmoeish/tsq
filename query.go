package tsq

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
)

// ErrUnknownSortField reports that a requested sort field is unknown.
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
	var other *ErrUnknownSortField

	ok := errors.As(target, &other)
	if !ok {
		return false
	}

	return other.field == "" || e.field == other.field
}

// ErrAmbiguousSortField reports that a sort field matches multiple selected columns.
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
	var other *ErrAmbiguousSortField

	ok := errors.As(target, &other)
	if !ok {
		return false
	}

	return other.field == "" || e.field == other.field
}

// ErrOrderCountMismatch reports that the ORDER BY field and direction counts differ.
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
	var other *ErrOrderCountMismatch

	ok := errors.As(target, &other)
	if !ok {
		return false
	}

	return (other.orderBys == 0 && other.orders == 0) ||
		(e.orderBys == other.orderBys && e.orders == other.orders)
}

// Query is a compiled SQL query with count, list, and keyword-search variants.
// Query is the immutable, concurrency-safe result of Build, separating query
// definition from execution.
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

// Build once and reuse Query values on hot paths instead of rebuilding the same shape.

// CountSQL returns the COUNT query SQL statement.
func (q *Query[O]) CountSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.cntSQL)
}

// ListSQL returns the main SELECT query SQL statement.
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
