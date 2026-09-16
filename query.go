package tsq

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
)

// UnknownSortFieldError reports that a requested sort field is unknown.
type UnknownSortFieldError struct {
	field string
}

// newErrUnknownSortField constructs an UnknownSortFieldError.
func newErrUnknownSortField(field string) *UnknownSortFieldError {
	return &UnknownSortFieldError{field: field}
}

// Error implements error.
func (e *UnknownSortFieldError) Error() string {
	return fmt.Sprintf("unknown sort field: %s", e.field)
}

// Is reports whether target is an *UnknownSortFieldError for the same field.
// An *UnknownSortFieldError with an empty field matches any UnknownSortFieldError,
// enabling both type-level and value-level errors.Is checks.
func (e *UnknownSortFieldError) Is(target error) bool {
	var other *UnknownSortFieldError

	ok := errors.As(target, &other)
	if !ok {
		return false
	}

	return other.field == "" || e.field == other.field
}

// AmbiguousSortFieldError reports that a sort field matches multiple selected columns.
type AmbiguousSortFieldError struct {
	field string
}

// newErrAmbiguousSortField constructs an AmbiguousSortFieldError.
func newErrAmbiguousSortField(field string) *AmbiguousSortFieldError {
	return &AmbiguousSortFieldError{field: field}
}

// Error implements error.
func (e *AmbiguousSortFieldError) Error() string {
	return fmt.Sprintf("ambiguous sort field: %s", e.field)
}

// Is reports whether target is an *AmbiguousSortFieldError for the same field.
// An *AmbiguousSortFieldError with an empty field matches any AmbiguousSortFieldError,
// enabling both type-level and value-level errors.Is checks.
func (e *AmbiguousSortFieldError) Is(target error) bool {
	var other *AmbiguousSortFieldError

	ok := errors.As(target, &other)
	if !ok {
		return false
	}

	return other.field == "" || e.field == other.field
}

// OrderCountMismatchError reports that the ORDER BY field and direction counts differ.
type OrderCountMismatchError struct {
	orderBys int
	orders   int
}

// newErrOrderCountMismatch constructs an OrderCountMismatchError.
func newErrOrderCountMismatch(orderbys, orders int) *OrderCountMismatchError {
	return &OrderCountMismatchError{orderBys: orderbys, orders: orders}
}

// Error implements error.
func (e *OrderCountMismatchError) Error() string {
	return fmt.Sprintf(
		"ORDER BY fields count(%d) and ORDER directions count(%d) mismatch",
		e.orderBys, e.orders,
	)
}

// Is reports whether target is an *OrderCountMismatchError with the same counts.
// An *OrderCountMismatchError with zero orderBys and zero orders matches any
// OrderCountMismatchError, enabling type-level errors.Is checks.
func (e *OrderCountMismatchError) Is(target error) bool {
	var other *OrderCountMismatchError

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
	// SQL templates rendered at Build time.
	cntSQL    string // COUNT query
	listSQL   string // main SELECT query
	kwCntSQL  string // COUNT query with keyword search
	kwListSQL string // SELECT query with keyword search

	// Base argument lists. May contain deferred-binding markers (externalArgMarker and friends).
	cntArgs    []any
	listArgs   []any
	kwCntArgs  []any
	kwListArgs []any

	cntArgState    queryArgState
	listArgState   queryArgState
	kwCntArgState  queryArgState
	kwListArgState queryArgState

	// Metadata.
	selectCols   []BoundColumn[O] // selected columns, used for Scan mapping
	selectTables map[string]Table // every table referenced by the query
	kwCols       []SearchColumn   // columns participating in keyword search
	kwTables     map[string]Table
	hasSetOps    bool // whether set operations (UNION etc.) are present; affects alias handling
	hasOrderBy   bool // whether the builder attached an ORDER BY; Page must not add a second one
	hasLimit     bool // whether the builder attached LIMIT/OFFSET; Page owns paging and refuses to fight it
	correlated   bool // whether the builder declared outer tables; such a query only runs inside an enclosing one
}

type (
	externalSliceArgMarker      struct{}
	externalNotInSliceArgMarker struct{}
)

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

// SearchCountSQL returns the keyword-search COUNT query SQL statement.
func (q *Query[O]) SearchCountSQL() string {
	if q == nil {
		return ""
	}

	return renderCanonicalSQL(q.kwCntSQL)
}

// SearchListSQL returns the keyword-search SELECT query SQL statement.
func (q *Query[O]) SearchListSQL() string {
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
