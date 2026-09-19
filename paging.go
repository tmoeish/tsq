package tsq

import (
	"errors"
	"fmt"
)

// defaultPageSize is the page size of a Paging that sets none.
const defaultPageSize = 20

// MaxPageNumber caps the page number. The offset is Size*(Page-1), and with Size
// capped at DefaultMaxPageSize the largest offset stays inside int on 32-bit builds.
const MaxPageNumber = 1000000

// Paging selects one page of a query for Query.Page.
type Paging struct {
	// Page is the 1-based page number; below 1 means 1.
	Page int
	// Size is the page size; 0 means 20, and the runtime's WithMaxPageSize caps it.
	Size int
	// OrderBy orders the rows. It must be empty when the query orders itself.
	OrderBy []OrderBy
}

func (p Paging) normalized(maxSize int) Paging {
	maxSize = boundPageSize(maxSize)
	p.Page = min(max(p.Page, 1), MaxPageNumber)

	if p.Size <= 0 {
		p.Size = defaultPageSize
	}

	p.Size = min(p.Size, maxSize)

	return p
}

// Offset is the number of rows before the page, after the same normalization Page
// applies (without a runtime's size cap).
func (p Paging) Offset() int {
	p = p.normalized(DefaultMaxPageSize)

	return p.Size * (p.Page - 1)
}

// Page is one page of rows plus the total count.
type Page[T any] struct {
	Page       int   `json:"page"`        // Page is the 1-based page number served.
	Size       int   `json:"size"`        // Size is the page size served.
	Total      int64 `json:"total"`       // Total is the number of matching rows.
	TotalPages int64 `json:"total_pages"` // TotalPages is Total divided by Size, rounded up.
	Data       []*T  `json:"data"`        // Data holds the rows of the page, never nil.
}

func newPage[T any](p Paging, total int64, data []*T) *Page[T] {
	if data == nil {
		data = make([]*T, 0)
	}

	return &Page[T]{
		Page:       p.Page,
		Size:       p.Size,
		Total:      total,
		TotalPages: (total + int64(p.Size) - 1) / int64(p.Size),
		Data:       data,
	}
}

// HasNext reports whether another page follows.
func (r *Page[T]) HasNext() bool { return r != nil && int64(r.Page) < r.TotalPages }

// HasPrev reports whether a page precedes.
func (r *Page[T]) HasPrev() bool { return r != nil && r.Page > 1 }

// IsEmpty reports whether the page holds no rows.
func (r *Page[T]) IsEmpty() bool { return r == nil || len(r.Data) == 0 }

// PageRequest is the HTTP shape of a page request: strings as a client sends them.
// Turn it into a Paging (or a Keyset) with the columns the endpoint allows
// sorting by; that is also where it is validated.
type PageRequest struct {
	Size    int    `json:"size"     query:"size"`     // Size is the requested page size.
	Page    int    `json:"page"     query:"page"`     // Page is the 1-based page number.
	OrderBy string `json:"order_by" query:"order_by"` // OrderBy lists sort fields separated by commas.
	Order   string `json:"order"    query:"order"`    // Order lists asc/desc aligned with OrderBy.
	Keyword string `json:"keyword"  query:"keyword"`  // Keyword is the optional search term.
	After   string `json:"after"    query:"after"`    // After is the cursor of a keyset page.
}

// Paging resolves the request against the columns it may sort by. A sort field
// names a column by its JSON field name or its column name; any other name is an
// UnknownSortFieldError, so a client can only sort by what the endpoint allows.
//
// A page or size below zero, a page above MaxPageNumber, or an order that is not
// asc/desc is an error. Zero means the first page and the default size. A size
// above the runtime's WithMaxPageSize is not an error: Page serves the capped
// size, and the response says which.
func (r *PageRequest) Paging(sortable ...SQLColumn) (Paging, error) {
	if r == nil {
		return Paging{}, nil
	}

	if err := r.validate(); err != nil {
		return Paging{}, err
	}

	order, err := r.orderBy(sortable)
	if err != nil {
		return Paging{}, err
	}

	return Paging{Page: r.Page, Size: r.Size, OrderBy: order}, nil
}

// Keyset resolves the request as a keyset page: Size, the sort fields as Paging
// resolves them, and After. Page is ignored. The last sort field must be the
// primary key, which the endpoint can append rather than leave to the client.
func (r *PageRequest) Keyset(sortable ...SQLColumn) (Keyset, error) {
	if r == nil {
		return Keyset{}, nil
	}

	if err := r.validate(); err != nil {
		return Keyset{}, err
	}

	order, err := r.orderBy(sortable)
	if err != nil {
		return Keyset{}, err
	}

	return Keyset{Size: r.Size, OrderBy: order, After: r.After}, nil
}

func (r *PageRequest) orderBy(sortable []SQLColumn) ([]OrderBy, error) {
	fields := splitCommaValues(r.OrderBy)
	if len(fields) == 0 {
		if len(splitCommaValues(r.Order)) > 0 {
			return nil, errors.New("order requires order_by")
		}

		return nil, nil
	}

	directions, err := normalizeSortOrders(splitCommaValues(r.Order), len(fields))
	if err != nil {
		return nil, err
	}

	var order []OrderBy

	byName := make(map[string][]SQLColumn)

	for _, col := range sortable {
		if isNilValue(col) {
			continue
		}

		byName[col.Name()] = append(byName[col.Name()], col)

		if json := col.JSONFieldName(); json != "" && json != "-" && json != col.Name() {
			byName[json] = append(byName[json], col)
		}
	}

	for i, field := range fields {
		matches := byName[field]

		switch {
		case len(matches) == 0:
			return nil, &UnknownSortFieldError{Field: field}
		case len(matches) > 1:
			return nil, &AmbiguousSortFieldError{Field: field}
		}

		order = append(order, OrderBy{column: matches[0], direction: directions[i]})
	}

	return order, nil
}

// UnknownSortFieldError reports a sort field the endpoint does not allow.
type UnknownSortFieldError struct {
	Field string
}

func (e *UnknownSortFieldError) Error() string { return "unknown sort field: " + e.Field }

// AmbiguousSortFieldError reports a sort field that names more than one column.
type AmbiguousSortFieldError struct {
	Field string
}

func (e *AmbiguousSortFieldError) Error() string { return "ambiguous sort field: " + e.Field }

// OrderCountMismatchError reports order_by and order lists of different lengths.
type OrderCountMismatchError struct {
	Fields     int
	Directions int
}

func (e *OrderCountMismatchError) Error() string {
	return fmt.Sprintf("order_by lists %d fields but order lists %d directions", e.Fields, e.Directions)
}

// validate rejects what no page can mean; out-of-range sizes are capped by Page.
func (r *PageRequest) validate() error {
	if r.Page < 0 {
		return fmt.Errorf("page must not be negative, got %d", r.Page)
	}

	// Offset is Size*(Page-1) and has to stay well inside int on 32-bit builds.
	if r.Page > MaxPageNumber {
		return fmt.Errorf("page must be less than or equal to %d, got %d", MaxPageNumber, r.Page)
	}

	if r.Size < 0 {
		return fmt.Errorf("size must not be negative, got %d", r.Size)
	}

	for _, rawOrder := range splitCommaValues(r.Order) {
		if _, err := parseOrder(rawOrder); err != nil {
			return err
		}
	}

	return nil
}

// boundPageSize resolves a page-size limit. DefaultMaxPageSize is the default,
// not a ceiling: a runtime built with WithMaxPageSize decides its own cap, in
// either direction.
func boundPageSize(maxSize int) int {
	if maxSize <= 0 {
		return DefaultMaxPageSize
	}

	return maxSize
}
