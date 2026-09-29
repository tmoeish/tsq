package tsq

import (
	"fmt"
)

// DefaultMaxPageSize caps Paging.Size unless WithMaxPageSize says otherwise.
const DefaultMaxPageSize = 1000

// defaultPageSize is the page size of a Paging that sets none.
const defaultPageSize = 20

// MaxPageNumber caps the page number, so that Size*(Page-1) stays inside int on
// 64-bit builds whatever WithMaxPageSize allows, and on 32-bit builds with Size at
// most DefaultMaxPageSize.
const MaxPageNumber = 1000000

// Paging selects one page of a query for Query.Page.
type Paging struct {
	// Page is the 1-based page number; below 1 means 1.
	Page int
	// Size is the page size; 0 means 20, and the runtime's WithMaxPageSize caps it.
	Size int
	// OrderBy orders the rows. It must be empty when the query orders itself.
	OrderBy []OrderBy

	// keyword is PageRequest.Keyword, which Page applies; see requestKeyword.
	keyword string
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

// offset is the number of rows before the page of a normalized p.
func (p Paging) offset() int { return p.Size * (p.Page - 1) }

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

// PageRequest is the HTTP shape of a page request, as a query string or JSON body
// decodes it: the sort as comma-separated names and directions.
// Turn it into a Paging (or a Keyset) with the columns the endpoint allows
// sorting by; that is also where it is validated.
type PageRequest struct {
	Size    int    `json:"size"     query:"size"`     // Size is the requested page size.
	Page    int    `json:"page"     query:"page"`     // Page is the 1-based page number.
	OrderBy string `json:"order_by" query:"order_by"` // OrderBy lists sort fields separated by commas.
	Order   string `json:"order"    query:"order"`    // Order lists asc/desc aligned with OrderBy, or one for every field.
	Keyword string `json:"keyword"  query:"keyword"`  // Keyword is the optional search term.
	After   string `json:"after"    query:"after"`    // After is the cursor of a keyset page.
}

// Paging resolves the request against the columns it may sort by. A sort field
// names a column by its JSON field name or its column name; any other name is a
// *PageRequestError, so a client can only sort by what the endpoint allows.
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

	return Paging{Page: r.Page, Size: r.Size, OrderBy: order, keyword: r.Keyword}, nil
}

// Keyset resolves the request as a keyset page: Size, the sort fields as Paging
// resolves them, and After. Page is ignored. The sort fields must include the
// primary key of every table the query reads (PageKeyset checks it), which the
// endpoint can append rather than leave to the client.
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

	return Keyset{Size: r.Size, OrderBy: order, After: r.After, keyword: r.Keyword}, nil
}

func (r *PageRequest) orderBy(sortable []SQLColumn) ([]OrderBy, error) {
	fields := splitCommaValues(r.OrderBy)
	if len(fields) == 0 {
		if len(splitCommaValues(r.Order)) > 0 {
			return nil, &PageRequestError{Reason: "order requires order_by"}
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

		if json := col.core().json; json != "" && json != "-" && json != col.Name() {
			byName[json] = append(byName[json], col)
		}
	}

	for i, field := range fields {
		matches := byName[field]

		switch {
		case len(matches) == 0:
			return nil, &PageRequestError{Field: field, Reason: "not a column the endpoint sorts by"}
		case len(matches) > 1:
			return nil, &PageRequestError{Field: field, Reason: "names more than one sortable column"}
		}

		order = append(order, OrderBy{column: matches[0], direction: directions[i]})
	}

	return order, nil
}

// PageRequestError reports a page request the client got wrong, so an HTTP
// handler answers it with 400: a negative page or size, a page past
// MaxPageNumber, an order_by / order pair it cannot sort by (a field the endpoint
// does not allow or that names more than one column, a direction other than
// asc/desc, lists of different lengths), or a keyset cursor that is malformed or
// was made for another order.
type PageRequestError struct {
	// Field is the offending parameter or sort field: "page", "size", "after",
	// or the sort field or direction; empty when order_by and order do not match.
	Field string
	// Reason says what is wrong with it.
	Reason string
}

func (e *PageRequestError) Error() string {
	if e.Field == "" {
		return "invalid page request: " + e.Reason
	}

	return fmt.Sprintf("invalid page request %q: %s", e.Field, e.Reason)
}

// validate rejects what no page can mean; out-of-range sizes are capped by Page.
func (r *PageRequest) validate() error {
	if r.Page < 0 {
		return &PageRequestError{Field: "page", Reason: fmt.Sprintf("must not be negative, got %d", r.Page)}
	}

	// Offset is Size*(Page-1) and has to stay well inside int on 32-bit builds.
	if r.Page > MaxPageNumber {
		return &PageRequestError{Field: "page", Reason: fmt.Sprintf("must be at most %d, got %d", MaxPageNumber, r.Page)}
	}

	if r.Size < 0 {
		return &PageRequestError{Field: "size", Reason: fmt.Sprintf("must not be negative, got %d", r.Size)}
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
