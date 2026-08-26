package tsq

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

// defaultPageSize is the default number of rows returned per page.
const defaultPageSize = 20

// MaxPageNumber caps PageRequest.Page. Offset multiplies Size by Page-1, so with Size
// capped at DefaultMaxPageSize the largest offset stays inside int on 32-bit builds.
const MaxPageNumber = 1000000

// PageRequest captures a page request, sort instructions, and optional keyword search.
type PageRequest struct {
	Size    int    `json:"size"     query:"size"`     // Size is the requested page size.
	Page    int    `json:"page"     query:"page"`     // Page is the 1-based page number.
	OrderBy string `json:"order_by" query:"order_by"` // OrderBy lists sortable field names separated by commas.
	Order   string `json:"order"    query:"order"`    // Order lists sort directions aligned with OrderBy.
	Keyword string `json:"keyword"  query:"keyword"`  // Keyword carries the optional free-text search term.
}

// NewPageRequest creates *PageRequest from query parameters(e.g. page=1&size=20&order_by=id&order=DESC).
func NewPageRequest(params url.Values) *PageRequest {
	page := &PageRequest{
		Page:    1,
		Size:    defaultPageSize,
		Order:   "",
		OrderBy: "",
		Keyword: "",
	}

	if params == nil {
		return page
	}

	if pageStr := params.Get("page"); pageStr != "" {
		if n, err := strconv.ParseInt(pageStr, 10, 64); err == nil && n > 0 {
			page.Page = int(n)
		}
	}

	if sizeStr := params.Get("size"); sizeStr != "" {
		if n, err := strconv.ParseInt(sizeStr, 10, 64); err == nil && n > 0 {
			page.Size = min(int(n), DefaultMaxPageSize)
		}
	}

	if page.Page > MaxPageNumber {
		page.Page = MaxPageNumber
	}

	page.OrderBy = params.Get("order_by")
	if page.OrderBy == "" {
		page.OrderBy = params.Get("sort")
	}

	page.Order = params.Get("order")

	page.Keyword = params.Get("keyword")

	return page
}

// ToQuery serializes the request back into URL query parameters.
func (r *PageRequest) ToQuery() url.Values {
	r = normalizePageReq(r)

	v := url.Values{}
	v.Set("size", strconv.Itoa(r.Size))
	v.Set("page", strconv.Itoa(r.Page))

	if r.OrderBy != "" {
		v.Set("order_by", r.OrderBy)
	}

	if r.Order != "" {
		v.Set("order", r.Order)
	}

	if r.Keyword != "" {
		v.Set("keyword", r.Keyword)
	}

	return v
}

// Offset calculates the offset for the SQL LIMIT clause.
//
// Page is clamped to MaxPageNumber first. Callers who need an out-of-range page to be
// rejected rather than clamped must call Validate before Offset: Offset alone cannot
// report an error, and silently answering with page one would be worse than clamping.
func (r *PageRequest) Offset() int {
	r = normalizePageReq(r)

	return r.Size * (r.Page - 1)
}

// Normalize applies default page values and clamps the requested page size to
// DefaultMaxPageSize.
//
// DefaultMaxPageSize is the absolute ceiling, not necessarily the effective one: a
// runtime built with RuntimeOptions.MaxPageSize clamps further when the query runs.
// Use NormalizeWithLimit to apply that runtime's limit here instead.
//
// The error return is always nil. It is kept so that callers can treat Normalize like
// Validate in a chain, and because dropping it would break every existing caller.
func (r *PageRequest) Normalize() error {
	return r.NormalizeWithLimit(DefaultMaxPageSize)
}

// NormalizeWithLimit is Normalize with an explicit page-size ceiling, so an HTTP
// handler can apply the same limit its runtime will apply. A maxSize of zero or less
// means DefaultMaxPageSize. The error return is always nil.
func (r *PageRequest) NormalizeWithLimit(maxSize int) error {
	if r == nil {
		return nil
	}

	if maxSize <= 0 {
		maxSize = DefaultMaxPageSize
	}

	if r.Page <= 0 {
		r.Page = 1
	}

	if r.Page > MaxPageNumber {
		r.Page = MaxPageNumber
	}

	if r.Size <= 0 {
		r.Size = defaultPageSize
	}

	if r.Size > maxSize {
		r.Size = maxSize
	}

	return nil
}

// Validate reports invalid paging or sorting input without mutating r.
//
// It checks Size against DefaultMaxPageSize, the absolute ceiling. A runtime built
// with RuntimeOptions.MaxPageSize clamps further at execution time, so a request that
// passes Validate may still come back with fewer rows than it asked for; use
// ValidateWithLimit to check against that runtime's limit instead.
func (r *PageRequest) Validate() error {
	return r.ValidateWithLimit(DefaultMaxPageSize)
}

// ValidateWithLimit is Validate with an explicit page-size ceiling. A maxSize of zero
// or less means DefaultMaxPageSize; a maxSize above DefaultMaxPageSize is capped to it,
// because no runtime raises the absolute ceiling.
func (r *PageRequest) ValidateWithLimit(maxSize int) error {
	if r == nil {
		return nil
	}

	if maxSize <= 0 || maxSize > DefaultMaxPageSize {
		maxSize = DefaultMaxPageSize
	}

	if r.Page <= 0 {
		return fmt.Errorf("page must be greater than 0, got %d", r.Page)
	}

	// Offset is Size*(Page-1) and has to stay well inside int on 32-bit builds, so an
	// out-of-range page is rejected here rather than silently clamped by Offset.
	if r.Page > MaxPageNumber {
		return fmt.Errorf("page must be less than or equal to %d, got %d", MaxPageNumber, r.Page)
	}

	if r.Size <= 0 {
		return fmt.Errorf("size must be greater than 0, got %d", r.Size)
	}

	if r.Size > maxSize {
		return fmt.Errorf("size must be less than or equal to %d, got %d", maxSize, r.Size)
	}

	if len(splitCommaValues(r.OrderBy)) == 0 && len(splitCommaValues(r.Order)) > 0 {
		return errors.New("order requires order_by")
	}

	for _, rawOrder := range splitCommaValues(r.Order) {
		if _, err := parseOrder(rawOrder); err != nil {
			return err
		}
	}

	return nil
}

// PageResponse wraps paginated data with request and count metadata.
type PageResponse[T any] struct {
	PageRequest

	Total     int64 `json:"total"`      // Total is the full number of matching rows.
	TotalPage int64 `json:"total_page"` // TotalPage is the number of available pages after rounding up.
	Data      []*T  `json:"data"`       // Data contains the rows for the current page.
}

// Response creates a typed page response from the request, total count, and data.
func (r *PageRequest) Response[T any](total int64, data []*T) *PageResponse[T] {
	r = normalizePageReq(r)

	resp := &PageResponse[T]{
		PageRequest: *r,
		Total:       total,
		Data:        data,
	}

	if r.Size > 0 {
		resp.TotalPage = total / int64(r.Size)
		if total%int64(r.Size) != 0 {
			resp.TotalPage++
		}
	}

	return resp
}

// NewPageResponse creates a PageResponse from the request, total count, and data.
// Deprecated: use PageRequest.Response.
func NewPageResponse[T any](r *PageRequest, total int64, data []*T) *PageResponse[T] {
	return r.Response(total, data)
}

// HasNext reports whether another page exists after the current one.
func (r *PageResponse[T]) HasNext() bool {
	if r == nil {
		return false
	}

	return r.Page < int(r.TotalPage)
}

// HasPrev reports whether a page exists before the current one.
func (r *PageResponse[T]) HasPrev() bool {
	if r == nil {
		return false
	}

	return r.Page > 1
}

// IsEmpty reports whether the current page contains any rows.
func (r *PageResponse[T]) IsEmpty() bool {
	if r == nil {
		return true
	}

	return len(r.Data) == 0
}
