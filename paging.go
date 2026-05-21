package tsq

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

const (
	// defaultPageSize is the default number of rows returned per page.
	defaultPageSize = 20
	// maxPageSize is the largest page size accepted by PageRequest.
	maxPageSize = 1000
)

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
			page.Size = min(int(n), maxPageSize)
		}
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

// Offset calculates the offset value for SQL LIMIT clause.
// Returns 0 if calculation would overflow; guaranteed safe for use in SQL.
func (r *PageRequest) Offset() int {
	r = normalizePageReq(r)

	const maxSafe = 1000000
	if r.Page > maxSafe || r.Size > maxSafe {
		return 0
	}

	return r.Size * (r.Page - 1)
}

// Normalize applies default page values and clamps the requested page size.
func (r *PageRequest) Normalize() error {
	if r == nil {
		return nil
	}

	if r.Page <= 0 {
		r.Page = 1
	}

	if r.Size <= 0 {
		r.Size = defaultPageSize
	}

	if r.Size > maxPageSize {
		r.Size = maxPageSize
	}

	return nil
}

// Validate reports invalid paging or sorting input without mutating r.
func (r *PageRequest) Validate() error {
	if r == nil {
		return nil
	}

	if r.Page <= 0 {
		return fmt.Errorf("page must be greater than 0, got %d", r.Page)
	}

	if r.Size <= 0 {
		return fmt.Errorf("size must be greater than 0, got %d", r.Size)
	}

	if r.Size > maxPageSize {
		return fmt.Errorf("size must be less than or equal to %d, got %d", maxPageSize, r.Size)
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

// NewPageResponse creates a PageResponse from the request, total count, and data.
func NewPageResponse[T any](r *PageRequest, total int64, data []*T) *PageResponse[T] {
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
