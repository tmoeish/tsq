package tsq

import (
	"errors"
	"fmt"
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

// Offset calculates the offset for the SQL LIMIT clause.
//
// Page is clamped to MaxPageNumber first. Callers who need an out-of-range page to be
// rejected rather than clamped must call Validate before Offset: Offset alone cannot
// report an error, and silently answering with page one would be worse than clamping.
func (r *PageRequest) Offset() int {
	r = normalizePageReq(r)

	return r.Size * (r.Page - 1)
}

// Normalize applies default page values and clamps Page to MaxPageNumber and Size to
// maxSize. A maxSize of zero or less means DefaultMaxPageSize, and a maxSize above
// DefaultMaxPageSize is capped to it. Pass the runtime's Runtime.MaxPageSize so that an
// HTTP handler applies the same limit the query will.
func (r *PageRequest) Normalize(maxSize int) {
	if r == nil {
		return
	}

	maxSize = boundPageSize(maxSize)

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
}

// Validate reports invalid paging or sorting input without mutating r. maxSize is
// resolved the same way Normalize resolves it.
func (r *PageRequest) Validate(maxSize int) error {
	if r == nil {
		return nil
	}

	maxSize = boundPageSize(maxSize)

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

	Total      int64 `json:"total"`       // Total is the full number of matching rows.
	TotalPages int64 `json:"total_pages"` // TotalPage is the number of available pages after rounding up.
	Data       []*T  `json:"data"`        // Data contains the rows for the current page.
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
		resp.TotalPages = total / int64(r.Size)
		if total%int64(r.Size) != 0 {
			resp.TotalPages++
		}
	}

	return resp
}

// HasNext reports whether another page exists after the current one.
func (r *PageResponse[T]) HasNext() bool {
	if r == nil {
		return false
	}

	return r.Page < int(r.TotalPages)
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

// boundPageSize resolves a caller-supplied page-size limit.
//
// DefaultMaxPageSize is the default, not a ceiling: a runtime built with
// WithMaxPageSize decides its own cap, in either direction. Treating the
// constant as an absolute maximum would make WithMaxPageSize(5000) silently do
// nothing, and the library would be overriding an explicit choice with a
// compile-time constant.
//
// Validate and Normalize must resolve the limit the same way. They did not:
// Validate clamped the limit to DefaultMaxPageSize and Normalize used it as
// given, so the same request could pass one and fail the other.
func boundPageSize(maxSize int) int {
	if maxSize <= 0 {
		return DefaultMaxPageSize
	}

	return maxSize
}
