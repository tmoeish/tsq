package tsq

import (
	"net/url"
	"strconv"

	"github.com/juju/errors"
)

// ================================================
// 分页常量和类型
// ================================================

// Direction is the paging-layer alias of Order.
type Direction = Order

const (
	Asc  Direction = ASC
	Desc Direction = DESC
)

const (
	DefaultPageSize = 20
	MaxPageSize     = 1000 // 防止过大的页面大小
)

// ================================================
// 分页请求结构体
// ================================================

// PageReq represents a pagination request with search and sorting capabilities
type PageReq struct {
	Size    int    `json:"size"     query:"size"`     // 页面大小，默认 20
	Page    int    `json:"page"     query:"page"`     // 页码，从 1 开始，默认 1
	OrderBy string `json:"order_by" query:"order_by"` // 排序字段，逗号分隔
	Order   string `json:"order"    query:"order"`    // 排序方向 [asc|desc]，逗号分隔
	Keyword string `json:"keyword"  query:"keyword"`  // 搜索关键词（可选）
}

// NewPageReq creates *PageReq from query parameters(e.g. page=1&size=20&order_by=id&order=DESC).
func NewPageReq(params url.Values) *PageReq {
	page := &PageReq{
		Page:    1,
		Size:    DefaultPageSize,
		Order:   "",
		OrderBy: "",
		Keyword: "",
	}

	if params == nil {
		return page
	}

	// Parse page number
	if pageStr := params.Get("page"); pageStr != "" {
		if n, err := strconv.ParseInt(pageStr, 10, 64); err == nil && n > 0 {
			page.Page = int(n)
		}
	}

	// Parse page size with limits
	if sizeStr := params.Get("size"); sizeStr != "" {
		if n, err := strconv.ParseInt(sizeStr, 10, 64); err == nil && n > 0 {
			page.Size = min(int(n), MaxPageSize)
		}
	}

	// Parse order by field
	page.OrderBy = params.Get("order_by")
	if page.OrderBy == "" {
		page.OrderBy = params.Get("sort") // Alternative parameter name
	}

	// Parse order direction
	page.Order = params.Get("order")

	// Parse search keyword
	page.Keyword = params.Get("keyword")

	return page
}

// ToQuery converts PageReq to url.Values for URL generation
func (r *PageReq) ToQuery() url.Values {
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

// Offset calculates the offset value for SQL LIMIT clause
func (r *PageReq) Offset() int {
	r = normalizePageReq(r)

	return r.Size * (r.Page - 1)
}

// Validate validates the pagination request parameters
func (r *PageReq) Validate() error {
	if r == nil {
		return nil
	}

	if r.Page <= 0 {
		r.Page = 1
	}

	if r.Size <= 0 {
		r.Size = DefaultPageSize
	}

	if r.Size > MaxPageSize {
		r.Size = MaxPageSize
	}

	return nil
}

// ValidateStrict reports invalid paging or sorting input without mutating r.
func (r *PageReq) ValidateStrict() error {
	if r == nil {
		return nil
	}

	if r.Page <= 0 {
		return errors.Errorf("page must be greater than 0, got %d", r.Page)
	}

	if r.Size <= 0 {
		return errors.Errorf("size must be greater than 0, got %d", r.Size)
	}

	if r.Size > MaxPageSize {
		return errors.Errorf("size must be less than or equal to %d, got %d", MaxPageSize, r.Size)
	}

	if len(splitCommaValues(r.OrderBy)) == 0 && len(splitCommaValues(r.Order)) > 0 {
		return errors.New("order requires order_by")
	}

	for _, rawOrder := range splitCommaValues(r.Order) {
		if _, err := parseOrder(rawOrder); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// ================================================
// 分页响应结构体
// ================================================

// PageResp represents a paginated response with metadata
type PageResp[T any] struct {
	PageReq

	Total     int64 `json:"total"`      // 总记录数
	TotalPage int64 `json:"total_page"` // 总页数
	Data      []*T  `json:"data"`       // 当前页数据
}

// NewResponse creates a new PageResp from request, total count, and data
func NewResponse[T any](r *PageReq, total int64, data []*T) *PageResp[T] {
	r = normalizePageReq(r)

	resp := &PageResp[T]{
		PageReq: *r,
		Total:   total,
		Data:    data,
	}

	// Calculate total pages
	if r.Size > 0 {
		resp.TotalPage = total / int64(r.Size)
		if total%int64(r.Size) != 0 {
			resp.TotalPage++
		}
	}

	return resp
}

// HasNext returns true if there are more pages available
func (r *PageResp[T]) HasNext() bool {
	if r == nil {
		return false
	}

	return r.Page < int(r.TotalPage)
}

// HasPrev returns true if there are previous pages available
func (r *PageResp[T]) HasPrev() bool {
	if r == nil {
		return false
	}

	return r.Page > 1
}

// IsEmpty returns true if the result set is empty
func (r *PageResp[T]) IsEmpty() bool {
	if r == nil {
		return true
	}

	return len(r.Data) == 0
}
