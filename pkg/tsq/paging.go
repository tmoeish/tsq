package tsq

import (
	"net/url"
	"strconv"
)

// Direction represents the sort direction
// swagger:strfmt Direction
// enum:ASC,DESC
type Direction string

// sort directions
const (
	Asc  Direction = "ASC"
	Desc Direction = "DESC"
)

const (
	DefaultPageSize = 20
)

// NewPageReq creates *PageReq from query parameters(e.g. page=1&page=20&sort_by=id&order=DESC).
func NewPageReq(params url.Values) *PageReq {
	page := &PageReq{
		Page:    1,
		Size:    DefaultPageSize,
		Order:   "DESC",
		OrderBy: "id",
		Keyword: "",
	}
	if params == nil {
		return page
	}

	n, err := strconv.ParseInt(params.Get("page"), 10, 64)
	if err == nil {
		page.Page = int(n)
	}
	n, err = strconv.ParseInt(params.Get("size"), 10, 64)
	if err == nil {
		page.Size = int(n)
	}
	if page.Size == 0 {
		page.Size = DefaultPageSize
	}
	page.OrderBy = params.Get("order_by")
	if len(page.OrderBy) == 0 {
		page.OrderBy = params.Get("sort")
	}
	if len(page.OrderBy) == 0 {
		page.OrderBy = "ID"
	}
	page.Order = params.Get("order")
	if len(page.Order) == 0 {
		page.Order = "DESC"
	}
	page.Keyword = params.Get("keyword")

	return page
}

// PageReq is a struct for paging request
type PageReq struct {
	Size    int    `query:"size" json:"size"`         // page size
	Page    int    `query:"page" json:"page"`         // start from 1
	OrderBy string `query:"order_by" json:"order_by"` // field name
	Order   string `query:"order" json:"order"`       // asc | desc
	Keyword string `query:"keyword" json:"keyword"`   // search keyword
}

// ToQuery convert PageReq to url.Values
func (r *PageReq) ToQuery() url.Values {
	v := url.Values{}
	v.Set("size", strconv.Itoa(r.Size))
	v.Set("page", strconv.Itoa(r.Page))
	v.Set("order_by", r.OrderBy)
	v.Set("order", r.Order)
	v.Set("keyword", r.Keyword)

	return v
}

// Offset return offset value
func (r *PageReq) Offset() int {
	return r.Size * (r.Page - 1)
}

// NewPageReq create a new PageReq
func NewResponse[T Table](r *PageReq, total int64, data []*T) *PageResp[T] {
	resp := &PageResp[T]{
		PageReq: *r,
	}

	resp.Total = total
	resp.TotalPage = total / int64(r.Size)
	if total%int64(r.Size) != 0 {
		resp.TotalPage++
	}
	resp.Data = data

	return resp
}

// PageResp is a struct for paging response
type PageResp[T Table] struct {
	PageReq

	Total     int64 `json:"total"`      // total record
	TotalPage int64 `json:"total_page"` // total page
	Data      []*T  `json:"data"`       // data
}
