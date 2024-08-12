package tsq

import (
	"net/url"
	"strconv"
)

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
