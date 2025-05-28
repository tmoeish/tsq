package tsq

import (
	"net/url"
	"strconv"
	"testing"
)

func TestDirection_Constants(t *testing.T) {
	tests := []struct {
		direction Direction
		expected  string
	}{
		{Asc, "ASC"},
		{Desc, "DESC"},
	}

	for _, tt := range tests {
		t.Run(string(tt.direction), func(t *testing.T) {
			if string(tt.direction) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(tt.direction))
			}
		})
	}
}

func TestNewPageReq_EmptyParams(t *testing.T) {
	page := NewPageReq(nil)

	if page.Page != 1 {
		t.Errorf("Expected default page 1, got %d", page.Page)
	}

	if page.Size != DefaultPageSize {
		t.Errorf("Expected default size %d, got %d", DefaultPageSize, page.Size)
	}

	if page.Order != "" {
		t.Errorf("Expected empty order, got '%s'", page.Order)
	}

	if page.OrderBy != "" {
		t.Errorf("Expected empty order_by, got '%s'", page.OrderBy)
	}

	if page.Keyword != "" {
		t.Errorf("Expected empty keyword, got '%s'", page.Keyword)
	}
}

func TestNewPageReq_WithParams(t *testing.T) {
	params := url.Values{}
	params.Set("page", "2")
	params.Set("size", "50")
	params.Set("order_by", "name,age")
	params.Set("order", "ASC,DESC")
	params.Set("keyword", "test")

	page := NewPageReq(params)

	if page.Page != 2 {
		t.Errorf("Expected page 2, got %d", page.Page)
	}

	if page.Size != 50 {
		t.Errorf("Expected size 50, got %d", page.Size)
	}

	if page.OrderBy != "name,age" {
		t.Errorf("Expected order_by 'name,age', got '%s'", page.OrderBy)
	}

	if page.Order != "ASC,DESC" {
		t.Errorf("Expected order 'ASC,DESC', got '%s'", page.Order)
	}

	if page.Keyword != "test" {
		t.Errorf("Expected keyword 'test', got '%s'", page.Keyword)
	}
}

func TestNewPageReq_InvalidPage(t *testing.T) {
	tests := []struct {
		name     string
		pageStr  string
		expected int
	}{
		{"negative page", "-1", 1},
		{"zero page", "0", 1},
		{"invalid string", "abc", 1},
		{"empty string", "", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := url.Values{}
			params.Set("page", tt.pageStr)

			page := NewPageReq(params)

			if page.Page != tt.expected {
				t.Errorf("Expected page %d, got %d", tt.expected, page.Page)
			}
		})
	}
}

func TestNewPageReq_InvalidSize(t *testing.T) {
	tests := []struct {
		name     string
		sizeStr  string
		expected int
	}{
		{"negative size", "-1", DefaultPageSize},
		{"zero size", "0", DefaultPageSize},
		{"invalid string", "abc", DefaultPageSize},
		{"empty string", "", DefaultPageSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := url.Values{}
			params.Set("size", tt.sizeStr)

			page := NewPageReq(params)

			if page.Size != tt.expected {
				t.Errorf("Expected size %d, got %d", tt.expected, page.Size)
			}
		})
	}
}

func TestNewPageReq_MaxSize(t *testing.T) {
	params := url.Values{}
	params.Set("size", strconv.Itoa(MaxPageSize+100))

	page := NewPageReq(params)

	if page.Size != MaxPageSize {
		t.Errorf("Expected size to be capped at %d, got %d", MaxPageSize, page.Size)
	}
}

func TestNewPageReq_AlternativeSortParam(t *testing.T) {
	params := url.Values{}
	params.Set("sort", "name")

	page := NewPageReq(params)

	if page.OrderBy != "name" {
		t.Errorf("Expected order_by 'name' from 'sort' param, got '%s'", page.OrderBy)
	}
}

func TestNewPageReq_OrderByPriority(t *testing.T) {
	// order_by should take priority over sort
	params := url.Values{}
	params.Set("order_by", "name")
	params.Set("sort", "age")

	page := NewPageReq(params)

	if page.OrderBy != "name" {
		t.Errorf("Expected order_by 'name' to take priority, got '%s'", page.OrderBy)
	}
}

func TestPageReq_ToQuery(t *testing.T) {
	page := &PageReq{
		Page:    2,
		Size:    50,
		OrderBy: "name,age",
		Order:   "ASC,DESC",
		Keyword: "test",
	}

	query := page.ToQuery()

	if query.Get("page") != "2" {
		t.Errorf("Expected page '2', got '%s'", query.Get("page"))
	}

	if query.Get("size") != "50" {
		t.Errorf("Expected size '50', got '%s'", query.Get("size"))
	}

	if query.Get("order_by") != "name,age" {
		t.Errorf("Expected order_by 'name,age', got '%s'", query.Get("order_by"))
	}

	if query.Get("order") != "ASC,DESC" {
		t.Errorf("Expected order 'ASC,DESC', got '%s'", query.Get("order"))
	}

	if query.Get("keyword") != "test" {
		t.Errorf("Expected keyword 'test', got '%s'", query.Get("keyword"))
	}
}

func TestPageReq_ToQuery_EmptyValues(t *testing.T) {
	page := &PageReq{
		Page: 1,
		Size: DefaultPageSize,
	}

	query := page.ToQuery()

	if query.Get("page") != "1" {
		t.Errorf("Expected page '1', got '%s'", query.Get("page"))
	}

	if query.Get("size") != strconv.Itoa(DefaultPageSize) {
		t.Errorf("Expected size '%d', got '%s'", DefaultPageSize, query.Get("size"))
	}

	// Empty values should not be set
	if query.Get("order_by") != "" {
		t.Errorf("Expected empty order_by, got '%s'", query.Get("order_by"))
	}

	if query.Get("order") != "" {
		t.Errorf("Expected empty order, got '%s'", query.Get("order"))
	}

	if query.Get("keyword") != "" {
		t.Errorf("Expected empty keyword, got '%s'", query.Get("keyword"))
	}
}

func TestPageReq_Offset(t *testing.T) {
	tests := []struct {
		name     string
		page     int
		size     int
		expected int
	}{
		{"first page", 1, 20, 0},
		{"second page", 2, 20, 20},
		{"third page", 3, 20, 40},
		{"large page", 10, 50, 450},
		{"size 1", 5, 1, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := &PageReq{
				Page: tt.page,
				Size: tt.size,
			}

			offset := page.Offset()
			if offset != tt.expected {
				t.Errorf("Expected offset %d, got %d", tt.expected, offset)
			}
		})
	}
}

func TestPageReq_Validate(t *testing.T) {
	tests := []struct {
		name         string
		input        *PageReq
		expectedPage int
		expectedSize int
	}{
		{
			name: "valid values",
			input: &PageReq{
				Page: 2,
				Size: 50,
			},
			expectedPage: 2,
			expectedSize: 50,
		},
		{
			name: "invalid page",
			input: &PageReq{
				Page: 0,
				Size: 50,
			},
			expectedPage: 1,
			expectedSize: 50,
		},
		{
			name: "invalid size",
			input: &PageReq{
				Page: 2,
				Size: 0,
			},
			expectedPage: 2,
			expectedSize: DefaultPageSize,
		},
		{
			name: "size too large",
			input: &PageReq{
				Page: 2,
				Size: MaxPageSize + 100,
			},
			expectedPage: 2,
			expectedSize: MaxPageSize,
		},
		{
			name: "negative values",
			input: &PageReq{
				Page: -1,
				Size: -10,
			},
			expectedPage: 1,
			expectedSize: DefaultPageSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.Validate()
			if err != nil {
				t.Errorf("Validate() should not return error, got %v", err)
			}

			if tt.input.Page != tt.expectedPage {
				t.Errorf("Expected page %d, got %d", tt.expectedPage, tt.input.Page)
			}

			if tt.input.Size != tt.expectedSize {
				t.Errorf("Expected size %d, got %d", tt.expectedSize, tt.input.Size)
			}
		})
	}
}

func TestNewResponse(t *testing.T) {
	req := &PageReq{
		Page: 2,
		Size: 10,
	}

	data := []*string{
		stringPtr("item1"),
		stringPtr("item2"),
		stringPtr("item3"),
	}

	resp := NewResponse(req, 25, data)

	if resp.Page != 2 {
		t.Errorf("Expected page 2, got %d", resp.Page)
	}

	if resp.Size != 10 {
		t.Errorf("Expected size 10, got %d", resp.Size)
	}

	if resp.Total != 25 {
		t.Errorf("Expected total 25, got %d", resp.Total)
	}

	expectedTotalPage := int64(3) // 25 / 10 = 2.5, rounded up to 3
	if resp.TotalPage != expectedTotalPage {
		t.Errorf("Expected total page %d, got %d", expectedTotalPage, resp.TotalPage)
	}

	if len(resp.Data) != 3 {
		t.Errorf("Expected 3 data items, got %d", len(resp.Data))
	}
}

func TestNewResponse_ExactDivision(t *testing.T) {
	req := &PageReq{
		Page: 1,
		Size: 10,
	}

	resp := NewResponse(req, 20, []*string{})

	expectedTotalPage := int64(2) // 20 / 10 = 2
	if resp.TotalPage != expectedTotalPage {
		t.Errorf("Expected total page %d, got %d", expectedTotalPage, resp.TotalPage)
	}
}

func TestNewResponse_ZeroSize(t *testing.T) {
	req := &PageReq{
		Page: 1,
		Size: 0,
	}

	resp := NewResponse(req, 20, []*string{})

	if resp.TotalPage != 0 {
		t.Errorf("Expected total page 0 when size is 0, got %d", resp.TotalPage)
	}
}

func TestPageResp_HasNext(t *testing.T) {
	tests := []struct {
		name     string
		page     int
		total    int64
		expected bool
	}{
		{"has next", 1, 3, true},
		{"no next", 3, 3, false},
		{"last page", 2, 2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &PageResp[string]{
				PageReq: PageReq{
					Page: tt.page,
				},
				TotalPage: tt.total,
			}

			if resp.HasNext() != tt.expected {
				t.Errorf("Expected HasNext() %v, got %v", tt.expected, resp.HasNext())
			}
		})
	}
}

func TestPageResp_HasPrev(t *testing.T) {
	tests := []struct {
		name     string
		page     int
		expected bool
	}{
		{"has prev", 2, true},
		{"no prev", 1, false},
		{"third page", 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &PageResp[string]{
				PageReq: PageReq{
					Page: tt.page,
				},
			}

			if resp.HasPrev() != tt.expected {
				t.Errorf("Expected HasPrev() %v, got %v", tt.expected, resp.HasPrev())
			}
		})
	}
}

func TestPageResp_IsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		data     []*string
		expected bool
	}{
		{"empty", []*string{}, true},
		{"not empty", []*string{stringPtr("item")}, false},
		{"nil", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &PageResp[string]{
				Data: tt.data,
			}

			if resp.IsEmpty() != tt.expected {
				t.Errorf("Expected IsEmpty() %v, got %v", tt.expected, resp.IsEmpty())
			}
		})
	}
}

func TestPageReq_RoundTrip(t *testing.T) {
	// Test that ToQuery() and NewPageReq() are inverse operations
	original := &PageReq{
		Page:    3,
		Size:    25,
		OrderBy: "name,created_at",
		Order:   "ASC,DESC",
		Keyword: "search term",
	}

	query := original.ToQuery()
	reconstructed := NewPageReq(query)

	if reconstructed.Page != original.Page {
		t.Errorf("Page mismatch: expected %d, got %d", original.Page, reconstructed.Page)
	}

	if reconstructed.Size != original.Size {
		t.Errorf("Size mismatch: expected %d, got %d", original.Size, reconstructed.Size)
	}

	if reconstructed.OrderBy != original.OrderBy {
		t.Errorf("OrderBy mismatch: expected '%s', got '%s'", original.OrderBy, reconstructed.OrderBy)
	}

	if reconstructed.Order != original.Order {
		t.Errorf("Order mismatch: expected '%s', got '%s'", original.Order, reconstructed.Order)
	}

	if reconstructed.Keyword != original.Keyword {
		t.Errorf("Keyword mismatch: expected '%s', got '%s'", original.Keyword, reconstructed.Keyword)
	}
}

func TestConstants(t *testing.T) {
	if DefaultPageSize != 20 {
		t.Errorf("Expected DefaultPageSize 20, got %d", DefaultPageSize)
	}

	if MaxPageSize != 1000 {
		t.Errorf("Expected MaxPageSize 1000, got %d", MaxPageSize)
	}
}
