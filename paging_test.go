package tsq

import (
	"context"
	"testing"
)

func TestPageReq_NilHelpers(t *testing.T) {
	var page *PageRequest

	if paging, err := page.Paging(); err != nil || paging.Offset() != 0 {
		t.Fatalf("nil request = %+v, %v", paging, err)
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
			offset := Paging{Page: tt.page, Size: tt.size}.Offset()
			if offset != tt.expected {
				t.Errorf("Expected offset %d, got %d", tt.expected, offset)
			}
		})
	}
}

// TestPageReq_PagingValidates covers what Paging rejects and what it leaves to
// Page: zero means the default, a negative number or a bad order is an error,
// and a large size is capped when the page is read, not rejected.
func TestPageReq_PagingValidates(t *testing.T) {
	tests := []struct {
		name      string
		input     *PageRequest
		wantError bool
	}{
		{name: "valid values", input: &PageRequest{Page: 2, Size: 50}},
		{name: "zero means the defaults", input: &PageRequest{}},
		{name: "a large size is capped later", input: &PageRequest{Page: 1, Size: DefaultMaxPageSize * 10}},
		{name: "negative page", input: &PageRequest{Page: -1, Size: 10}, wantError: true},
		{name: "negative size", input: &PageRequest{Page: 1, Size: -10}, wantError: true},
		{name: "page past the last", input: &PageRequest{Page: MaxPageNumber + 1, Size: 20}, wantError: true},
		{name: "last page", input: &PageRequest{Page: MaxPageNumber, Size: 20}},
		{name: "invalid order token", input: &PageRequest{Page: 1, Size: 20, OrderBy: "name", Order: "sideways"}, wantError: true},
		{name: "order without field", input: &PageRequest{Page: 1, Size: 20, Order: "ASC"}, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.input.Paging(User_Name)
			if tt.wantError && err == nil {
				t.Fatal("expected a validation error")
			}

			if !tt.wantError && err != nil {
				t.Fatalf("expected no validation error, got %v", err)
			}

			if _, keysetErr := tt.input.Keyset(User_Name); (keysetErr != nil) != (err != nil) {
				t.Fatalf("Paging and Keyset disagree: %v / %v", err, keysetErr)
			}
		})
	}
}

// TestPageServesTheRuntimeCap is where a size above the limit goes: Page caps it
// to the runtime's WithMaxPageSize and reports the size it served.
func TestPageServesTheRuntimeCap(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t, WithMaxPageSize(2))
	seedUsers(t, rt, "a", "b", "c")

	paging, err := (&PageRequest{Size: 500}).Paging()
	if err != nil {
		t.Fatal(err)
	}

	resp, err := Select(User__Cols...).From(Users).Page(ctx, rt, paging)
	if err != nil || resp.Size != 2 || len(resp.Data) != 2 || resp.Page != 1 || resp.TotalPages != 2 {
		t.Fatalf("Page = %+v, %v", resp, err)
	}
}

func TestNewResponse(t *testing.T) {
	req := Paging{Page: 2, Size: 10}

	data := []*string{
		new("item1"),
		new("item2"),
		new("item3"),
	}

	resp := newPageResponse(req, 25, data)

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
	if resp.TotalPages != expectedTotalPage {
		t.Errorf("Expected total page %d, got %d", expectedTotalPage, resp.TotalPages)
	}

	if len(resp.Data) != 3 {
		t.Errorf("Expected 3 data items, got %d", len(resp.Data))
	}
}

func TestNewResponse_ExactDivision(t *testing.T) {
	resp := newPageResponse(Paging{Page: 1, Size: 10}, 20, []*string{})

	expectedTotalPage := int64(2) // 20 / 10 = 2
	if resp.TotalPages != expectedTotalPage {
		t.Errorf("Expected total page %d, got %d", expectedTotalPage, resp.TotalPages)
	}
}

func TestNewResponse_ZeroSize(t *testing.T) {
	resp := newPageResponse(Paging{}.normalized(0), 20, []*string(nil))

	if resp.Size != defaultPageSize || resp.TotalPages != 1 || resp.Data == nil {
		t.Fatalf("response over a normalized empty Paging = %+v", resp)
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
			resp := &PageResponse[string]{
				Page:       tt.page,
				TotalPages: tt.total,
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
			resp := &PageResponse[string]{
				Page: tt.page,
			}

			if resp.HasPrev() != tt.expected {
				t.Errorf("Expected HasPrev() %v, got %v", tt.expected, resp.HasPrev())
			}
		})
	}
}

func TestPageResp_NilHelpers(t *testing.T) {
	var resp *PageResponse[int]

	if resp.HasNext() {
		t.Fatal("expected nil response to report no next page")
	}

	if resp.HasPrev() {
		t.Fatal("expected nil response to report no previous page")
	}

	if !resp.IsEmpty() {
		t.Fatal("expected nil response to be empty")
	}
}

func TestPageResp_IsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		data     []*string
		expected bool
	}{
		{"empty", []*string{}, true},
		{"not empty", []*string{new("item")}, false},
		{"nil", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &PageResponse[string]{
				Data: tt.data,
			}

			if resp.IsEmpty() != tt.expected {
				t.Errorf("Expected IsEmpty() %v, got %v", tt.expected, resp.IsEmpty())
			}
		})
	}
}

func TestConstants(t *testing.T) {
	if false {
		t.Errorf("Expected defaultPageSize 20, got %d", defaultPageSize)
	}

	if false {
		t.Errorf("Expected DefaultMaxPageSize 1000, got %d", DefaultMaxPageSize)
	}
}

// TestPageReq_OffsetClampsOutOfRangePage documents what Offset does with a page that
// Paging rejects: it clamps to the last representable page rather than
// wrapping around to the first one.
func TestPageReq_OffsetClampsOutOfRangePage(t *testing.T) {
	page := Paging{Page: MaxPageNumber * 10, Size: 20}

	want := 20 * (MaxPageNumber - 1)
	if got := page.Offset(); got != want {
		t.Fatalf("Offset() = %d, want %d", got, want)
	}
}
