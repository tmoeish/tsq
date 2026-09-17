package tsq

import (
	"strings"
	"testing"
)

func TestPageReq_NilHelpers(t *testing.T) {
	var page *PageRequest

	page.Normalize(0)

	if paging, err := page.Paging(); err != nil || paging.Offset() != 0 {
		t.Fatalf("nil request = %+v, %v", paging, err)
	}
}

func TestPageReq_HelpersNormalizeInvalidValues(t *testing.T) {
	page := &PageRequest{
		Page: -2,
		Size: 0,
	}

	page.Normalize(0)

	if page.Page != 1 {
		t.Fatalf("expected normalized page 1, got %d", page.Page)
	}

	if page.Size != defaultPageSize {
		t.Fatalf("expected normalized size %d, got %d", defaultPageSize, page.Size)
	}

	if offset := (Paging{Page: page.Page, Size: page.Size}).Offset(); offset != 0 {
		t.Fatalf("expected normalized offset 0, got %d", offset)
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

func TestPageReq_Validate(t *testing.T) {
	tests := []struct {
		name         string
		input        *PageRequest
		expectedPage int
		expectedSize int
	}{
		{
			name: "valid values",
			input: &PageRequest{
				Page: 2,
				Size: 50,
			},
			expectedPage: 2,
			expectedSize: 50,
		},
		{
			name: "invalid page",
			input: &PageRequest{
				Page: 0,
				Size: 50,
			},
			expectedPage: 1,
			expectedSize: 50,
		},
		{
			name: "invalid size",
			input: &PageRequest{
				Page: 2,
				Size: 0,
			},
			expectedPage: 2,
			expectedSize: defaultPageSize,
		},
		{
			name: "size too large",
			input: &PageRequest{
				Page: 2,
				Size: DefaultMaxPageSize + 100,
			},
			expectedPage: 2,
			expectedSize: DefaultMaxPageSize,
		},
		{
			name: "negative values",
			input: &PageRequest{
				Page: -1,
				Size: -10,
			},
			expectedPage: 1,
			expectedSize: defaultPageSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.input.Normalize(0)

			if tt.input.Page != tt.expectedPage {
				t.Errorf("Expected page %d, got %d", tt.expectedPage, tt.input.Page)
			}

			if tt.input.Size != tt.expectedSize {
				t.Errorf("Expected size %d, got %d", tt.expectedSize, tt.input.Size)
			}
		})
	}
}

func TestPageReq_ValidateStrict(t *testing.T) {
	tests := []struct {
		name      string
		input     *PageRequest
		wantError bool
	}{
		{
			name: "valid values",
			input: &PageRequest{
				Page: 2,
				Size: 50,
			},
		},
		{
			name: "invalid order token",
			input: &PageRequest{
				Page:    1,
				Size:    20,
				OrderBy: "name",
				Order:   "sideways",
			},
			wantError: true,
		},
		{
			name: "order without field",
			input: &PageRequest{
				Page:  1,
				Size:  20,
				Order: "ASC",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.Validate(0)
			if tt.wantError && err == nil {
				t.Fatal("expected validation error")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("expected no validation error, got %v", err)
			}
		})
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

// TestPageReq_ValidateRejectsOutOfRangePage covers the bound that Offset used to
// swallow. Offset returned 0 for any page past its internal ceiling, so a request for
// page 2000000 silently came back with the first page of rows -- a wrong answer
// dressed up as a correct one. Validate now refuses the request instead.
func TestPageReq_ValidateRejectsOutOfRangePage(t *testing.T) {
	page := &PageRequest{Page: MaxPageNumber + 1, Size: 20}

	err := page.Validate(0)
	if err == nil {
		t.Fatalf("expected page %d to be rejected", page.Page)
	}

	if !strings.Contains(err.Error(), "page must be less than or equal to") {
		t.Fatalf("unexpected error for out-of-range page: %v", err)
	}

	if err := (&PageRequest{Page: MaxPageNumber, Size: 20}).Validate(0); err != nil {
		t.Fatalf("page %d is the last valid page, got %v", MaxPageNumber, err)
	}
}

// TestPageReq_OffsetClampsOutOfRangePage documents what Offset does with a page that
// Validate would have rejected: it clamps to the last representable page rather than
// wrapping around to the first one.
func TestPageReq_OffsetClampsOutOfRangePage(t *testing.T) {
	page := Paging{Page: MaxPageNumber * 10, Size: 20}

	want := 20 * (MaxPageNumber - 1)
	if got := page.Offset(); got != want {
		t.Fatalf("Offset() = %d, want %d", got, want)
	}
}

// TestPageReq_WithLimitAppliesRuntimeCeiling covers the gap between the absolute
// ceiling and a runtime's own. WithMaxPageSize used to apply only inside
// query execution, so a handler calling Validate approved a size the runtime then
// silently clamped. The *WithLimit variants let both sides check the same number.
func TestPageReq_WithLimitAppliesRuntimeCeiling(t *testing.T) {
	if err := (&PageRequest{Page: 1, Size: 500}).Validate(0); err != nil {
		t.Fatalf("500 is under the absolute ceiling, got %v", err)
	}

	err := (&PageRequest{Page: 1, Size: 500}).Validate(50)
	if err == nil {
		t.Fatal("expected size 500 to be rejected against a limit of 50")
	}

	page := &PageRequest{Page: 1, Size: 500}
	page.Normalize(50)

	if page.Size != 50 {
		t.Fatalf("Normalize(50) left Size = %d, want 50", page.Size)
	}

	// A runtime sets its own cap in either direction: DefaultMaxPageSize is the
	// default, and a limit passed explicitly is the one that applies.
	if err := (&PageRequest{Page: 1, Size: DefaultMaxPageSize + 1}).Validate(DefaultMaxPageSize * 10); err != nil {
		t.Fatalf("expected an explicitly raised limit to be honored, got %v", err)
	}
}

// TestValidateAndNormalizeResolveTheSameLimit is the invariant the two used to
// break: Validate clamped the supplied limit to DefaultMaxPageSize while
// Normalize used it as given, so one request could pass validation and be
// silently shrunk, or fail validation at a size normalization would have kept.
func TestValidateAndNormalizeResolveTheSameLimit(t *testing.T) {
	limits := []int{0, -1, 50, DefaultMaxPageSize, DefaultMaxPageSize * 10}
	sizes := []int{1, 50, DefaultMaxPageSize, DefaultMaxPageSize * 2}

	for _, limit := range limits {
		for _, size := range sizes {
			rejected := (&PageRequest{Page: 1, Size: size}).Validate(limit) != nil

			page := &PageRequest{Page: 1, Size: size}
			page.Normalize(limit)

			clamped := page.Size != size
			if rejected != clamped {
				t.Fatalf("limit %d size %d: Validate rejected=%v but Normalize clamped=%v",
					limit, size, rejected, clamped)
			}
		}
	}
}
