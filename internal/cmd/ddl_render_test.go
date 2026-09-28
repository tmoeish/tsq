package cmd

import (
	"slices"
	"testing"
)

func TestParseDDLTagOptionsSupportsExplicitTypes(t *testing.T) {
	t.Parallel()

	opts := parseDDLTagOptions(`amount,size:32,type:DECIMAL(10,2)`)
	if opts.size != 32 {
		t.Fatalf("parseDDLTagOptions() size = %d, want 32", opts.size)
	}
	if opts.rawType != "DECIMAL(10,2)" {
		t.Fatalf("parseDDLTagOptions() rawType = %q, want %q", opts.rawType, "DECIMAL(10,2)")
	}
}

func TestSplitDDLTagPartsKeepsTypeCommas(t *testing.T) {
	t.Parallel()

	for tag, want := range map[string][]string{
		`price,size:32,type:DECIMAL(10,2)`: {"price", "size:32", "type:DECIMAL(10,2)"},
		// A string default used to be cut at its comma.
		`tags,default:'a, b'`:             {"tags", "default:'a, b'"},
		`note,default:'it''s, (x',size:9`: {"note", "default:'it''s, (x'", "size:9"},
	} {
		if parts := splitDDLTagParts(tag); !slices.Equal(parts, want) {
			t.Errorf("splitDDLTagParts(%q) = %q, want %q", tag, parts, want)
		}
	}
}
