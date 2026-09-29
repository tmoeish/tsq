package cmd

import (
	"slices"
	"testing"
)

func TestParseDDLTagOptionsSupportsExplicitTypes(t *testing.T) {
	t.Parallel()

	opts, err := parseDDLTagOptions(`amount,size:32,type:DECIMAL(10,2)`)
	if err != nil || opts.size != 32 {
		t.Fatalf("parseDDLTagOptions() size = %d, want 32", opts.size)
	}
	if opts.rawType != "DECIMAL(10,2)" {
		t.Fatalf("parseDDLTagOptions() rawType = %q, want %q", opts.rawType, "DECIMAL(10,2)")
	}
}

// TestParseDDLTagOptionsRefusesWhatItDoesNotKnow covers options that were
// ignored: a misspelled key, a size that is not a number, an option without a
// value. The column quietly got the type or default nobody asked for.
func TestParseDDLTagOptionsRefusesWhatItDoesNotKnow(t *testing.T) {
	t.Parallel()

	for _, tag := range []string{"name,sise:10", "name,size:ten", "name,size:0", "name,defualt:5", "name,type:", "name,,size:3"} {
		if _, err := parseDDLTagOptions(tag); err == nil {
			t.Errorf("parseDDLTagOptions(%q): want an error", tag)
		}
	}

	if _, err := parseDDLTagOptions("slug,generated"); err != nil {
		t.Errorf("generated without an expression: %v", err)
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
