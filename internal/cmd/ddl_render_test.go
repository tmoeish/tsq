package cmd

import (
	"slices"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
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

// TestMySQLIndexWarningGivesAFixThatWorks covers the advice for an indexed []byte,
// which is a BLOB on MySQL whatever its size: and was told to set a size:.
func TestMySQLIndexWarningGivesAFixThatWorks(t *testing.T) {
	types := map[string]tsqdialect.ColumnType{
		"h":    {Kind: tsqdialect.ColumnKindBytes, Size: 16},
		"body": {Kind: tsqdialect.ColumnKindString, Size: 100000},
	}
	typeOf := func(c string) (tsqdialect.ColumnType, bool) { t, ok := types[c]; return t, ok }

	if got := mysqlIndexProblem("ux_h", []string{"h"}, typeOf); !strings.Contains(got, "type:VARBINARY(n)") || strings.Contains(got, "size:") {
		t.Fatalf("[]byte warning = %q; want type:VARBINARY(n)", got)
	}

	if got := mysqlIndexProblem("ix_body", []string{"body"}, typeOf); !strings.Contains(got, "give it a size:") {
		t.Fatalf("TEXT warning = %q", got)
	}
}
