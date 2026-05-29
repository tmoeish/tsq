package cmd

import "testing"

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

	parts := splitDDLTagParts(`price,size:32,type:DECIMAL(10,2)`)
	want := []string{"price", "size:32", "type:DECIMAL(10,2)"}
	if len(parts) != len(want) {
		t.Fatalf("splitDDLTagParts() len = %d, want %d (%v)", len(parts), len(want), parts)
	}

	for i := range want {
		if parts[i] != want[i] {
			t.Fatalf("splitDDLTagParts()[%d] = %q, want %q", i, parts[i], want[i])
		}
	}
}
