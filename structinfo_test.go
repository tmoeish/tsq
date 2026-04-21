package tsq

import "testing"

func TestStructInfo_SetTSQVersionHandlesNilReceiver(t *testing.T) {
	var info *StructInfo

	info.SetTSQVersion("v1.2.3")
}

func TestStructInfo_SetTSQVersion(t *testing.T) {
	info := &StructInfo{}

	info.SetTSQVersion("v1.2.3")

	if info.TSQVersion != "v1.2.3" {
		t.Fatalf("expected TSQ version to be updated, got %q", info.TSQVersion)
	}
}
