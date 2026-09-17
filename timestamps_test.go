package tsq

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	null "gopkg.in/nullbio/null.v6"
)

// TestManagedTimestampKinds covers every field type the generator accepts for
// created_at, updated_at and deleted_at. Stamping them used to be generated code, one
// template branch per kind; it lives here now, so this is where a missing kind fails.
func TestManagedTimestampKinds(t *testing.T) {
	now := time.Date(2026, 9, 17, 1, 2, 3, 0, time.UTC)

	fields := []any{new(time.Time), new(*time.Time), new(sql.NullTime), new(null.Time)}

	for _, ptr := range fields {
		v := reflect.ValueOf(ptr).Elem()

		t.Run(v.Type().String(), func(t *testing.T) {
			if !isUnset(v) {
				t.Fatal("a zero value must read as unset")
			}

			if err := applyTimestamp(v, now); err != nil {
				t.Fatalf("applyTimestamp() error = %v", err)
			}

			if isUnset(v) {
				t.Fatal("a stamped value must read as set")
			}

			if err := applyTombstone(v, now); err != nil {
				t.Fatalf("applyTombstone() error = %v", err)
			}
		})
	}

	var tombstone int64
	if err := applyTombstone(reflect.ValueOf(&tombstone).Elem(), now); err != nil || tombstone != now.UnixNano() {
		t.Fatalf("integer tombstone = %d, %v", tombstone, err)
	}

	var bad string
	if err := applyTimestamp(reflect.ValueOf(&bad).Elem(), now); err == nil {
		t.Fatal("expected an unsupported type to be refused")
	}
}
