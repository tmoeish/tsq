package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

// scannerTime has the shape of gopkg.in/nullbio/null.v6's Time: a struct with its
// own Scan and Value rather than an embedded sql.NullTime. The real type is written
// by the integration tests through the examples; importing it here would put it in
// every TSQ user's go.sum.
type scannerTime struct {
	Time  time.Time
	Valid bool
}

func (t *scannerTime) Scan(value any) error {
	t.Time, t.Valid = value.(time.Time)
	return nil
}

func (t scannerTime) Value() (driver.Value, error) {
	if !t.Valid {
		return nil, nil
	}

	return t.Time, nil
}

// TestManagedTimestampKinds covers every field type the generator accepts for
// created_at, updated_at and deleted_at. Stamping them used to be generated code, one
// template branch per kind; it lives here now, so this is where a missing kind fails.
func TestManagedTimestampKinds(t *testing.T) {
	now := time.Date(2026, 9, 17, 1, 2, 3, 0, time.UTC)

	fields := []any{new(time.Time), new(*time.Time), new(sql.NullTime), new(scannerTime)}

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

// TestRefusedUpdatesLeaveUpdatedAtAlone covers the caller's row after an Update
// that did not happen. updated_at used to be written into it before anything was
// checked, so a version conflict left the row holding a time the database never
// stored, while its version stayed where it was.
func TestRefusedUpdatesLeaveUpdatedAtAlone(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	row := seedUsers(t, rt, "a")[0]

	stale := *row
	row.Name = "moved on"

	if err := Users.Update(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	stamp := stale.UpdatedAt
	stale.Name = "lost"

	if err := Users.Update(ctx, rt, &stale); !IsOptimisticLockError(err) {
		t.Fatalf("Update of a stale copy = %v; want an OptimisticLockError", err)
	}

	if !stale.UpdatedAt.Equal(stamp) {
		t.Fatalf("updated_at of the refused row = %v, want %v as loaded", stale.UpdatedAt, stamp)
	}
}
