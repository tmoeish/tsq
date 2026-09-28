package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
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

	// A pointer to the zero time is as unset as a nil one: Insert used to keep it
	// as the caller's created_at and store year 1.
	zero := &time.Time{}
	if !isUnset(reflect.ValueOf(&zero).Elem()) {
		t.Fatal("a pointer to the zero time must read as unset")
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

// stamped keeps its managed times behind pointers, the field shape where one
// value shared by several rows would be visible.
type stamped struct {
	ID        int64
	UpdatedAt *time.Time
	DeletedAt *time.Time
}

var (
	stampedHandle   = NewSoftDeleteTable[stamped, int64]("stamped")
	Stamped_ID      = NewColumn(stampedHandle.TableOf, "id", "id", func(r *stamped) *int64 { return &r.ID })
	Stamped_Updated = NewNullColumn[time.Time](stampedHandle.TableOf, "updated_at", "updated_at", func(r *stamped) **time.Time { return &r.UpdatedAt })
	Stamped_Deleted = NewNullColumn[time.Time](stampedHandle.TableOf, "deleted_at", "deleted_at", func(r *stamped) **time.Time { return &r.DeletedAt })
	stampedTable    = stampedHandle.Define(TableSpec[stamped, int64]{
		Columns:       []BoundColumn[stamped]{Stamped_ID, Stamped_Updated, Stamped_Deleted},
		PrimaryKey:    Stamped_ID,
		AutoIncrement: true,
		UpdatedAt:     Stamped_Updated,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "updated_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime, Nullable: true}},
			{Name: "deleted_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime, Nullable: true}},
		},
	}, Stamped_Deleted)
)

// TestBatchTombstonesGiveEachRowItsOwnTime covers BatchDelete and BatchRestore on
// *time.Time fields, which used to point every row at one shared time.
func TestBatchTombstonesGiveEachRowItsOwnTime(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "stamped.db"), []Table{stampedTable}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	rows := []*stamped{{}, {}}
	if err := stampedTable.BatchInsert(ctx, rt, rows); err != nil {
		t.Fatal(err)
	}

	if err := stampedTable.BatchDelete(ctx, rt, rows); err != nil {
		t.Fatal(err)
	}

	if rows[0].UpdatedAt == rows[1].UpdatedAt || rows[0].DeletedAt == rows[1].DeletedAt {
		t.Fatal("BatchDelete gave two rows one shared time")
	}

	if err := stampedTable.BatchRestore(ctx, rt, rows); err != nil {
		t.Fatal(err)
	}

	if rows[0].UpdatedAt == rows[1].UpdatedAt {
		t.Fatal("BatchRestore gave two rows one shared time")
	}
}
