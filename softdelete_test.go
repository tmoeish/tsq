package tsq

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"gopkg.in/nullbio/null.v6"
)

// Every deleted_at shape TSQ documents gets a fixture here. The soft-delete
// path used to live in the template, where the only coverage was a string
// comparison, and a whole field shape stayed broken because nothing ever built
// a row with it.
type softDeleteRow struct {
	ID        int64
	Name      string
	UpdatedAt sql.NullTime
	DeletedAt int64
	Version   int64
}

func (softDeleteRow) TSQOwner() {}

func (softDeleteRow) Table() string { return "soft_rows" }

func (softDeleteRow) Cols() []SQLColumn { return SQLColumns(softDeleteRowColumns()...) }

func (softDeleteRow) SearchColumns() []SearchColumn { return nil }

func (softDeleteRow) PrimaryKey() string { return "id" }

func (softDeleteRow) AutoIncrement() bool { return true }

func (softDeleteRow) ManagedColumns() ManagedColumns {
	return ManagedColumns{
		Version:   "version",
		UpdatedAt: "updated_at",
		DeletedAt: "deleted_at",
	}
}

func softDeleteRowColumns() []BoundColumn[softDeleteRow] {
	return []BoundColumn[softDeleteRow]{
		NewCol[softDeleteRow, int64]("id", "id", func(t *softDeleteRow) *int64 { return &t.ID }),
		NewCol[softDeleteRow, string]("name", "name", func(t *softDeleteRow) *string { return &t.Name }),
		NewCol[softDeleteRow, sql.NullTime]("updated_at", "updated_at", func(t *softDeleteRow) *sql.NullTime { return &t.UpdatedAt }),
		NewCol[softDeleteRow, int64]("deleted_at", "deleted_at", func(t *softDeleteRow) *int64 { return &t.DeletedAt }),
		NewCol[softDeleteRow, int64]("version", "version", func(t *softDeleteRow) *int64 { return &t.Version }),
	}
}

func TestApplyTombstoneCoversEveryDocumentedShape(t *testing.T) {
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		field any
		check func(*testing.T, any)
	}{
		{
			name:  "int64",
			field: new(int64),
			check: func(t *testing.T, got any) {
				if v := *got.(*int64); v != at.UnixNano() {
					t.Fatalf("expected %d, got %d", at.UnixNano(), v)
				}
			},
		},
		{
			name:  "uint64",
			field: new(uint64),
			check: func(t *testing.T, got any) {
				if v := *got.(*uint64); v != uint64(at.UnixNano()) {
					t.Fatalf("expected %d, got %d", uint64(at.UnixNano()), v)
				}
			},
		},
		{
			name:  "time.Time",
			field: new(time.Time),
			check: func(t *testing.T, got any) {
				if v := *got.(*time.Time); !v.Equal(at) {
					t.Fatalf("expected %v, got %v", at, v)
				}
			},
		},
		{
			name:  "pointer to time.Time",
			field: new(*time.Time),
			check: func(t *testing.T, got any) {
				v := *got.(**time.Time)
				if v == nil || !v.Equal(at) {
					t.Fatalf("expected %v, got %v", at, v)
				}
			},
		},
		{
			name:  "sql.NullTime",
			field: new(sql.NullTime),
			check: func(t *testing.T, got any) {
				v := *got.(*sql.NullTime)
				if !v.Valid || !v.Time.Equal(at) {
					t.Fatalf("expected valid %v, got %+v", at, v)
				}
			},
		},
		{
			name:  "null.Time",
			field: new(null.Time),
			check: func(t *testing.T, got any) {
				v := *got.(*null.Time)
				if !v.Valid || !v.Time.Equal(at) {
					t.Fatalf("expected valid %v, got %+v", at, v)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field := reflect.ValueOf(test.field).Elem()

			if err := applyTombstone(field, at); err != nil {
				t.Fatalf("applyTombstone: %v", err)
			}

			test.check(t, test.field)
		})
	}
}

func TestApplyTombstoneRejectsUnsupportedShapes(t *testing.T) {
	field := reflect.ValueOf(new(struct{ Other string })).Elem()

	if err := applyTombstone(field, time.Now()); err == nil {
		t.Fatal("expected an unsupported column type to be reported, not silently skipped")
	}
}

func TestMarkDeletedStampsTombstoneAndUpdatedAt(t *testing.T) {
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	row := &softDeleteRow{ID: 7, Name: "row"}

	if err := markDeleted(row, at); err != nil {
		t.Fatalf("markDeleted: %v", err)
	}

	if row.DeletedAt != at.UnixNano() {
		t.Fatalf("expected tombstone %d, got %d", at.UnixNano(), row.DeletedAt)
	}

	if !row.UpdatedAt.Valid || !row.UpdatedAt.Time.Equal(at) {
		t.Fatalf("expected updated_at to be refreshed, got %+v", row.UpdatedAt)
	}

	// A soft delete goes through the update path, so it must not touch the
	// version itself: the update path owns the increment.
	if row.Version != 0 {
		t.Fatalf("expected markDeleted to leave the version alone, got %d", row.Version)
	}
}

func TestSoftDeleteAssignmentsDescribeTheUpdate(t *testing.T) {
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	columns, values, version, err := softDeleteAssignments[softDeleteRow](at)
	if err != nil {
		t.Fatalf("softDeleteAssignments: %v", err)
	}

	if version != "version" {
		t.Fatalf("expected the version column to be reported, got %q", version)
	}

	want := []string{"deleted_at", "updated_at"}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("expected columns %v, got %v", want, columns)
	}

	if len(values) != len(columns) {
		t.Fatalf("expected one value per column, got %d for %d", len(values), len(columns))
	}

	if got, ok := values[0].(int64); !ok || got != at.UnixNano() {
		t.Fatalf("expected the tombstone value %d, got %#v", at.UnixNano(), values[0])
	}
}

func TestBuildSoftDeleteByPKsSQLRendersUpdate(t *testing.T) {
	got, err := buildSoftDeleteByPKsSQL("soft_rows", []string{"deleted_at", "updated_at"}, "version", "id", 2)
	if err != nil {
		t.Fatalf("buildSoftDeleteByPKsSQL: %v", err)
	}

	// The builder emits identifier markers; they become quoted names per dialect.
	rendered := renderCanonicalSQL(got)

	want := `UPDATE "soft_rows" SET "deleted_at" = ?, "updated_at" = ?, "version" = "version" + 1 WHERE "id" IN (?,?)`
	if rendered != want {
		t.Fatalf("expected\n%s\ngot\n%s", want, rendered)
	}
}
