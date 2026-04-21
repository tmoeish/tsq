package tsq

import (
	"testing"

	"gopkg.in/gorp.v2"
)

func TestRegisterTablePanicsOnDuplicateName(t *testing.T) {
	tablesMu.Lock()
	oldTables := tables
	tables = make(map[string]*RegisteredTable)
	tablesMu.Unlock()

	t.Cleanup(func() {
		tablesMu.Lock()
		tables = oldTables
		tablesMu.Unlock()
	})

	table := newMockTable("users")
	RegisterTable(table, func(db *gorp.DbMap) {}, func(db *gorp.DbMap) error { return nil })

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected duplicate table registration to panic")
		}
	}()

	RegisterTable(table, func(db *gorp.DbMap) {}, func(db *gorp.DbMap) error { return nil })
}

func TestRegisterTableRejectsNilInputs(t *testing.T) {
	tablesMu.Lock()
	oldTables := tables
	tables = make(map[string]*RegisteredTable)
	tablesMu.Unlock()

	t.Cleanup(func() {
		tablesMu.Lock()
		tables = oldTables
		tablesMu.Unlock()
	})

	tests := []struct {
		name string
		fn   func()
	}{
		{
			name: "nil table",
			fn: func() {
				RegisterTable(nil, func(db *gorp.DbMap) {}, func(db *gorp.DbMap) error { return nil })
			},
		},
		{
			name: "nil add table func",
			fn: func() {
				RegisterTable(newMockTable("users"), nil, func(db *gorp.DbMap) error { return nil })
			},
		},
		{
			name: "nil init func",
			fn: func() {
				RegisterTable(newMockTable("users"), func(db *gorp.DbMap) {}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected invalid registration input to panic")
				}
			}()

			tt.fn()
		})
	}
}

func TestSnapshotRegisteredTablesReturnsDeterministicOrder(t *testing.T) {
	tablesMu.Lock()
	oldTables := tables
	tables = map[string]*RegisteredTable{
		"users": {
			Table:        newMockTable("users"),
			AddTableFunc: func(db *gorp.DbMap) {},
			InitFunc:     func(db *gorp.DbMap) error { return nil },
		},
		"accounts": {
			Table:        newMockTable("accounts"),
			AddTableFunc: func(db *gorp.DbMap) {},
			InitFunc:     func(db *gorp.DbMap) error { return nil },
		},
	}
	tablesMu.Unlock()

	t.Cleanup(func() {
		tablesMu.Lock()
		tables = oldTables
		tablesMu.Unlock()
	})

	snapshot := snapshotRegisteredTables()
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 registered tables, got %d", len(snapshot))
	}

	if got := snapshot[0].Table.Table(); got != "accounts" {
		t.Fatalf("expected deterministic alphabetical order, got first table %q", got)
	}

	if got := snapshot[1].Table.Table(); got != "users" {
		t.Fatalf("expected deterministic alphabetical order, got second table %q", got)
	}
}

func TestInitDeduplicatesProvidedTracers(t *testing.T) {
	tablesMu.Lock()
	oldTables := tables
	tables = make(map[string]*RegisteredTable)
	tablesMu.Unlock()

	tracersMu.Lock()
	oldTracers := tracers
	tracers = nil
	tracersMu.Unlock()

	t.Cleanup(func() {
		tablesMu.Lock()
		tables = oldTables
		tablesMu.Unlock()

		tracersMu.Lock()
		tracers = oldTracers
		tracersMu.Unlock()
	})

	tracer := func(next Fn) Fn { return next }

	if err := Init(&gorp.DbMap{}, false, false, tracer); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	if err := Init(&gorp.DbMap{}, false, false, tracer); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	if got := len(GetTracers()); got != 1 {
		t.Fatalf("expected Init to deduplicate tracers, got %d", got)
	}
}

func TestUpsertIndexRejectsInvalidIdentifiers(t *testing.T) {
	db := &gorp.DbMap{Dialect: gorp.MySQLDialect{}}

	err := UpsertIndex(db, "users;drop", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected invalid table name to return an error")
	}

	err = UpsertIndex(db, "users", false, "idx users id", []string{"id"})
	if err == nil {
		t.Fatal("expected invalid index name to return an error")
	}

	err = UpsertIndex(db, "users", false, "idx_users_id", []string{"id", "name desc"})
	if err == nil {
		t.Fatal("expected invalid field name to return an error")
	}
}

func TestUpsertIndexRejectsEmptyFields(t *testing.T) {
	db := &gorp.DbMap{Dialect: gorp.MySQLDialect{}}

	err := UpsertIndex(db, "users", false, "idx_users_id", nil)
	if err == nil {
		t.Fatal("expected empty index fields to return an error")
	}
}

func TestUpsertIndexRejectsNilDbMap(t *testing.T) {
	err := UpsertIndex(nil, "users", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected nil db map to return an error")
	}
}

func TestInitRejectsNilDbMap(t *testing.T) {
	if err := Init(nil, false, false); err == nil {
		t.Fatal("expected nil db map to return an error")
	}
}
