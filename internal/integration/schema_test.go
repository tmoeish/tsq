package integration_test

// Schema policies and generated DDL over shapes the academy fixture does not have:
// tables that already hold rows, unsigned keys, raw types and defaults that each
// engine reports in a spelling of its own. Every case here failed on at least one
// real engine while the SQLite-only suite was green.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tmoeish/tsq/v5"
	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// dropTables removes the named tables from the target database.
func dropTables(t *testing.T, target integrationTarget, names ...string) {
	t.Helper()

	db, err := sql.Open(target.driver, target.dsn)
	if err != nil {
		t.Fatalf("open %s: %v", target.name, err)
	}

	defer func() { _ = db.Close() }()

	for _, name := range names {
		if _, err := db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+name); err != nil {
			t.Fatalf("drop %s on %s: %v", name, target.name, err)
		}
	}
}

// openQuietly opens a runtime with policy and returns the DDL it ran.
func openQuietly(target integrationTarget, policy tsq.SchemaPolicy, tables ...tsq.Table) (*tsq.Runtime, []string, error) {
	recorder := &ddlRecorder{}

	rt, err := tsq.Open(context.Background(), target.driver, target.dsn, tables, tsq.WithSchemaPolicy(policy), tsq.WithLogger(recorder))

	return rt, recorder.statements(), err
}

// columnValues reads one column of every row as text, in key order.
func columnValues(t *testing.T, target integrationTarget, table, column string) []string {
	t.Helper()

	db, err := sql.Open(target.driver, target.dsn)
	if err != nil {
		t.Fatalf("open %s: %v", target.name, err)
	}

	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(context.Background(), "SELECT "+column+" FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatalf("read %s.%s on %s: %v", table, column, target.name, err)
	}

	defer func() { _ = rows.Close() }()

	var values []string

	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan %s.%s on %s: %v", table, column, target.name, err)
		}

		if b, ok := v.([]byte); ok {
			v = string(b)
		}

		values = append(values, fmt.Sprint(v))
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("read %s.%s on %s: %v", table, column, target.name, err)
	}

	return values
}

// grownBefore is the grown table as first created: two columns it keeps, and
// three nullable ones a later version declares NOT NULL.
type grownBefore struct {
	ID   int64
	Name string
	Y    *int64
	YT   *time.Time
	YS   *string
}

// grownAfter is the same table once the struct gained fields and tightened others.
type grownAfter struct {
	ID    int64
	Name  string
	XI    int32
	XS    string
	XL    string
	XT    time.Time
	XB    []byte
	XF    float64
	XBool bool
	Y     sql.Null[int64]
	YT    sql.Null[time.Time]
	YS    sql.Null[string]
}

func grownBeforeTable() *tsq.TableOf[grownBefore, int64] {
	h := tsq.NewTable[grownBefore, int64]("grown")
	id := tsq.NewColumn(h, "id", "id", func(r *grownBefore) *int64 { return &r.ID })

	return h.Define(tsq.TableSpec[grownBefore, int64]{
		Columns: []tsq.BoundColumn[grownBefore]{
			id,
			tsq.NewColumn(h, "name", "name", func(r *grownBefore) *string { return &r.Name }),
			tsq.NewNullColumn[int64](h, "y", "y", func(r *grownBefore) **int64 { return &r.Y }),
			tsq.NewNullColumn[time.Time](h, "yt", "yt", func(r *grownBefore) **time.Time { return &r.YT }),
			tsq.NewNullColumn[string](h, "ys", "ys", func(r *grownBefore) **string { return &r.YS }),
		},
		PrimaryKey:    id,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40}},
			{Name: "y", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64, Nullable: true}},
			{Name: "yt", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime, Nullable: true}},
			{Name: "ys", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40, Nullable: true}},
		},
	})
}

// grownAfterTable declares the grown table with one more NOT NULL column (add),
// or with one of y, yt, ys NOT NULL (tighten).
func grownAfterTable(change string) (*tsq.TableOf[grownAfter, int64], tsq.Column[grownAfter, int64]) {
	h := tsq.NewTable[grownAfter, int64]("grown")
	id := tsq.NewColumn(h, "id", "id", func(r *grownAfter) *int64 { return &r.ID })

	columns := []tsq.BoundColumn[grownAfter]{
		id,
		tsq.NewColumn(h, "name", "name", func(r *grownAfter) *string { return &r.Name }),
		tsq.NewNullColumn[int64](h, "y", "y", func(r *grownAfter) *sql.Null[int64] { return &r.Y }),
		tsq.NewNullColumn[time.Time](h, "yt", "yt", func(r *grownAfter) *sql.Null[time.Time] { return &r.YT }),
		tsq.NewNullColumn[string](h, "ys", "ys", func(r *grownAfter) *sql.Null[string] { return &r.YS }),
	}
	specs := []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "name", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40}},
		{Name: "y", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64, Nullable: true}},
		{Name: "yt", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime, Nullable: true}},
		{Name: "ys", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40, Nullable: true}},
	}

	add := func(column tsq.BoundColumn[grownAfter], spec tsqdialect.ColumnSpec) {
		columns = append(columns, column)
		specs = append(specs, spec)
	}

	switch change {
	case "add int":
		add(tsq.NewColumn(h, "xi", "xi", func(r *grownAfter) *int32 { return &r.XI }),
			tsqdialect.ColumnSpec{Name: "xi", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 32}})
	case "add string":
		add(tsq.NewColumn(h, "xs", "xs", func(r *grownAfter) *string { return &r.XS }),
			tsqdialect.ColumnSpec{Name: "xs", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40}})
	case "add large string":
		add(tsq.NewColumn(h, "xl", "xl", func(r *grownAfter) *string { return &r.XL }),
			tsqdialect.ColumnSpec{Name: "xl", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 100000}})
	case "add time":
		add(tsq.NewColumn(h, "xt", "xt", func(r *grownAfter) *time.Time { return &r.XT }),
			tsqdialect.ColumnSpec{Name: "xt", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime}})
	case "add bytes":
		add(tsq.NewColumn(h, "xb", "xb", func(r *grownAfter) *[]byte { return &r.XB }),
			tsqdialect.ColumnSpec{Name: "xb", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBytes}})
	case "add float":
		add(tsq.NewColumn(h, "xf", "xf", func(r *grownAfter) *float64 { return &r.XF }),
			tsqdialect.ColumnSpec{Name: "xf", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindFloat, Bits: 64}})
	case "add bool":
		add(tsq.NewColumn(h, "xbool", "xbool", func(r *grownAfter) *bool { return &r.XBool }),
			tsqdialect.ColumnSpec{Name: "xbool", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBool}})
	case "tighten int":
		specs[2].Type.Nullable = false
	case "tighten time":
		specs[3].Type.Nullable = false
	case "tighten string":
		specs[4].Type.Nullable = false
	default:
		panic(change)
	}

	return h.Define(tsq.TableSpec[grownAfter, int64]{Columns: columns, PrimaryKey: id, AutoIncrement: true, ColumnSpecs: specs}), id
}

// TestIntegrationPoliciesFollowAStructOverATableWithRows covers "change the
// struct and restart": a field added to a struct is a NOT NULL column, and adding
// one to a table that held rows failed on PostgreSQL and SQLite for every type and
// on MySQL for a time; a nullable column turned NOT NULL failed on SQLite. The
// rows present get the zero value, as the generator's migrations give them.
func TestIntegrationPoliciesFollowAStructOverATableWithRows(t *testing.T) {
	ctx := context.Background()
	adds := []string{"add int", "add string", "add large string", "add time", "add bytes", "add float", "add bool"}
	tightens := []string{"tighten int", "tighten time", "tighten string"}

	for _, target := range integrationTargets(t) {
		for _, policy := range []tsq.SchemaPolicy{tsq.SchemaPolicyCreateMissing, tsq.SchemaPolicyReconcile} {
			changes := adds
			if policy == tsq.SchemaPolicyReconcile {
				changes = append(append([]string{}, adds...), tightens...)
			}

			for _, change := range changes {
				t.Run(target.name+"/"+string(policy)+"/"+change, func(t *testing.T) {
					dropTables(t, target, "grown")

					before := grownBeforeTable()

					rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, before)
					if err != nil {
						t.Fatalf("create: %v", err)
					}

					for _, name := range []string{"a", "b"} {
						if err := before.Insert(ctx, rt, &grownBefore{Name: name}); err != nil {
							t.Fatalf("seed: %v", err)
						}
					}

					_ = rt.Close()

					after, afterID := grownAfterTable(change)
					if err := after.Err(); err != nil {
						t.Fatalf("define: %v", err)
					}

					rt, ran, err := openQuietly(target, policy, after)
					if err != nil {
						t.Fatalf("%s over a table with rows: %v\nran:\n  %s", policy, err, strings.Join(ran, "\n  "))
					}

					rows, err := tsq.Select(after.Columns()...).From(after).OrderBy(afterID.Asc()).List(ctx, rt)
					_ = rt.Close()

					if err != nil || len(rows) != 2 {
						t.Fatalf("rows after the change: %d, %v", len(rows), err)
					}

					for _, r := range rows {
						if r.XI != 0 || r.XS != "" || r.XL != "" || !r.XT.IsZero() || len(r.XB) != 0 || r.XF != 0 || r.XBool {
							t.Fatalf("a new column did not get its zero value: %+v", *r)
						}
					}

					// MySQL stores a time literal it cannot take as 0000-00-00, which
					// reads back as the zero time and then fails every table copy.
					for _, column := range []string{"xt", "yt"} {
						if (change == "add time" && column == "xt") || (change == "tighten time" && column == "yt") {
							// Read as text: the driver hands 0000-00-00 back as the zero time.
							for _, stored := range columnValues(t, target, "grown", "CAST("+column+" AS CHAR(40))") {
								if !strings.HasPrefix(stored, "0001-01-01") {
									t.Fatalf("%s holds %q, want the zero time 0001-01-01", column, stored)
								}
							}
						}
					}

					// The table is now what is declared: nothing left to do.
					rt, ran, err = openQuietly(target, policy, after)
					if err != nil {
						t.Fatalf("second start: %v", err)
					}

					_ = rt.Close()

					if len(ran) != 0 {
						t.Fatalf("second start ran DDL again:\n  %s", strings.Join(ran, "\n  "))
					}
				})
			}
		}
	}
}

type keyed struct {
	ID   uint64
	Note string
}

// TestIntegrationUnsignedAutoIncrementKeysAreStable covers a `uint` key, the most
// common key type there is: PostgreSQL has no unsigned types, so the column was
// declared one type wider than the SERIAL it was created as, and the table TSQ had
// just created failed validation on the next start.
func TestIntegrationUnsignedAutoIncrementKeysAreStable(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		for _, bits := range []int{8, 16, 32, 64} {
			t.Run(fmt.Sprintf("%s/uint%d", target.name, bits), func(t *testing.T) {
				dropTables(t, target, "keyed")

				h := tsq.NewTable[keyed, uint64]("keyed")
				id := tsq.NewColumn(h, "id", "id", func(r *keyed) *uint64 { return &r.ID })
				table := h.Define(tsq.TableSpec[keyed, uint64]{
					Columns:       []tsq.BoundColumn[keyed]{id, tsq.NewColumn(h, "note", "note", func(r *keyed) *string { return &r.Note })},
					PrimaryKey:    id,
					AutoIncrement: true,
					ColumnSpecs: []tsqdialect.ColumnSpec{
						{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: bits, Unsigned: true}, PrimaryKey: true, AutoIncrement: true},
						{Name: "note", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 20}},
					},
				})

				rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, table)
				if err != nil {
					t.Fatalf("create: %v", err)
				}

				row := &keyed{Note: "first"}
				if err := table.Insert(ctx, rt, row); err != nil || row.ID != 1 {
					t.Fatalf("insert: id=%d, %v", row.ID, err)
				}

				if got, err := table.Get(ctx, rt, 1); err != nil || got.Note != "first" {
					t.Fatalf("get by an unsigned key: %v, %v", got, err)
				}

				_ = rt.Close()

				for _, policy := range []tsq.SchemaPolicy{tsq.SchemaPolicyValidate, tsq.SchemaPolicyCreateMissing, tsq.SchemaPolicyReconcile} {
					rt, ran, err := openQuietly(target, policy, table)
					if err != nil {
						t.Fatalf("%s over the table TSQ created: %v", policy, err)
					}

					_ = rt.Close()

					if len(ran) != 0 {
						t.Fatalf("%s ran DDL over the table TSQ created:\n  %s", policy, strings.Join(ran, "\n  "))
					}
				}
			})
		}
	}
}

type drifting struct {
	ID int64
	C  sql.Null[string]
}

func driftingTable(columnType tsqdialect.ColumnType, defaultSQL string) *tsq.TableOf[drifting, int64] {
	h := tsq.NewTable[drifting, int64]("drifting")
	id := tsq.NewColumn(h, "id", "id", func(r *drifting) *int64 { return &r.ID })

	spec := tsqdialect.ColumnSpec{Name: "c", Type: columnType, Default: defaultSQL}
	if defaultSQL != "" {
		spec.Type.Nullable = true
		spec.Fill = tsqdialect.FillDefault
	}

	return h.Define(tsq.TableSpec[drifting, int64]{
		Columns:       []tsq.BoundColumn[drifting]{id, tsq.NewNullColumn[string](h, "c", "c", func(r *drifting) *sql.Null[string] { return &r.C })},
		PrimaryKey:    id,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			spec,
		},
	})
}

// TestIntegrationDeclaredColumnsDoNotDrift is the suite's core assertion (a
// second start runs no DDL) over what each engine reports in its own spelling: a
// literal default MySQL hands back without its quotes, and a raw type the engine
// knows under another name. Each of these failed Validate on the table TSQ had
// created, and had Reconcile run the same ALTER on every start.
func TestIntegrationDeclaredColumnsDoNotDrift(t *testing.T) {
	text := func(size int) tsqdialect.ColumnType {
		return tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: size}
	}
	raw := func(spelled string) tsqdialect.ColumnType { return tsqdialect.ColumnType{RawType: spelled} }

	type driftCase struct {
		name    string
		column  tsqdialect.ColumnType
		def     string
		engines string // empty: every engine
	}

	cases := []driftCase{
		{"default in parentheses", text(40), "'(none)'", ""},
		{"default with a cast-like ::", text(40), "'a::b'", ""},
		{"default with padding", text(40), "'  pad  '", ""},
		{"default with a quote", text(40), "'it''s'", ""},
		{"default that reads like a keyword", text(40), "'CURRENT_TIMESTAMP'", ""},
		{"time literal default", tsqdialect.ColumnType{Kind: tsqdialect.KindTime}, "'2020-01-02 03:04:05'", ""},
		{"string longer than a VARCHAR", text(20000000), "", ""},
		{"DECIMAL(10,2) default", raw("DECIMAL(10,2)"), "1.50", ""},
		{"DECIMAL", raw("DECIMAL"), "", ""},
		{"NUMERIC", raw("NUMERIC"), "", ""},
		{"CHAR(3) default", raw("CHAR(3)"), "'USD'", ""},
		{"TIME", raw("TIME"), "", ""},
		{"REAL", raw("REAL"), "", ""},
		{"DOUBLE PRECISION", raw("DOUBLE PRECISION"), "", ""},
		{"BOOL", raw("BOOL"), "", "mysql,sqlite"},
		{"INT(11)", raw("INT(11)"), "", "mysql"},
		{"VARCHAR with a collation", raw("VARCHAR(20) COLLATE utf8mb4_bin"), "", "mysql"},
		{"FLOAT", raw("FLOAT"), "", ""},
		{"TIMESTAMP(3)", raw("TIMESTAMP(3)"), "", "mysql,postgres"},
		{"INT4", raw("INT4"), "", "postgres"},
		{"INT8", raw("INT8"), "", "postgres"},
		{"FLOAT8", raw("FLOAT8"), "", "postgres"},
	}

	for _, target := range integrationTargets(t) {
		for _, c := range cases {
			if c.engines != "" && !strings.Contains(c.engines, target.name) {
				continue
			}

			t.Run(target.name+"/"+c.name, func(t *testing.T) {
				dropTables(t, target, "drifting")

				table := driftingTable(c.column, c.def)

				rt, created, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, table)
				if err != nil {
					t.Fatalf("create: %v", err)
				}

				_ = rt.Close()

				for _, policy := range []tsq.SchemaPolicy{tsq.SchemaPolicyValidate, tsq.SchemaPolicyReconcile} {
					rt, ran, err := openQuietly(target, policy, table)
					if err != nil {
						t.Fatalf("%s over the table TSQ created: %v\ncreated with:\n  %s", policy, err, strings.Join(created, "\n  "))
					}

					_ = rt.Close()

					if len(ran) != 0 {
						t.Fatalf("%s ran DDL over the table TSQ created:\n  %s", policy, strings.Join(ran, "\n  "))
					}
				}
			})
		}
	}
}

// TestIntegrationRetypeRefusesAValueThatDoesNotFit covers a column type change
// over rows on the engines that enforce a length: PostgreSQL was told to cast to
// the new type itself, and a cast to VARCHAR(5) cuts 'abcdefghij' to 'abcde'
// without a word. The change must fail and leave the rows as they were.
func TestIntegrationRetypeRefusesAValueThatDoesNotFit(t *testing.T) {
	text := func(size int) tsqdialect.ColumnType {
		return tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: size}
	}

	for _, target := range integrationTargets(t) {
		if target.name == "sqlite" {
			continue // SQLite keeps any value in any column.
		}

		for _, c := range []struct {
			name     string
			from, to tsqdialect.ColumnType
			value    string
		}{
			{"TEXT to a short string", tsqdialect.ColumnType{RawType: "TEXT"}, text(5), "'abcdefghij'"},
			{"string to CHAR(3)", text(40), tsqdialect.ColumnType{RawType: "CHAR(3)"}, "'abcdefghij'"},
			{"integer to a short string", tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, text(1), "12345"},
			{"time to a short string", tsqdialect.ColumnType{Kind: tsqdialect.KindTime}, text(10), "'2020-01-02 03:04:05'"},
		} {
			t.Run(target.name+"/"+c.name, func(t *testing.T) {
				dropTables(t, target, "drifting")

				rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, driftingTable(c.from, ""))
				if err != nil {
					t.Fatalf("create: %v", err)
				}

				if _, err := rt.ExecContext(context.Background(), "INSERT INTO drifting (c) VALUES ("+c.value+")"); err != nil {
					t.Fatalf("seed: %v", err)
				}

				_ = rt.Close()

				before := columnValues(t, target, "drifting", "c")

				rt, ran, err := openQuietly(target, tsq.SchemaPolicyReconcile, driftingTable(c.to, ""))
				if err == nil {
					_ = rt.Close()

					t.Fatalf("the change went through; the column now holds %v (was %v)\nran:\n  %s",
						columnValues(t, target, "drifting", "c"), before, strings.Join(ran, "\n  "))
				}

				if after := columnValues(t, target, "drifting", "c"); !strings.EqualFold(strings.Join(after, ","), strings.Join(before, ",")) {
					t.Fatalf("the change failed and still altered the rows: %v, was %v", after, before)
				}
			})
		}
	}
}
