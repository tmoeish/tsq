package integration_test

// Schema policies and generated DDL over shapes the academy fixture does not have:
// tables that already hold rows, unsigned keys, raw types and defaults that each
// engine reports in a spelling of its own. Every case here failed on at least one
// real engine while the SQLite-only suite was green.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
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

// driftCase is a column that must read back as it was declared.
type driftCase struct {
	name    string
	column  tsqdialect.ColumnType
	def     string
	engines string // empty: every engine
}

// driftCases lists the columns each engine reports in a spelling of its own.
func driftCases() []driftCase {
	text := func(size int) tsqdialect.ColumnType {
		return tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: size}
	}
	raw := func(spelled string) tsqdialect.ColumnType { return tsqdialect.ColumnType{RawType: spelled} }
	integer := tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}

	return []driftCase{
		{"default in parentheses", text(40), "'(none)'", ""},
		{"default with a cast-like ::", text(40), "'a::b'", ""},
		{"default with padding", text(40), "'  pad  '", ""},
		{"default with a quote", text(40), "'it''s'", ""},
		{"default that reads like a keyword", text(40), "'CURRENT_TIMESTAMP'", ""},
		{"time literal default", tsqdialect.ColumnType{Kind: tsqdialect.KindTime}, "'2020-01-02 03:04:05'", ""},
		// PostgreSQL takes TRUE and FALSE only; the other two take either spelling.
		{"boolean default as a number", tsqdialect.ColumnType{Kind: tsqdialect.KindBool}, "1", ""},
		{"boolean default as zero", tsqdialect.ColumnType{Kind: tsqdialect.KindBool}, "0", ""},
		{"boolean default as a word", tsqdialect.ColumnType{Kind: tsqdialect.KindBool}, "TRUE", ""},
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
		// What no table of spellings kept up with, settled by asking the engine.
		{"DECIMAL(10)", raw("DECIMAL(10)"), "", "mysql"},
		{"INTEGER UNSIGNED", raw("INTEGER UNSIGNED"), "", "mysql"},
		{"CHAR", raw("CHAR"), "", "mysql,postgres"},
		{"BIT", raw("BIT"), "", "mysql"},
		{"BIT(1) default", raw("BIT(1)"), "1", "mysql"},
		{"NVARCHAR(10)", raw("NVARCHAR(10)"), "", "mysql"},
		{"NCHAR(3)", raw("NCHAR(3)"), "", "mysql"},
		{"YEAR(4)", raw("YEAR(4)"), "", "mysql"},
		{"FLOAT(24)", raw("FLOAT(24)"), "", "mysql,postgres"},
		{"FLOAT(53)", raw("FLOAT(53)"), "", "mysql,postgres"},
		{"INT1", raw("INT1"), "", "mysql"},
		{"MIDDLEINT", raw("MIDDLEINT"), "", "mysql"},
		{"INT ZEROFILL", raw("INT ZEROFILL"), "", "mysql"},
		{"LONG VARCHAR", raw("LONG VARCHAR"), "", "mysql"},
		{"VARCHAR BINARY", raw("VARCHAR(10) BINARY"), "", "mysql"},
		{"DATE default", raw("DATE"), "(CURRENT_DATE)", "mysql"},
		{"INT[]", raw("INT[]"), "", "postgres"},
		{"VARCHAR(10)[]", raw("VARCHAR(10)[]"), "", "postgres"},
		{"INT[] default", raw("INT[]"), "'{}'", "postgres"},
		{"NUMERIC(10)", raw("NUMERIC(10)"), "", "postgres"},
		{"default with a backslash", text(40), `'a\\b'`, ""},
		{"expression default", integer, "(1+1)", ""},
		{"function default", raw("INTEGER"), "(abs(-1))", ""},
		{"function default over text", raw("TEXT"), "(lower('X'))", "postgres,sqlite"},
		{"signed default", integer, "+5", "postgres,sqlite"},
		{"time default with a zone", tsqdialect.ColumnType{Kind: tsqdialect.KindTime}, "'2020-01-02T03:04:05Z'", "postgres"},
		{"empty bytes default", tsqdialect.ColumnType{Kind: tsqdialect.KindBytes}, "''", "postgres,sqlite"},
		// MySQL describes a kept table from its dictionary and a temporary one from
		// memory, and the two spell these defaults differently: the probe compares
		// two temporary tables, the declaration and the live column.
		{"VARBINARY literal default", raw("VARBINARY(8)"), "'ab'", "mysql"},
		{"VARBINARY hex default", raw("VARBINARY(8)"), "X'6162'", "mysql"},
		{"BINARY literal default", raw("BINARY(4)"), "'ab'", "mysql"},
		{"bytes hex default", tsqdialect.ColumnType{Kind: tsqdialect.KindBytes}, "X'00'", "mysql,sqlite"},
		{"default with a four-byte character", text(40), "'ok \U0001F600'", ""},
		// A SERIAL off the key draws from a sequence named after its own table.
		{"SERIAL off the key", raw("SERIAL"), "", "postgres"},
		{"BIGSERIAL off the key", raw("BIGSERIAL"), "", "postgres"},
	}
}

// TestIntegrationDeclaredColumnsDoNotDrift is the suite's core assertion (a
// second start runs no DDL) over what each engine reports in its own spelling: a
// literal default MySQL hands back without its quotes, and a raw type the engine
// knows under another name. Each of these failed Validate on the table TSQ had
// created, and had Reconcile run the same ALTER on every start.
func TestIntegrationDeclaredColumnsDoNotDrift(t *testing.T) {
	for _, target := range integrationTargets(t) {
		for _, c := range driftCases() {
			if c.engines != "" && !strings.Contains(c.engines, target.name) {
				continue
			}

			t.Run(target.name+"/"+c.name, func(t *testing.T) { declaredColumnDoesNotDrift(t, target, c) })
		}
	}
}

// declaredColumnDoesNotDrift creates the column and starts twice more over it.
func declaredColumnDoesNotDrift(t *testing.T, target integrationTarget, c driftCase) {
	t.Helper()
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
		for _, c := range []struct {
			name     string
			from, to tsqdialect.ColumnType
			value    string
			sqlite   bool // SQLite does not enforce a length; it refuses a value of another kind.
		}{
			{"TEXT to a short string", tsqdialect.ColumnType{RawType: "TEXT"}, text(5), "'abcdefghij'", false},
			{"string to CHAR(3)", text(40), tsqdialect.ColumnType{RawType: "CHAR(3)"}, "'abcdefghij'", false},
			{"integer to a short string", tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, text(1), "12345", false},
			{"time to a short string", tsqdialect.ColumnType{Kind: tsqdialect.KindTime}, text(10), "'2020-01-02 03:04:05'", false},
			{"text to an integer", text(40), tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, "'Hello'", true},
			{"text to a number", text(40), tsqdialect.ColumnType{Kind: tsqdialect.KindFloat, Bits: 64}, "'Hello'", true},
			{"text to a time", text(40), tsqdialect.ColumnType{Kind: tsqdialect.KindTime}, "'Hello'", true},
			{"text to a boolean", text(40), tsqdialect.ColumnType{Kind: tsqdialect.KindBool}, "'Hello'", true},
		} {
			if target.name == "sqlite" && !c.sqlite {
				continue
			}

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

// TestIntegrationRetypeCarriesTheValues covers a type change every value
// converts under, where the three engines must end up holding the same thing.
// PostgreSQL cast bytes to text as their hex spelling and text to bytes as an
// escape string, SQLite kept 1.5 under an integer column and 2 under a boolean
// one (after which no read of the table worked), and MySQL kept the 2 as well.
type dupRow struct {
	ID    int64
	Code  string
	Note  *string
	Extra int64
}

// dupTable declares the dup table with or without an extra column and a unique
// index over code and note.
func dupTable(withExtra, withIndex bool) *tsq.TableOf[dupRow, int64] {
	t := tsq.NewTable[dupRow, int64]("dup")
	id := tsq.NewColumn(t, "id", "id", func(r *dupRow) *int64 { return &r.ID })
	code := tsq.NewColumn(t, "code", "code", func(r *dupRow) *string { return &r.Code })
	note := tsq.NewNullColumn[string](t, "note", "note", func(r *dupRow) **string { return &r.Note })
	columns := []tsq.BoundColumn[dupRow]{id, code, note}
	specs := []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "code", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 20}},
		{Name: "note", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 20, Nullable: true}},
	}

	if withExtra {
		columns = append(columns, tsq.NewColumn(t, "extra", "extra", func(r *dupRow) *int64 { return &r.Extra }))
		specs = append(specs, tsqdialect.ColumnSpec{Name: "extra", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}})
	}

	var indexes []tsq.IndexSpec
	if withIndex {
		indexes = []tsq.IndexSpec{{Name: "ux_dup_code_note", Columns: []string{"code", "note"}, Unique: true}}
	}

	return t.Define(tsq.TableSpec[dupRow, int64]{Columns: columns, PrimaryKey: id, AutoIncrement: true, ColumnSpecs: specs, Indexes: indexes})
}

// TestIntegrationAUniqueIndexOverDuplicatesIsRefusedFirst covers a declaration
// that adds a column and a unique index over values rows already share. The
// column was added, the index was then refused by the engine, and the table was
// left altered and without it, which every later start repeated; now the rows
// are looked at before any DDL, the start fails with a DuplicateRowsError naming
// the shared values, and the table is as it was. Rows holding NULL in an index
// column never conflict and do not count.
func TestIntegrationAUniqueIndexOverDuplicatesIsRefusedFirst(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropTables(t, target, "dup")

			plain := dupTable(false, false)

			rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, plain)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			note := "n"
			if err := plain.BatchInsert(ctx, rt, []*dupRow{{Code: "a", Note: &note}, {Code: "a", Note: &note}, {Code: "b"}, {Code: "b"}}); err != nil {
				t.Fatalf("rows: %v", err)
			}

			_ = rt.Close()

			_, ran, err := openQuietly(target, tsq.SchemaPolicyReconcile, dupTable(true, true))

			var dup *tsq.DuplicateRowsError
			if !errors.As(err, &dup) || dup.Table != "dup" || dup.Index != "ux_dup_code_note" || dup.Rows != 2 || fmt.Sprint(dup.Values) != "[a n]" {
				t.Fatalf("Reconcile = %v (ran %v); want a DuplicateRowsError for the two a/n rows", err, ran)
			}

			if len(ran) != 0 {
				t.Fatalf("the refusal came after DDL: %v", ran)
			}

			// The table is as it was: the first declaration still validates.
			if rt, _, err := openQuietly(target, tsq.SchemaPolicyValidate, plain); err != nil {
				t.Fatalf("the table changed before the refusal: %v", err)
			} else {
				_ = rt.Close()
			}

			// With the duplicates gone (the b rows hold NULL and never conflicted),
			// the same start goes through.
			rt, _, err = openQuietly(target, tsq.SchemaPolicyCreateMissing, plain)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := tsq.HardDeleteFrom(plain).Where(tsq.NewColumn(plain, "code", "code", func(r *dupRow) *string { return &r.Code }).EQ(tsq.Val("a"))).MustBuild().Exec(ctx, rt); err != nil {
				t.Fatal(err)
			}

			_ = rt.Close()

			rt, ran, err = openQuietly(target, tsq.SchemaPolicyReconcile, dupTable(true, true))
			if err != nil || len(ran) == 0 {
				t.Fatalf("Reconcile without duplicates = %v, ran %v", err, ran)
			}

			_ = rt.Close()
		})
	}
}

// TestIntegrationAnIndexInTheWayGoesFirst covers an indexed column retyped into
// one the index cannot cover, while the declaration drops the index too: MySQL
// refused to alter the column while the index stood ("BLOB/TEXT column used in
// key specification without a key length"), and the index was dropped only
// afterwards, by the index policy. An index no longer declared now goes before
// the column it covers is altered.
func TestIntegrationAnIndexInTheWayGoesFirst(t *testing.T) {
	ctx := context.Background()
	text := tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40}
	bytes := tsqdialect.ColumnType{Kind: tsqdialect.KindBytes}

	declare := func(columnType tsqdialect.ColumnType, indexes ...tsq.IndexSpec) *tsq.TableOf[drifting, int64] {
		h := tsq.NewTable[drifting, int64]("drifting")
		id := tsq.NewColumn(h, "id", "id", func(r *drifting) *int64 { return &r.ID })

		return h.Define(tsq.TableSpec[drifting, int64]{
			Columns:       []tsq.BoundColumn[drifting]{id, tsq.NewNullColumn[string](h, "c", "c", func(r *drifting) *sql.Null[string] { return &r.C })},
			PrimaryKey:    id,
			AutoIncrement: true,
			ColumnSpecs: []tsqdialect.ColumnSpec{
				{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
				{Name: "c", Type: columnType},
			},
			Indexes: indexes,
		})
	}

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropTables(t, target, "drifting")

			indexed := declare(text, tsq.IndexSpec{Name: "idx_drifting_c", Columns: []string{"c"}})

			rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, indexed)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			if _, err := rt.ExecContext(ctx, "INSERT INTO drifting (c) VALUES ('abc')"); err != nil {
				t.Fatalf("seed: %v", err)
			}

			_ = rt.Close()

			plain := declare(bytes)

			rt, ran, err := openQuietly(target, tsq.SchemaPolicyReconcile, plain)
			if err != nil {
				t.Fatalf("reconcile: %v\nran:\n  %s", err, strings.Join(ran, "\n  "))
			}

			_ = rt.Close()

			if got := columnValues(t, target, "drifting", "c"); strings.Join(got, ",") != "abc" {
				t.Fatalf("the column holds %q after the change, want abc", got)
			}

			rt, again, err := openQuietly(target, tsq.SchemaPolicyValidate, plain)
			if err != nil || len(again) != 0 {
				t.Fatalf("validate after the change: %v, ran %v", err, again)
			}

			_ = rt.Close()

			// A declared index on such a column is the declaration's mistake, and the
			// server's refusal stands: nothing is dropped for it.
			dropTables(t, target, "drifting")

			rt, _, err = openQuietly(target, tsq.SchemaPolicyCreateMissing, indexed)
			if err != nil {
				t.Fatalf("create again: %v", err)
			}

			_ = rt.Close()

			if target.name == "mysql" {
				rt, _, err := openQuietly(target, tsq.SchemaPolicyReconcile, declare(bytes, tsq.IndexSpec{Name: "idx_drifting_c", Columns: []string{"c"}}))
				if err == nil {
					_ = rt.Close()

					t.Fatal("reconcile altered an indexed column into a BLOB with the index declared")
				}

				// The refusal left the table as it was.
				rt, kept, err := openQuietly(target, tsq.SchemaPolicyValidate, indexed)
				if err != nil || len(kept) != 0 {
					t.Fatalf("the table after the refused change: %v, ran %v", err, kept)
				}

				_ = rt.Close()
			}
		})
	}
}

func TestIntegrationRetypeCarriesTheValues(t *testing.T) {
	text := tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40}
	bytes := tsqdialect.ColumnType{Kind: tsqdialect.KindBytes}
	integer := tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}
	number := tsqdialect.ColumnType{Kind: tsqdialect.KindFloat, Bits: 64}
	boolean := tsqdialect.ColumnType{Kind: tsqdialect.KindBool}

	for _, target := range integrationTargets(t) {
		for _, c := range []struct {
			name     string
			from, to tsqdialect.ColumnType
			values   []any
			want     string
			defaults [2]string
		}{
			{"bytes to text", bytes, text, []any{[]byte("abc"), []byte("it's")}, "abc,it's", [2]string{}},
			{"text to bytes", text, bytes, []any{`tab\101x`, `a\\b`, "plain"}, `tab\101x,a\\b,plain`, [2]string{}},
			{"fraction to an integer", number, integer, []any{1.6, 2.25, -1.7}, "2,2,-2", [2]string{}},
			{"number to a boolean", integer, boolean, []any{int64(0), int64(1), int64(2), int64(-7)}, "false,true,true,true", [2]string{}},
			{"fraction to a boolean", number, boolean, []any{0.0, 0.4}, "false,true", [2]string{}},
			{"numeric text to an integer", text, integer, []any{"12", "-3"}, "12,-3", [2]string{}},
			{"boolean to an integer", boolean, integer, []any{true, false}, "1,0", [2]string{}},
			{"integer to text", integer, text, []any{int64(12)}, "12", [2]string{}},
			// The range constraint of an unsigned field is read against the new
			// type on PostgreSQL ("operator does not exist: character varying >=
			// integer") unless it goes first.
			{"unsigned to text", tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 8, Unsigned: true}, text, []any{int64(7), int64(200)}, "7,200", [2]string{}},
			{"unsigned to a wider unsigned", tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 8, Unsigned: true}, tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 32, Unsigned: true}, []any{int64(7), int64(200)}, "7,200", [2]string{}},
			// A default of the old type goes before the change on PostgreSQL, which
			// casts it on its own and refused the change ("default for column
			// cannot be cast automatically to type boolean").
			{"numeric text with a default to a boolean", text, boolean, []any{"1", "0"}, "true,false", [2]string{"'0'", "FALSE"}},
			{"integer with a default to text with another", integer, text, []any{int64(7)}, "7", [2]string{"5", "'five'"}},
			{"integer with a default to text without", integer, text, []any{int64(7)}, "7", [2]string{"5", ""}},
		} {
			t.Run(target.name+"/"+c.name, func(t *testing.T) {
				dropTables(t, target, "drifting")

				rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, driftingTable(c.from, c.defaults[0]))
				if err != nil {
					t.Fatalf("create: %v", err)
				}

				placeholder := "?"
				if target.name == "postgres" {
					placeholder = "$1"
				}

				for _, value := range c.values {
					if _, err := rt.ExecContext(context.Background(), "INSERT INTO drifting (c) VALUES ("+placeholder+")", value); err != nil {
						t.Fatalf("seed %v: %v", value, err)
					}
				}

				_ = rt.Close()

				rt, ran, err := openQuietly(target, tsq.SchemaPolicyReconcile, driftingTable(c.to, c.defaults[1]))
				if err != nil {
					t.Fatalf("reconcile: %v", err)
				}

				_ = rt.Close()

				got := columnValues(t, target, "drifting", "c")
				for i, value := range got {
					// A boolean reads as true/false on PostgreSQL and 1/0 elsewhere.
					if c.to.Kind == tsqdialect.KindBool {
						got[i] = map[string]string{"1": "true", "0": "false"}[value]
						if got[i] == "" {
							got[i] = value
						}
					}
				}

				if strings.Join(got, ",") != c.want {
					t.Fatalf("the column holds %q, want %q\nran:\n  %s", strings.Join(got, ","), c.want, strings.Join(ran, "\n  "))
				}

				// What was carried over is what the declaration says: nothing more to do.
				rt, ran, err = openQuietly(target, tsq.SchemaPolicyValidate, driftingTable(c.to, c.defaults[1]))
				if err != nil {
					t.Fatalf("validate after the change: %v", err)
				}

				_ = rt.Close()

				if len(ran) != 0 {
					t.Fatalf("validate ran DDL: %v", ran)
				}
			})
		}
	}
}

// TestIntegrationAChangedColumnIsStillAChange is the other half of asking the
// engine for its spelling: a declaration that differs from the column is altered,
// once, however the engine spells either.
func TestIntegrationAChangedColumnIsStillAChange(t *testing.T) {
	raw := func(spelled string) tsqdialect.ColumnType { return tsqdialect.ColumnType{RawType: spelled} }
	integer := tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}
	text := tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40}

	for _, target := range integrationTargets(t) {
		for _, c := range []struct {
			name           string
			from, to       tsqdialect.ColumnType
			fromDef, toDef string
			engines        string
		}{
			{"precision", raw("DECIMAL(10)"), raw("DECIMAL(12)"), "", "", "mysql,postgres"},
			{"expression default", integer, integer, "(1+1)", "(1+2)", ""},
			{"array element", raw("INT[]"), raw("BIGINT[]"), "", "", "postgres"},
			{"default added", raw("CHAR"), raw("CHAR"), "", "'x'", "mysql,postgres"},
			// '' and no default compared as one: the '' stayed after a declaration
			// dropped it, and the next type change met it.
			{"empty-string default dropped", text, text, "''", "", ""},
			{"empty-string default added", text, text, "", "''", ""},
		} {
			if c.engines != "" && !strings.Contains(c.engines, target.name) {
				continue
			}

			t.Run(target.name+"/"+c.name, func(t *testing.T) {
				dropTables(t, target, "drifting")

				// Both declarations hold NULL, so only what the case changes differs.
				declared := func(column tsqdialect.ColumnType, def string) tsq.Table {
					column.Nullable = true

					return driftingTable(column, def)
				}

				rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, declared(c.from, c.fromDef))
				if err != nil {
					t.Fatalf("create: %v", err)
				}

				_ = rt.Close()

				if rt, _, err := openQuietly(target, tsq.SchemaPolicyValidate, declared(c.to, c.toDef)); err == nil {
					_ = rt.Close()

					t.Fatal("validate took the changed declaration for the column as it is")
				}

				rt, ran, err := openQuietly(target, tsq.SchemaPolicyReconcile, declared(c.to, c.toDef))
				if err != nil {
					t.Fatalf("reconcile: %v", err)
				}

				_ = rt.Close()

				if len(ran) == 0 {
					t.Fatal("reconcile ran nothing for a changed declaration")
				}

				rt, ran, err = openQuietly(target, tsq.SchemaPolicyReconcile, declared(c.to, c.toDef))
				if err != nil {
					t.Fatalf("second reconcile: %v", err)
				}

				_ = rt.Close()

				if len(ran) != 0 {
					t.Fatalf("the change was made again on the next start: %v", ran)
				}
			})
		}
	}
}

type slugged struct {
	ID    int64
	Title string
	Slug  string
}

// sluggedTable declares a table with a title, and with a column the database
// computes from it when generated is not empty ("-" declares one whose expression
// TSQ is not told, which belongs to a migration).
func sluggedTable(generated string) *tsq.TableOf[slugged, int64] {
	h := tsq.NewTable[slugged, int64]("slugged")
	id := tsq.NewColumn(h, "id", "id", func(r *slugged) *int64 { return &r.ID })

	columns := []tsq.BoundColumn[slugged]{id, tsq.NewColumn(h, "title", "title", func(r *slugged) *string { return &r.Title })}
	specs := []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "title", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40}},
	}

	if generated != "" {
		spec := tsqdialect.ColumnSpec{Name: "slug", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 40}, Fill: tsqdialect.FillGenerated}
		if generated != "-" {
			spec.Generated = generated
		}

		columns = append(columns, tsq.NewColumn(h, "slug", "slug", func(r *slugged) *string { return &r.Slug }))
		specs = append(specs, spec)
	}

	return h.Define(tsq.TableSpec[slugged, int64]{Columns: columns, PrimaryKey: id, AutoIncrement: true, ColumnSpecs: specs})
}

// TestIntegrationAMissingGeneratedColumnIsSeen covers a generated column declared
// after its table was created. Generated columns left the schema comparison
// altogether (no two engines report one alike, and SQLite's table_info does not
// list them), so Validate passed a table without the column and the first read
// failed with "no such column". A missing one is now a column to add: Validate
// says so, CreateMissing and Reconcile add it over the rows present, and one that
// is there is still never compared.
func TestIntegrationAMissingGeneratedColumnIsSeen(t *testing.T) {
	for _, target := range integrationTargets(t) {
		for _, policy := range []tsq.SchemaPolicy{tsq.SchemaPolicyCreateMissing, tsq.SchemaPolicyReconcile} {
			t.Run(target.name+"/"+string(policy), func(t *testing.T) {
				ctx := context.Background()

				dropTables(t, target, "slugged")

				rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, sluggedTable(""))
				if err != nil {
					t.Fatalf("create: %v", err)
				}

				if _, err := rt.ExecContext(ctx, "INSERT INTO slugged (title) VALUES ('Hello'), ('WORLD')"); err != nil {
					t.Fatalf("seed: %v", err)
				}

				_ = rt.Close()

				with := sluggedTable("LOWER(title)")

				var mismatch *tsq.SchemaMismatchError
				if rt, _, err := openQuietly(target, tsq.SchemaPolicyValidate, with); !errors.As(err, &mismatch) || !strings.Contains(err.Error(), "add column slug") {
					if rt != nil {
						_ = rt.Close()
					}

					t.Fatalf("validate over a table without the generated column: %v", err)
				}

				// A column whose expression TSQ is not told cannot be added by it.
				if rt, _, err := openQuietly(target, policy, sluggedTable("-")); err == nil {
					_ = rt.Close()

					t.Fatal("a generated column with no expression was added")
				}

				rt, ran, err := openQuietly(target, policy, with)
				if err != nil {
					t.Fatalf("%s: %v", policy, err)
				}

				rows, err := with.Query().List(ctx, rt)
				if err != nil {
					t.Fatalf("read after %s: %v\nran:\n  %s", policy, err, strings.Join(ran, "\n  "))
				}

				_ = rt.Close()

				if len(rows) != 2 || rows[0].Slug != "hello" || rows[1].Slug != "world" {
					t.Fatalf("the rows present read %+v, %+v", rows[0], rows[1])
				}

				// The column is there now, and is never compared: no second change.
				for _, again := range []tsq.SchemaPolicy{tsq.SchemaPolicyValidate, tsq.SchemaPolicyReconcile} {
					rt, ran, err := openQuietly(target, again, with)
					if err != nil {
						t.Fatalf("%s after the column was added: %v", again, err)
					}

					_ = rt.Close()

					if len(ran) != 0 {
						t.Fatalf("%s ran DDL over a table that has the column: %v", again, ran)
					}
				}
			})
		}
	}
}

// TestIntegrationInstancesStartTogether covers several instances of one service
// starting at once under a policy that changes the schema: a rolling deploy, or
// replicas coming up after a release that adds a column. Each found the same thing
// missing, and all but the first failed to start: "already exists" on every
// engine, a duplicate key in PostgreSQL's own catalog for two CREATE TABLE of one
// name, "database is locked" on SQLite. They now take turns under a lock, and the
// ones that waited find nothing left to do.
func TestIntegrationInstancesStartTogether(t *testing.T) {
	const instances = 6

	for _, target := range integrationTargets(t) {
		for _, policy := range []tsq.SchemaPolicy{tsq.SchemaPolicyCreateMissing, tsq.SchemaPolicyReconcile} {
			t.Run(target.name+"/"+string(policy), func(t *testing.T) {
				start := func(what string, table tsq.Table) {
					t.Helper()

					var wg sync.WaitGroup

					errs := make([]error, instances)
					ran := make([]int, instances)
					gate := make(chan struct{})

					for i := range instances {
						wg.Go(func() {
							<-gate

							rt, statements, err := openQuietly(target, policy, table)
							if rt != nil {
								_ = rt.Close()
							}

							errs[i], ran[i] = err, len(statements)
						})
					}

					close(gate)
					wg.Wait()

					changed := 0

					for i, err := range errs {
						if err != nil {
							t.Errorf("%s: instance %d failed to start: %v", what, i, err)
						}

						if ran[i] > 0 {
							changed++
						}
					}

					if changed != 1 {
						t.Errorf("%s: %d instances changed the schema, want exactly one", what, changed)
					}
				}

				dropTables(t, target, "indexed")
				start("a database without the table", indexedTable(tsq.IndexSpec{Name: "ux_indexed_a", Columns: []string{"a"}, Unique: true}))

				// The next release declares another index: every instance sees it missing.
				start("an index added", indexedTable(
					tsq.IndexSpec{Name: "ux_indexed_a", Columns: []string{"a"}, Unique: true},
					tsq.IndexSpec{Name: "idx_indexed_b", Columns: []string{"b"}},
				))

				// A pool of one connection has none to spare for the lock.
				db, err := sql.Open(target.driver, target.dsn)
				if err != nil {
					t.Fatal(err)
				}

				defer func() { _ = db.Close() }()

				db.SetMaxOpenConns(1)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				rt, err := tsq.NewRuntime(ctx, db, targetDialect(target), []tsq.Table{indexedTable(tsq.IndexSpec{Name: "idx_indexed_b", Columns: []string{"b"}})},
					tsq.WithSchemaPolicy(policy), tsq.WithLogger(&ddlRecorder{}))
				if err != nil {
					t.Fatalf("a pool of one connection: %v", err)
				}

				_ = rt.Close()
			})
		}
	}
}

// targetDialect is the dialect of an integration target.
func targetDialect(target integrationTarget) tsqdialect.Name {
	switch target.name {
	case "mysql":
		return tsqdialect.MySQL
	case "postgres":
		return tsqdialect.Postgres
	default:
		return tsqdialect.SQLite
	}
}

type indexed struct {
	ID   int64
	A, B string
	Body string
}

func indexedTable(indexes ...tsq.IndexSpec) *tsq.TableOf[indexed, int64] {
	h := tsq.NewTable[indexed, int64]("indexed")
	id := tsq.NewColumn(h, "id", "id", func(r *indexed) *int64 { return &r.ID })
	text := func(name string, size int) tsqdialect.ColumnSpec {
		return tsqdialect.ColumnSpec{Name: name, Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: size}}
	}

	return h.Define(tsq.TableSpec[indexed, int64]{
		Columns: []tsq.BoundColumn[indexed]{
			id,
			tsq.NewColumn(h, "a", "a", func(r *indexed) *string { return &r.A }),
			tsq.NewColumn(h, "b", "b", func(r *indexed) *string { return &r.B }),
			tsq.NewColumn(h, "body", "body", func(r *indexed) *string { return &r.Body }),
		},
		PrimaryKey:    id,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			text("a", 40), text("b", 40), text("body", 400),
		},
		Indexes: indexes,
	})
}

// TestIntegrationReconcileDropsTheIndexesItNamed covers a //tsq:unique that was
// widened by a column, which changes its derived name: Reconcile created the new
// index and left the old one, which went on refusing rows the declaration allows.
// It drops an index of the table that carries a derived name (ux_<table>_...) and
// is no longer declared, and nothing else: not an index under another name, and
// not under any other policy.
func TestIntegrationReconcileDropsTheIndexesItNamed(t *testing.T) {
	ctx := context.Background()

	before := []tsq.IndexSpec{
		{Name: "ux_indexed_a", Columns: []string{"a"}, Unique: true},
		{Name: "idx_indexed_b", Columns: []string{"b"}},
	}
	after := []tsq.IndexSpec{{Name: "ux_indexed_a_b", Columns: []string{"a", "b"}, Unique: true}}

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropTables(t, target, "indexed")

			rt, _, err := openQuietly(target, tsq.SchemaPolicyReconcile, indexedTable(before...))
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			// Two indexes somebody made by hand: one under a name of their own, one
			// that only looks like TSQ's and names another table.
			for _, statement := range []string{
				"CREATE INDEX by_hand ON indexed (body)",
				"CREATE INDEX ux_other_thing ON indexed (b, a)",
			} {
				if _, err := rt.ExecContext(ctx, statement); err != nil {
					t.Fatalf("%s: %v", statement, err)
				}
			}

			_ = rt.Close()

			// Adding the new index is all CreateMissing does.
			table := indexedTable(after...)

			rt, ran, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, table)
			if err != nil {
				t.Fatalf("create missing: %v", err)
			}

			if joined := strings.Join(ran, "\n"); strings.Contains(joined, "DROP INDEX") {
				t.Fatalf("CreateMissing dropped an index:\n%s", joined)
			}

			if err := table.Insert(ctx, rt, &indexed{A: "x", B: "1"}); err != nil {
				t.Fatal(err)
			}

			if err := table.Insert(ctx, rt, &indexed{A: "x", B: "2"}); !tsq.IsDuplicateKeyError(err) {
				t.Fatalf("the old unique index is gone under CreateMissing: %v", err)
			}

			_ = rt.Close()

			rt, ran, err = openQuietly(target, tsq.SchemaPolicyReconcile, table)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}

			defer func() { _ = rt.Close() }()

			joined := strings.Join(ran, "\n")
			for _, gone := range []string{"ux_indexed_a", "idx_indexed_b"} {
				if !strings.Contains(joined, "DROP INDEX "+quoteFor(target, gone)) {
					t.Errorf("Reconcile left %s, which is no longer declared; it ran:\n%s", gone, joined)
				}
			}

			for _, kept := range []string{"by_hand", "ux_other_thing", "ux_indexed_a_b"} {
				if strings.Contains(joined, "DROP INDEX "+quoteFor(target, kept)) {
					t.Errorf("Reconcile dropped %s; it ran:\n%s", kept, joined)
				}
			}

			// What the declaration allows is now allowed: a alone no longer has to be unique.
			if err := table.Insert(ctx, rt, &indexed{A: "x", B: "2"}); err != nil {
				t.Errorf("a row the declared unique index allows: %v", err)
			}

			if err := table.Insert(ctx, rt, &indexed{A: "x", B: "2"}); !tsq.IsDuplicateKeyError(err) {
				t.Errorf("the declared unique index does not hold: %v", err)
			}

			// Nothing left to do.
			again, ran, err := openQuietly(target, tsq.SchemaPolicyReconcile, table)
			if err != nil || len(ran) != 0 {
				t.Fatalf("second reconcile: %v, ran:\n%s", err, strings.Join(ran, "\n"))
			}

			_ = again.Close()
		})
	}
}

// quoteFor quotes an identifier as the target's dialect does.
func quoteFor(target integrationTarget, name string) string {
	if target.name == "mysql" {
		return "`" + name + "`"
	}

	return `"` + name + `"`
}

// TestIntegrationFullTextIndexFollowsItsColumns covers a full-text index kept under
// one name while its field list changed. It was compared by name only, so the index
// stayed over the old columns; MySQL's MATCH needs an index over exactly the columns
// it names, and every search failed (error 1191).
func TestIntegrationFullTextIndexFollowsItsColumns(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropTables(t, target, "indexed")

			wide := indexedTable(tsq.IndexSpec{Name: "ft_docs", Columns: []string{"body", "a"}, FullText: true})

			rt, _, err := openQuietly(target, tsq.SchemaPolicyReconcile, wide)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			if err := wide.Insert(ctx, rt, &indexed{A: "x", B: "y", Body: "the quick brown fox"}); err != nil {
				t.Fatal(err)
			}

			_ = rt.Close()

			narrow := indexedTable(tsq.IndexSpec{Name: "ft_docs", Columns: []string{"body"}, FullText: true})

			rt, _, err = openQuietly(target, tsq.SchemaPolicyReconcile, narrow)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}

			defer func() { _ = rt.Close() }()

			found, err := tsq.Select(narrow.Columns()...).From(narrow).Where(tsq.Matches(narrow.FullText("ft_docs"), tsq.Val("quick"))).List(ctx, rt)
			if err != nil || len(found) != 1 {
				t.Fatalf("search over the index as declared: %d rows, %v", len(found), err)
			}

			again, ran, err := openQuietly(target, tsq.SchemaPolicyReconcile, narrow)
			if err != nil || len(ran) != 0 {
				t.Fatalf("second reconcile: %v, ran:\n%s", err, strings.Join(ran, "\n"))
			}

			_ = again.Close()
		})
	}
}

// warnings records what a runtime says at the warning level.
type warnings struct {
	mu   sync.Mutex
	said []string
}

func (w *warnings) Enabled(context.Context, slog.Level) bool { return true }

func (w *warnings) LogAttrs(_ context.Context, level slog.Level, msg string, _ ...slog.Attr) {
	if level < slog.LevelWarn {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.said = append(w.said, msg)
}

// TestIntegrationMySQLSessionModes covers what the sql_mode of a pool changed,
// each of them green under the server's default mode. Under ANSI_QUOTES the server
// writes a table's definition with double quotes, the column to ask it about was
// not found there, and every raw type and default it spells its own way was a
// drift on each start. Under NO_BACKSLASH_ESCAPES it does not read back the BINARY
// default it wrote. Outside strict mode ALTER TABLE cut the values that did not
// fit the new type of a column, where strict mode refuses the change.
func TestIntegrationMySQLSessionModes(t *testing.T) {
	var base *integrationTarget

	for _, target := range integrationTargets(t) {
		if target.name == "mysql" {
			base = &target
		}
	}

	if base == nil {
		t.Skip("TSQ_MYSQL_DSN is not set")
	}

	under := func(mode string) integrationTarget {
		target := *base
		target.dsn += "&sql_mode=" + url.QueryEscape("'"+mode+"'")

		return target
	}

	for _, mode := range []string{"ANSI,STRICT_TRANS_TABLES", "NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES"} {
		for _, c := range driftCases() {
			if c.engines != "" && !strings.Contains(c.engines, "mysql") {
				continue
			}

			t.Run(mode+"/"+c.name, func(t *testing.T) { declaredColumnDoesNotDrift(t, under(mode), c) })
		}
	}

	ctx := context.Background()
	loose := under("")
	short := tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 5}

	t.Run("a change of type that cuts a value is refused outside strict mode", func(t *testing.T) {
		dropTables(t, loose, "drifting")

		rt, _, err := openQuietly(loose, tsq.SchemaPolicyCreateMissing, driftingTable(tsqdialect.ColumnType{RawType: "TEXT"}, ""))
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		if _, err := rt.ExecContext(ctx, "INSERT INTO drifting (c) VALUES ('abcdefghij')"); err != nil {
			t.Fatalf("seed: %v", err)
		}

		_ = rt.Close()

		rt, ran, err := openQuietly(loose, tsq.SchemaPolicyReconcile, driftingTable(short, ""))
		if err == nil {
			_ = rt.Close()
		}

		if held := columnValues(t, loose, "drifting", "c"); err == nil || len(held) != 1 || held[0] != "abcdefghij" {
			t.Fatalf("reconcile = %v; the column holds %v, want the ten characters it had\nran:\n  %s", err, held, strings.Join(ran, "\n  "))
		}
	})

	t.Run("the pool keeps its mode", func(t *testing.T) {
		dropTables(t, loose, "drifting")

		db, err := sql.Open(loose.driver, loose.dsn)
		if err != nil {
			t.Fatal(err)
		}

		defer func() { _ = db.Close() }()

		// One connection: the session the policies made strict is the one asked.
		db.SetMaxOpenConns(1)

		said := &warnings{}

		if _, err := tsq.NewRuntime(ctx, db, tsqdialect.MySQL, []tsq.Table{driftingTable(short, "")},
			tsq.WithSchemaPolicy(tsq.SchemaPolicyReconcile), tsq.WithLogger(said)); err != nil {
			t.Fatalf("start: %v", err)
		}

		var mode string
		if err := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&mode); err != nil || mode != "" {
			t.Errorf("the session's sql_mode after the policies = %q, %v; want it empty, as the pool set it", mode, err)
		}

		if len(said.said) != 1 || !strings.Contains(said.said[0], "not in strict mode") {
			t.Errorf("a pool outside strict mode was told %q; want one warning that says so", said.said)
		}

		strict := &warnings{}

		rt, err := tsq.Open(ctx, base.driver, under("STRICT_TRANS_TABLES").dsn, []tsq.Table{driftingTable(short, "")}, tsq.WithLogger(strict))
		if err != nil {
			t.Fatalf("start in strict mode: %v", err)
		}

		_ = rt.Close()

		if len(strict.said) != 0 {
			t.Errorf("a pool in strict mode was warned: %q", strict.said)
		}
	})
}

type ranged struct {
	ID    int64
	Qty   uint32
	Small int16
	Big   uint64
	Plain int64
}

type rangedTable struct {
	*tsq.TableOf[ranged, int64]

	ID    tsq.Column[ranged, int64]
	Qty   tsq.Column[ranged, uint32]
	Small tsq.Column[ranged, int16]
	Big   tsq.Column[ranged, uint64]
	Plain tsq.Column[ranged, int64]
}

var rangedCols = func() rangedTable {
	h := tsq.NewTable[ranged, int64]("ranged")
	t := rangedTable{
		TableOf: h,
		ID:      tsq.NewColumn(h, "id", "id", func(r *ranged) *int64 { return &r.ID }),
		Qty:     tsq.NewColumn(h, "qty", "qty", func(r *ranged) *uint32 { return &r.Qty }),
		Small:   tsq.NewColumn(h, "small", "small", func(r *ranged) *int16 { return &r.Small }),
		Big:     tsq.NewColumn(h, "big", "big", func(r *ranged) *uint64 { return &r.Big }),
		Plain:   tsq.NewColumn(h, "plain", "plain", func(r *ranged) *int64 { return &r.Plain }),
	}

	h.Define(tsq.TableSpec[ranged, int64]{
		Columns:       []tsq.BoundColumn[ranged]{t.ID, t.Qty, t.Small, t.Big, t.Plain},
		PrimaryKey:    t.ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "qty", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 32, Unsigned: true}},
			{Name: "small", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 16}},
			{Name: "big", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64, Unsigned: true}},
			{Name: "plain", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
		},
	})

	return t
}()

// TestIntegrationIntegerColumnsKeepTheFieldsRange covers the range constraint
// an integer column carries where the engine's type is wider than the field:
// PostgreSQL has no unsigned types and SQLite's INTEGER is 64 bits whatever the
// field, so Set(t.Stock, Sub(t.Stock, qty)) below zero, or a product past the
// field's width, stored a value the field could not read back, and the row was
// unreadable from then on; MySQL's UNSIGNED and widths refused the write. A table
// from before the constraint was written is a mismatch, and Reconcile adds it.
func TestIntegrationIntegerColumnsKeepTheFieldsRange(t *testing.T) {
	ctx := context.Background()
	r := rangedCols

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropTables(t, target, "ranged")

			rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, r)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			defer func() { _ = rt.Close() }()

			row := &ranged{Qty: 3, Small: 100, Big: 5, Plain: -1}
			if err := r.Insert(ctx, rt, row); err != nil {
				t.Fatalf("insert: %v", err)
			}

			where := r.ID.EQ(tsq.Val(row.ID))

			for name, mutation := range map[string]*tsq.Mutation[ranged]{
				"unsigned below zero":      tsq.UpdateTable(r).Set(r.Qty, tsq.Sub(r.Qty, tsq.Val(uint32(5)))).Where(where).MustBuild(),
				"past 32 unsigned bits":    tsq.UpdateTable(r).Set(r.Qty, tsq.Mul(r.Qty, tsq.Val(uint32(2000000000)))).Where(where).MustBuild(),
				"past 16 signed bits":      tsq.UpdateTable(r).Set(r.Small, tsq.Add(r.Small, tsq.Val(int16(32700)))).Where(where).MustBuild(),
				"below 16 signed bits":     tsq.UpdateTable(r).Set(r.Small, tsq.Mul(r.Small, tsq.Val(int16(-400)))).Where(where).MustBuild(),
				"wide unsigned below zero": tsq.UpdateTable(r).Set(r.Big, tsq.Sub(r.Big, tsq.Val(uint64(10)))).Where(where).MustBuild(),
			} {
				if n, err := mutation.Exec(ctx, rt); err == nil {
					t.Errorf("%s: the write was taken (%d rows)", name, n)
				}
			}

			// A signed 64-bit field has the column's own range.
			if _, err := tsq.UpdateTable(r).Set(r.Plain, tsq.Sub(r.Plain, tsq.Val(int64(10)))).Where(where).MustBuild().Exec(ctx, rt); err != nil {
				t.Errorf("a signed 64-bit column refused a value in range: %v", err)
			}

			got, err := r.Get(ctx, rt, row.ID)
			if err != nil || got.Qty != 3 || got.Small != 100 || got.Big != 5 || got.Plain != -11 {
				t.Fatalf("the row after the refused writes: %+v, %v", got, err)
			}

			// A second start finds the constraint as declared.
			again, ran, err := openQuietly(target, tsq.SchemaPolicyValidate, r)
			if err != nil || len(ran) != 0 {
				t.Fatalf("validate over the table TSQ created: %v, ran %v", err, ran)
			}

			_ = again.Close()

			if target.name == "mysql" {
				return // MySQL's own types keep the range; there is no constraint to add.
			}

			// A table from before the constraint: Validate names the columns, Reconcile
			// adds them, and the next start has nothing to do.
			dropTables(t, target, "ranged")

			plain := map[string]string{
				"postgres": `CREATE TABLE ranged (id BIGSERIAL PRIMARY KEY, qty BIGINT NOT NULL, small SMALLINT NOT NULL, big NUMERIC(20) NOT NULL, plain BIGINT NOT NULL)`,
				"sqlite":   `CREATE TABLE ranged (id INTEGER PRIMARY KEY AUTOINCREMENT, qty INTEGER NOT NULL, small INTEGER NOT NULL, big INTEGER NOT NULL, plain INTEGER NOT NULL)`,
			}[target.name]

			if _, err := rt.ExecContext(ctx, plain); err != nil {
				t.Fatalf("create the table without constraints: %v", err)
			}

			if _, err := rt.ExecContext(ctx, "INSERT INTO ranged (qty, small, big, plain) VALUES (1, 2, 3, 4)"); err != nil {
				t.Fatalf("seed: %v", err)
			}

			_, _, err = openQuietly(target, tsq.SchemaPolicyValidate, r)

			var mismatch *tsq.SchemaMismatchError
			if !errors.As(err, &mismatch) || !strings.Contains(err.Error(), "qty") || strings.Contains(err.Error(), "plain") {
				t.Fatalf("validate over a table without the constraints = %v; want a mismatch naming qty, small and big", err)
			}

			fixed, ran, err := openQuietly(target, tsq.SchemaPolicyReconcile, r)
			if err != nil {
				t.Fatalf("reconcile: %v\nran:\n  %s", err, strings.Join(ran, "\n  "))
			}

			if _, err := tsq.UpdateTable(r).Set(r.Qty, tsq.Sub(r.Qty, tsq.Val(uint32(5)))).Where(r.Qty.EQ(tsq.Val(uint32(1)))).MustBuild().Exec(ctx, fixed); err == nil {
				t.Error("the reconciled table took a value below zero")
			}

			_ = fixed.Close()

			done, ran, err := openQuietly(target, tsq.SchemaPolicyValidate, r)
			if err != nil || len(ran) != 0 {
				t.Fatalf("validate after reconcile: %v, ran %v", err, ran)
			}

			_ = done.Close()

			// A row outside the range refuses the constraint, and with it the start.
			dropTables(t, target, "ranged")

			if _, err := rt.ExecContext(ctx, plain); err != nil {
				t.Fatal(err)
			}

			if _, err := rt.ExecContext(ctx, "INSERT INTO ranged (qty, small, big, plain) VALUES (-1, 2, 3, 4)"); err != nil {
				t.Fatalf("seed a value below zero: %v", err)
			}

			if refused, _, err := openQuietly(target, tsq.SchemaPolicyReconcile, r); err == nil {
				_ = refused.Close()

				t.Fatal("reconcile added a range constraint over a row outside it")
			}
		})
	}
}
