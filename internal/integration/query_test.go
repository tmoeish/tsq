package integration_test

// Queries and values whose answer must not depend on the engine, over field
// shapes the academy fixture does not have: a floating-point column, a named bool,
// times read through expressions. Each case here gave a different answer, or
// failed, on one engine while the others agreed.

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tmoeish/tsq/v5"
	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

type flag bool

type measure struct {
	ID     int64
	Label  string
	Amount float64
	Qty    int64
	At     time.Time
	Seen   *time.Time
	On     flag
	Maybe  *flag
}

type measureTable struct {
	*tsq.TableOf[measure, int64]

	ID     tsq.Column[measure, int64]
	Label  tsq.Column[measure, string]
	Amount tsq.Column[measure, float64]
	Qty    tsq.Column[measure, int64]
	At     tsq.Column[measure, time.Time]
	Seen   tsq.NullColumn[measure, time.Time]
	On     tsq.Column[measure, flag]
	Maybe  tsq.NullColumn[measure, flag]
}

var measures = func() measureTable {
	h := tsq.NewTable[measure, int64]("measures")
	t := measureTable{
		TableOf: h,
		ID:      tsq.NewColumn(h, "id", "id", func(r *measure) *int64 { return &r.ID }),
		Label:   tsq.NewColumn(h, "label", "label", func(r *measure) *string { return &r.Label }),
		Amount:  tsq.NewColumn(h, "amount", "amount", func(r *measure) *float64 { return &r.Amount }),
		Qty:     tsq.NewColumn(h, "qty", "qty", func(r *measure) *int64 { return &r.Qty }),
		At:      tsq.NewColumn(h, "at", "at", func(r *measure) *time.Time { return &r.At }),
		Seen:    tsq.NewNullColumn[time.Time](h, "seen", "seen", func(r *measure) **time.Time { return &r.Seen }),
		On:      tsq.NewColumn(h, "is_on", "is_on", func(r *measure) *flag { return &r.On }),
		Maybe:   tsq.NewNullColumn[flag](h, "maybe", "maybe", func(r *measure) **flag { return &r.Maybe }),
	}

	h.Define(tsq.TableSpec[measure, int64]{
		Columns:       []tsq.BoundColumn[measure]{t.ID, t.Label, t.Amount, t.Qty, t.At, t.Seen, t.On, t.Maybe},
		PrimaryKey:    t.ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "label", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 20}},
			{Name: "amount", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindFloat, Bits: 64}},
			{Name: "qty", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
			{Name: "at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime}},
			{Name: "seen", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime, Nullable: true}},
			{Name: "is_on", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBool}},
			{Name: "maybe", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBool, Nullable: true}},
		},
	})

	return t
}()

var measureEpoch = time.Date(2024, 1, 31, 23, 30, 0, 0, time.UTC)

// openMeasures creates the measures table with rows whose amounts sit on rounding
// ties and whose quantities do not average to a short decimal.
func openMeasures(t *testing.T, target integrationTarget) *tsq.Runtime {
	t.Helper()
	dropTables(t, target, "measures")

	rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, measures)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	yes := flag(true)

	for i, amount := range []float64{-6.5, 8.5, 2.5, -0.25, 0.125, 1e40} {
		row := &measure{Label: fmt.Sprintf("m%d", i), Amount: amount, Qty: int64(i%3 + 1), At: measureEpoch.Add(time.Duration(i) * 13 * time.Hour), On: i%2 == 0}
		if i%2 == 1 {
			seen := row.At.Add(time.Minute)
			row.Seen, row.Maybe = &seen, &yes
		}

		if err := measures.Insert(context.Background(), rt, row); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	return rt
}

// TestIntegrationRoundAndAverageAgree covers tsq.Round and tsq.Avg, whose answer
// depended on the engine: MySQL rounds a DOUBLE to the nearest even digit (-6.5 is
// -6, 8.5 is 8) where PostgreSQL and SQLite round a tie away from zero, and
// averages an integer column as a DECIMAL with four more digits (1.8333).
func TestIntegrationRoundAndAverageAgree(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			rt := openMeasures(t, target)

			for precision, want := range map[int][]float64{
				0: {-7, 9, 3, 0, 0, 1e40},
				1: {-6.5, 8.5, 2.5, -0.3, 0.1, 1e40},
				2: {-6.5, 8.5, 2.5, -0.25, 0.13, 1e40},
			} {
				rounded, err := tsq.SelectValue(tsq.Round(measures.Amount, precision)).From(measures).OrderBy(measures.ID.Asc()).List(ctx, rt)
				if err != nil {
					t.Fatalf("round to %d: %v", precision, err)
				}

				for i, got := range rounded {
					if math.Abs(*got-want[i]) > math.Abs(want[i])*1e-12 {
						t.Errorf("Round(%v, %d) = %v, want %v", []float64{-6.5, 8.5, 2.5, -0.25, 0.125, 1e40}[i], precision, *got, want[i])
					}
				}
			}

			// Quantities 1, 2, 3, 1, 2, 3 average to 2; without the last row, to 1.8.
			mean, err := tsq.SelectNullValue(tsq.Avg(measures.Qty)).From(measures).Where(measures.ID.LT(tsq.Val(int64(4)))).Get(ctx, rt)
			if err != nil || !mean.Valid || math.Abs(mean.V-2) > 1e-12 {
				t.Errorf("Avg of 1, 2, 3 = %+v, %v", mean, err)
			}

			mean, err = tsq.SelectNullValue(tsq.Avg(measures.Qty)).From(measures).Where(measures.ID.LT(tsq.Val(int64(3)))).Get(ctx, rt)
			if err != nil || mean.V != 1.5 {
				t.Errorf("Avg of 1, 2 = %+v, %v", mean, err)
			}

			third, err := tsq.SelectNullValue(tsq.Avg(measures.Qty)).From(measures).Where(measures.ID.In(tsq.Vals[int64](1, 2, 4))).Get(ctx, rt)
			if err != nil || math.Abs(third.V-4.0/3) > 1e-12 {
				t.Errorf("Avg of 1, 2, 1 = %+v, %v; want 1.3333333333333333", third, err)
			}
		})
	}
}

// TestIntegrationTimeExpressionsAreRead covers a time read through an expression.
// SQLite keeps a time as text and its drivers hand back a time.Time only for a
// column declared as one, so MAX(at), MIN(seen) and COALESCE(seen, at) failed to
// scan there ("storing driver.Value type string into type *time.Time").
func TestIntegrationTimeExpressionsAreRead(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			rt := openMeasures(t, target)

			latest, err := tsq.SelectNullValue(tsq.Max(measures.At)).From(measures).Get(ctx, rt)
			if err != nil || !latest.Valid || !latest.V.Equal(measureEpoch.Add(5*13*time.Hour)) {
				t.Errorf("Max(at) = %+v, %v", latest, err)
			}

			first, err := tsq.SelectNullValue(tsq.Min(measures.Seen)).From(measures).Get(ctx, rt)
			if err != nil || !first.Valid || !first.V.Equal(measureEpoch.Add(13*time.Hour+time.Minute)) {
				t.Errorf("Min(seen) = %+v, %v", first, err)
			}

			none, err := tsq.SelectNullValue(tsq.Max(measures.Seen)).From(measures).Where(measures.ID.EQ(tsq.Val(int64(1)))).Get(ctx, rt)
			if err != nil || none.Valid {
				t.Errorf("Max(seen) over a row that holds NULL = %+v, %v", none, err)
			}

			either, err := tsq.SelectValue(tsq.Coalesce(measures.Seen, measures.At)).From(measures).OrderBy(measures.ID.Asc()).List(ctx, rt)
			if err != nil || len(either) != 6 || !either[0].Equal(measureEpoch) || !either[1].Equal(measureEpoch.Add(13*time.Hour+time.Minute)) {
				t.Errorf("Coalesce(seen, at) = %v, %v", either, err)
			}

			type latestBy struct {
				On   flag
				Last time.Time
			}

			grouped, err := tsq.Select(
				tsq.MapInto(measures.On, func(r *latestBy) *flag { return &r.On }),
				tsq.MapInto(tsq.Max(measures.At), func(r *latestBy) *time.Time { return &r.Last }),
			).From(measures).GroupBy(measures.On).OrderBy(measures.On.Asc()).List(ctx, rt)
			if err != nil || len(grouped) != 2 || !grouped[1].Last.Equal(measureEpoch.Add(4*13*time.Hour)) {
				t.Errorf("Max(at) by group = %+v, %v", grouped, err)
			}
		})
	}
}

// TestIntegrationNamedBoolFieldsAreRead covers a field of a named bool type. MySQL
// and SQLite report a boolean as an integer, which database/sql converts for a
// bool and for nothing named after one: tsq gen took the field, and every read of
// the row failed there.
func TestIntegrationNamedBoolFieldsAreRead(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			rt := openMeasures(t, target)

			rows, err := tsq.Select(measures.Columns()...).From(measures).OrderBy(measures.ID.Asc()).List(ctx, rt)
			if err != nil || len(rows) != 6 {
				t.Fatalf("rows with a named bool: %d, %v", len(rows), err)
			}

			if !rows[0].On || rows[1].On || rows[0].Maybe != nil || rows[1].Maybe == nil || !*rows[1].Maybe {
				t.Errorf("named bools read back wrong: %+v, %+v", *rows[0], *rows[1])
			}

			on, err := tsq.SelectValue(measures.ID).From(measures).Where(measures.On.EQ(tsq.Val(flag(true))), measures.Maybe.IsNull()).Count(ctx, rt)
			if err != nil || on != 3 {
				t.Errorf("rows that are on = %d, %v", on, err)
			}

			maybe, err := tsq.SelectNullValue(measures.Maybe).From(measures).OrderBy(measures.ID.Asc()).Limit(2).List(ctx, rt)
			if err != nil || len(maybe) != 2 || maybe[0].Valid || !maybe[1].Valid || !bool(maybe[1].V) {
				t.Errorf("a nullable named bool as sql.Null = %+v, %v", maybe, err)
			}
		})
	}
}

// TestIntegrationDistinctOrdersADerivedColumnWithNullsLast covers MySQL's NULL
// placement key ("expr IS NULL") in a DISTINCT query: the key is not in the select
// list, which MySQL refuses for an expression (error 3065). The key tests the
// selected expression through its alias.
func TestIntegrationDistinctOrdersADerivedColumnWithNullsLast(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			rt := openMeasures(t, target)

			size := tsq.Case(measures.Amount.GT(tsq.Val(5.0)), tsq.Val("big")).When(measures.Amount.LT(tsq.Val(-5.0)), tsq.Val("neg")).End()

			sizes, err := tsq.SelectDistinct(tsq.MapIntoNull(size, func(v *sql.Null[string]) *sql.Null[string] { return v })).
				From(measures).OrderBy(size.Asc().NullsLast()).List(ctx, rt)
			if err != nil {
				t.Fatalf("distinct ordered with NULLs last: %v", err)
			}

			var got []string

			for _, s := range sizes {
				if s.Valid {
					got = append(got, s.V)
				} else {
					got = append(got, "NULL")
				}
			}

			if strings.Join(got, ",") != "big,neg,NULL" {
				t.Errorf("sizes = %v, want big, neg, NULL", got)
			}
		})
	}
}

// TestIntegrationASplitListInKeepsTheRowsAJoinRepeats covers ListIn over a list
// long enough to be split, on a query that joins: the parts were deduplicated by
// primary key, which also dropped the rows the join repeats, so the same query
// returned fewer rows for a long list than for a short one.
func TestIntegrationASplitListInKeepsTheRowsAJoinRepeats(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			rt := openMeasures(t, target)

			other := measures.As("o")
			labels := tsq.NewListParam[string]("labels")
			// Every measure joins to the three that share its quantity's parity or
			// not; what matters is that a row comes back more than once.
			q := tsq.Select(measures.Columns()...).From(measures).
				InnerJoin(other, measures.Qty.WithTable(other).EQ(measures.Qty)).
				Where(measures.Label.In(labels)).MustBuild()

			short := []string{"m0", "m1"}

			long := make([]string, 0, 70002)
			long = append(long, short...)

			for i := range 70000 {
				long = append(long, fmt.Sprintf("x%d", i))
			}

			few, err := q.ListIn(ctx, rt, labels, short)
			if err != nil {
				t.Fatal(err)
			}

			many, err := q.ListIn(ctx, rt, labels, long)
			if err != nil {
				t.Fatal(err)
			}

			if len(few) != 4 || len(many) != len(few) {
				t.Errorf("the same two rows matched: %d rows for a short list, %d once the list is split; want 4 and 4", len(few), len(many))
			}
		})
	}
}

// TestIntegrationZeroTimeIsStoredOnEveryEngine covers a NOT NULL time field left
// at its zero value. The MySQL driver writes it as '0000-00-00', which MySQL
// refuses in its default mode, where the other engines store year 1.
func TestIntegrationZeroTimeIsStoredOnEveryEngine(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			rt := openMeasures(t, target)

			row := &measure{Label: "unset"}
			if err := measures.Insert(ctx, rt, row); err != nil {
				t.Fatalf("insert a row whose time was never set: %v", err)
			}

			got, err := measures.Get(ctx, rt, row.ID)
			if err != nil || !got.At.IsZero() {
				t.Fatalf("read back = %+v, %v; want the zero time", got, err)
			}

			found, err := tsq.SelectValue(measures.ID).From(measures).Where(measures.At.EQ(tsq.Val(time.Time{}))).Count(ctx, rt)
			if err != nil || found != 1 {
				t.Errorf("rows at the zero time = %d, %v", found, err)
			}
		})
	}
}

// TestIntegrationOpenRefusesAMySQLLocOtherThanUTC covers the go-sql-driver loc
// parameter, "loc=Local" above all. The driver writes a time in loc and reads a
// DATETIME as one in loc: TSQ's stamps were then stored in local time where the
// documentation says UTC, and a time the database filled (in UTC) was read back
// hours off, without an error.
func TestIntegrationOpenRefusesAMySQLLocOtherThanUTC(t *testing.T) {
	for _, target := range integrationTargets(t) {
		if target.name != "mysql" {
			continue
		}

		for _, loc := range []string{"&loc=Local", "&loc=Asia%2FTokyo"} {
			rt, err := tsq.Open(context.Background(), target.driver, target.dsn+loc, nil)
			if err == nil {
				_ = rt.Close()

				t.Fatalf("Open took a DSN with %s", loc)
			}

			if !strings.Contains(err.Error(), "loc=") {
				t.Fatalf("Open(%s) = %v; want it to name loc", loc, err)
			}
		}

		rt, err := tsq.Open(context.Background(), target.driver, target.dsn+"&loc=UTC", nil)
		if err != nil {
			t.Fatalf("Open refused loc=UTC: %v", err)
		}

		_ = rt.Close()

		// A pool handed to NewRuntime has no DSN to read: the driver is asked how
		// it reads a time, and the same two settings are refused.
		for dsn, want := range map[string]string{
			target.dsn + "&loc=Local": "loc=",
			strings.NewReplacer("parseTime=true&", "", "?parseTime=true", "?", "&parseTime=true", "").Replace(target.dsn): "parseTime",
		} {
			db, err := sql.Open(target.driver, dsn)
			if err != nil {
				t.Fatal(err)
			}

			rt, err := tsq.NewRuntime(context.Background(), db, tsqdialect.MySQL, nil)
			if err == nil {
				_ = rt.Close()
				_ = db.Close()

				t.Fatalf("NewRuntime took a pool opened with %s", dsn)
			}

			_ = db.Close()

			if !strings.Contains(err.Error(), want) {
				t.Fatalf("NewRuntime over %s = %v; want it to name %s", dsn, err, want)
			}
		}

		db, err := sql.Open(target.driver, target.dsn)
		if err != nil {
			t.Fatal(err)
		}

		rt, err = tsq.NewRuntime(context.Background(), db, tsqdialect.MySQL, nil)
		if err != nil {
			t.Fatalf("NewRuntime refused a pool opened as Open requires: %v", err)
		}

		_ = rt.Close()
		_ = db.Close()
	}
}

// TestIntegrationFunctionsEveryEngineHas covers three spellings that worked on
// some engines only. CEIL and FLOOR are missing from a SQLite built without its
// math functions (the default of mattn/go-sqlite3), PostgreSQL has no MAX or MIN
// over a boolean, and MySQL knows a grouped expression only where it stands whole
// in the select list or ORDER BY: HAVING UPPER(label) and a selected
// LOWER(UPPER(label)) are errors 1054 and 1055 there.
func TestIntegrationFunctionsEveryEngineHas(t *testing.T) {
	ctx := context.Background()

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			rt := openMeasures(t, target)

			// The amounts are -6.5, 8.5, 2.5, -0.25, 0.125 and 1e40.
			small := measures.Amount.LT(tsq.Val(1e30))

			up, err := tsq.SelectValue(tsq.Ceil(measures.Amount)).From(measures).Where(small).OrderBy(measures.ID.Asc()).List(ctx, rt)
			if err != nil {
				t.Fatalf("Ceil: %v", err)
			}

			down, err := tsq.SelectValue(tsq.Floor(measures.Amount)).From(measures).Where(small).OrderBy(measures.ID.Asc()).List(ctx, rt)
			if err != nil {
				t.Fatalf("Floor: %v", err)
			}

			for i, want := range [][2]float64{{-6, -7}, {9, 8}, {3, 2}, {0, -1}, {1, 0}} {
				if *up[i] != want[0] || *down[i] != want[1] {
					t.Errorf("row %d: ceil %v floor %v, want %v", i, *up[i], *down[i], want)
				}
			}

			// On is true for the even rows; Maybe is true for the odd rows and NULL elsewhere.
			for name, c := range map[string]struct {
				query tsq.QueryStage[sql.Null[flag]]
				want  sql.Null[flag]
			}{
				"max of a boolean":      {tsq.SelectNullValue(tsq.Max(measures.On)).From(measures), sql.Null[flag]{V: true, Valid: true}},
				"min of a boolean":      {tsq.SelectNullValue(tsq.Min(measures.On)).From(measures), sql.Null[flag]{V: false, Valid: true}},
				"min of all true":       {tsq.SelectNullValue(tsq.Min(measures.Maybe)).From(measures), sql.Null[flag]{V: true, Valid: true}},
				"max over no rows":      {tsq.SelectNullValue(tsq.Max(measures.On)).From(measures).Where(measures.ID.LT(tsq.Val(int64(0)))), sql.Null[flag]{}},
				"max of only NULLs":     {tsq.SelectNullValue(tsq.Max(measures.Maybe)).From(measures).Where(measures.Maybe.IsNull()), sql.Null[flag]{}},
				"min of the false rows": {tsq.SelectNullValue(tsq.Min(measures.On)).From(measures).Where(measures.On.EQ(tsq.Val(flag(true)))), sql.Null[flag]{V: true, Valid: true}},
			} {
				got, err := c.query.Get(ctx, rt)
				if err != nil || *got != c.want {
					t.Errorf("%s = %+v, %v; want %+v", name, got, err, c.want)
				}
			}

			// Grouped by an expression, and that expression used again.
			initial := tsq.Upper(tsq.Substring(measures.Label, 1, 1))
			byQty := tsq.Add(measures.Qty, tsq.Val(int64(10)))

			labels, err := tsq.SelectValue(tsq.Lower(initial)).From(measures).GroupBy(initial).Having(initial.NE(tsq.Val("X")), tsq.Length(initial).EQ(tsq.Val(int64(1)))).List(ctx, rt)
			if err != nil || len(labels) != 1 || *labels[0] != "m" {
				t.Errorf("nested select and HAVING over a grouped expression: %v, %v", labels, err)
			}

			counts, err := tsq.SelectValue(tsq.Count(measures.ID)).From(measures).GroupBy(byQty).
				Having(byQty.GT(tsq.Val(int64(11))), tsq.Count(measures.ID).GT(tsq.Val(int64(0)))).
				OrderBy(tsq.Mul(byQty, tsq.Val(int64(-1))).Asc()).List(ctx, rt)
			if err != nil || len(counts) != 2 || *counts[0] != 2 || *counts[1] != 2 {
				t.Errorf("HAVING and ORDER BY over a grouped expression with a bound value: %v, %v", counts, err)
			}

			// A condition that mixes the grouped expression with an aggregate, and an
			// aggregate written by hand over it, which must stay as written.
			mixed, err := tsq.SelectValue(tsq.Count(measures.ID)).From(measures).GroupBy(initial).
				Having(tsq.Or(initial.EQ(tsq.Val("X")), tsq.Count(measures.ID).GT(tsq.Val(int64(1))))).List(ctx, rt)
			if err != nil || len(mixed) != 1 || *mixed[0] != 6 {
				t.Errorf("HAVING that mixes a grouped expression with an aggregate: %v, %v", mixed, err)
			}

			byHand, err := tsq.SelectValue(initial.Expr("MIN(%s)")).From(measures).GroupBy(initial).List(ctx, rt)
			if err != nil || len(byHand) != 1 || *byHand[0] != "M" {
				t.Errorf("an aggregate written by hand over the grouped expression: %v, %v", byHand, err)
			}

			// An aggregate over the grouped expression is valid as it is, and stays so.
			inside, err := tsq.SelectValue(tsq.Count(initial)).From(measures).GroupBy(initial).Having(tsq.Count(initial).GT(tsq.Val(int64(1)))).List(ctx, rt)
			if err != nil || len(inside) != 1 || *inside[0] != 6 {
				t.Errorf("an aggregate over the grouped expression: %v, %v", inside, err)
			}
		})
	}
}
