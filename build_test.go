package tsq

import (
	"context"
	"strings"
	"testing"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

func TestArgumentsAreMatchedByParameter(t *testing.T) {
	low, high := NewParam[int64]("low"), NewParam[int64]("high")
	q := Select(User_ID).From(Users).Where(User_Version.Between(low, high)).MustBuild()

	// Order of the arguments does not matter; identity does.
	_, args := sqlOf(t, q, onSQLite, high.Bind(9), low.Bind(1))
	if args[0] != int64(1) || args[1] != int64(9) {
		t.Fatalf("args = %v, want [1 9]", args)
	}

	tests := map[string]struct {
		args []Arg
		want string
	}{
		"missing":    {args: []Arg{low.Bind(1)}, want: "high has no value"},
		"unused":     {args: []Arg{low.Bind(1), high.Bind(2), User_Name.Bind("x")}, want: "users.name is not used"},
		"duplicated": {args: []Arg{low.Bind(1), low.Bind(2), high.Bind(3)}, want: "low is bound more than once"},
		"null":       {args: []Arg{low.Bind(1), NewParam[*int64]("p").Bind(nil)}, want: "NULL"},
		"list":       {args: []Arg{low.Bind(1), high.Bind(2), User_Version.BindList(1)}, want: "not used"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := q.SQL(onSQLite, tt.args...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestColumnParametersSurviveRebinding(t *testing.T) {
	alias := User_ID.WithTable(Users.As("u2"))
	q := Select(User_ID).From(Users).
		InnerJoin(Users.As("u2"), alias.EQ(User_ID)).
		Where(alias.EQ(alias.Param())).
		MustBuild()

	// The alias shares the column's parameter, so the column binds it.
	if _, _, err := q.SQL(onSQLite, User_ID.Bind(7)); err != nil {
		t.Fatalf("SQL() error = %v", err)
	}
}

func TestBuildRejectsInvalidStructure(t *testing.T) {
	tests := map[string]struct {
		stage interface{ Build() (*Query[user], error) }
		want  string
	}{
		"table not in query": {
			stage: Select(User_ID).From(Users).Where(Order_Amount.GT(Val(int64(1)))),
			want:  "orders is referenced",
		},
		"join without reference": {
			stage: Select(User_ID).From(Users).InnerJoin(Orders, User_Name.EQ(Val("x"))),
			want:  "must reference orders",
		},
		"join twice": {
			stage: Select(User_ID).From(Users).InnerJoin(Orders, Order_UserID.EQ(User_ID)).InnerJoin(Orders, Order_UserID.EQ(User_ID)),
			want:  "already in the query",
		},
		"correlate shadows": {
			stage: Select(User_ID).From(Users).Correlate(Users),
			want:  "shadow",
		},
		"set operand ordered": {
			stage: Select(User_ID).From(Users).Union(Select(User_ID).From(Users).OrderBy(User_ID.Desc()).Limit(3)),
			want:  "operands cannot order, limit or lock",
		},
		"set operand locked": {
			stage: Select(User_ID).From(Users).UnionAll(Select(User_ID).From(Users).ForUpdate()),
			want:  "operands cannot order, limit or lock",
		},
		"set operand width": {
			stage: Select(User_ID).From(Users).Union(Select(User_ID, User_Name).From(Users)),
			want:  "matching select column counts",
		},
		"no columns": {
			stage: Select[user]().From(Users),
			want:  "selects no columns",
		},
		"bad expression": {
			stage: Select(User_ID).From(Users).Where(User_Name.Pred("%s = %d", 1)),
			want:  "%d",
		},
		"nil value": {
			stage: Select(User_ID).From(Users).Where(User_Name.Pred("%s = %s", nil)),
			want:  "IsNull",
		},
		"undefined table": {
			stage: Select(undefinedColumn).From(undefinedTable),
			want:  "before Define",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := tt.stage.Build()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Build() error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

var (
	undefinedTable  = NewTable[user, int64]("never_defined")
	undefinedColumn = NewColumn(undefinedTable, "id", "id", func(r *user) *int64 { return &r.ID })
)

func TestPhaseChecksCatchAssertedStages(t *testing.T) {
	grouped := Select(User_ID).From(Users).GroupBy(User_ID)

	// The GroupedStage interface has no Where; asserting past it must not work either.
	where, ok := grouped.(interface {
		Where(Condition, ...Condition) FilteredStage[user]
	})
	if ok {
		if _, err := where.Where(User_ID.EQ(Val(int64(1)))).Build(); err == nil {
			t.Fatal("expected Where after GroupBy to fail")
		}
	}

	again, ok := grouped.(interface {
		GroupBy(SQLColumn, ...SQLColumn) GroupedStage[user]
	})
	if !ok {
		t.Fatal("the builder implements GroupBy")
	}

	if _, err := again.GroupBy(User_Name).Build(); err == nil {
		t.Fatal("expected a second GroupBy to fail")
	}
}

func TestDefineReportsInvalidTables(t *testing.T) {
	// Each case declares its own row type: a row type describes one table.
	tests := map[string]func() error{
		"no primary key": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("t1")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })

			return h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{id}}).Err()
		},
		"primary key not listed": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("t2")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })
			other := NewColumn(h, "other", "other", func(r *row) *int64 { return &r.Other })

			return h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{other}, PrimaryKey: id}).Err()
		},
		"foreign column": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("t3")
			h2 := NewTable[row, int64]("t4")
			id := NewColumn(h2, "id", "id", func(r *row) *int64 { return &r.ID })

			return h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{id}, PrimaryKey: id}).Err()
		},
		"unknown index field": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("t5")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })

			return h.Define(TableSpec[row, int64]{
				Columns:    []BoundColumn[row]{id},
				PrimaryKey: id,
				Indexes:    []IndexSpec{{Name: "idx_t5_x", Columns: []string{"x"}}},
			}).Err()
		},
		"defined twice": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("t6")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })
			spec := TableSpec[row, int64]{Columns: []BoundColumn[row]{id}, PrimaryKey: id}
			h.Define(spec)

			return h.Define(spec).Err()
		},
		"key is the version": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("t7")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })

			err := h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{id}, PrimaryKey: id, Version: id}).Err()
			if err != nil && !strings.Contains(err.Error(), "both the primary key and the version column") {
				t.Errorf("key is the version: %v", err)
			}

			return err
		},
		"schema disagrees on the key": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("t8")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })
			other := NewColumn(h, "other", "other", func(r *row) *int64 { return &r.Other })

			return h.Define(TableSpec[row, int64]{
				Columns: []BoundColumn[row]{id, other}, PrimaryKey: id, AutoIncrement: true,
				ColumnSpecs: []tsqdialect.ColumnSpec{
					{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true},
					{Name: "other", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
				},
			}).Err()
		},
		"schema misses a column": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("t9")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })
			other := NewColumn(h, "other", "other", func(r *row) *int64 { return &r.Other })

			return h.Define(TableSpec[row, int64]{
				Columns: []BoundColumn[row]{id, other}, PrimaryKey: id,
				ColumnSpecs: []tsqdialect.ColumnSpec{
					{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true},
				},
			}).Err()
		},
		"row type of another table": func() error {
			type row struct{ ID, Other int64 }

			for _, name := range []string{"t10", "t11"} {
				h := NewTable[row, int64](name)
				id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })

				if err := h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{id}, PrimaryKey: id}).Err(); err != nil {
					if !strings.Contains(err.Error(), "already describes table t10") {
						t.Errorf("second table: %v", err)
					}

					return err
				}
			}

			return nil
		},
		"bad name": func() error {
			type row struct{ ID, Other int64 }

			h := NewTable[row, int64]("bad name")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })

			return h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{id}, PrimaryKey: id}).Err()
		},
	}

	for name, define := range tests {
		t.Run(name, func(t *testing.T) {
			if err := define(); err == nil {
				t.Fatal("expected Define to report an error")
			}
		})
	}

	if err := Users.Err(); err != nil {
		t.Fatalf("fixture table is invalid: %v", err)
	}
}

func TestRebindRequiresTheColumnOnTheTarget(t *testing.T) {
	q := Select(User_ID).From(Users).InnerJoin(Orders, Order_UserID.EQ(User_ID)).Where(Order_Note.WithTable(Users).IsNull())
	if _, err := q.Build(); err == nil || !strings.Contains(err.Error(), "does not exist on users") {
		t.Fatalf("Build() error = %v", err)
	}

	// A derived expression has no WithTable to call; rebind the column first.
	rebound := Select(User_ID).From(Users).InnerJoin(Users.As("u"), User_ID.EQ(User_ID.WithTable(Users.As("u")))).
		Where(Upper(User_Name.WithTable(Users.As("u"))).IsNull())
	if _, err := rebound.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
}

// TestStagesAreSubqueries covers the stage used directly as a value: its build
// errors surface in the outer Build, and a value must be one column.
func TestStagesAreSubqueries(t *testing.T) {
	ids := SelectValue(Order_UserID).From(Orders).Where(Order_Amount.GT(Val(int64(10))))
	if _, err := Select(User_ID).From(Users).Where(User_ID.In(ids), User_ID.EQ(SelectValue(Max(Order_UserID)).From(Orders))).Build(); err != nil {
		t.Fatalf("stage as IN and scalar subquery: %v", err)
	}

	broken := SelectValue(Order_UserID).From(Users) // the column is not in the query
	if _, err := Select(User_ID).From(Users).Where(User_ID.In(broken)).Build(); err == nil {
		t.Fatal("expected the subquery's build error to fail the outer Build")
	}

	var built Subquery[int64] = SelectValue(Order_UserID).From(Orders).MustBuild()
	if _, err := Select(User_ID).From(Users).Where(User_ID.In(built)).Build(); err != nil {
		t.Fatalf("a built *Query is a subquery too: %v", err)
	}

	// Any stage goes to Exists, whatever it selects.
	if _, err := Select(User_ID).From(Users).Where(Exists(Select(Order_ID, Order_Amount).From(Orders))).Build(); err != nil {
		t.Fatalf("Exists over a two-column stage: %v", err)
	}
}

// TestBuildChecksGrouping covers a grouped query reading a column neither grouped
// nor aggregated: it built, and SQLite returned an arbitrary row's value while
// PostgreSQL refused it at execution.
func TestBuildChecksGrouping(t *testing.T) {
	count := MapInto(Count(Order_ID), func(r *order) *int64 { return &r.Amount })

	refused := map[string]interface{ Build() (*Query[order], error) }{
		"ungrouped column":             Select(Order_UserID, Order_Note, count).From(Orders).GroupBy(Order_UserID),
		"column beside an aggregate":   Select(Order_Note, count).From(Orders),
		"order by an ungrouped column": Select(Order_UserID, count).From(Orders).GroupBy(Order_UserID).OrderBy(Order_Note.Asc()),
	}

	for name, stage := range refused {
		if _, err := stage.Build(); err == nil || !strings.Contains(err.Error(), "neither in GROUP BY nor inside an aggregate") {
			t.Errorf("%s: Build = %v", name, err)
		}
	}

	upper := MapInto(Upper(Order_Note), func(r *order) *string { return &r.Note })

	allowed := map[string]interface{ Build() (*Query[order], error) }{
		"grouped column":          Select(Order_UserID, count).From(Orders).GroupBy(Order_UserID),
		"grouped expression":      Select(upper, count).From(Orders).GroupBy(Upper(Order_Note)),
		"column of a grouped key": Select(Order_ID, Order_Note, count).From(Orders).GroupBy(Order_ID),
		"aggregate alone":         Select(count).From(Orders),
	}

	for name, stage := range allowed {
		if _, err := stage.Build(); err != nil {
			t.Errorf("%s: Build = %v", name, err)
		}
	}
}

// TestBuiltQueriesComposeLikeStages covers a built *Query as a set-operation
// operand and a CTE body, which took only stages: a query built once could not be
// reused in either.
func TestBuiltQueriesComposeLikeStages(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b")

	first := Select(User_ID).From(Users).Where(User_Name.EQ(Val("a"))).MustBuild()
	second := Select(User_ID).From(Users).Where(User_Name.EQ(Val("b"))).MustBuild()

	rows, err := Select(User_ID).From(Users).Where(User_Name.EQ(Val("a"))).Union(second).MustBuild().List(ctx, rt)
	if err != nil || len(rows) != 2 {
		t.Fatalf("union with a built query = %d rows, %v", len(rows), err)
	}

	cte := CTE("picked", first)
	picked, err := Select(User_ID.WithTable(cte)).From(cte).MustBuild().List(ctx, rt)
	if err != nil || len(picked) != 1 {
		t.Fatalf("cte over a built query = %d rows, %v", len(picked), err)
	}

	var missing *Query[user]
	if _, err := Select(User_ID).From(Users).Union(missing).Build(); err == nil {
		t.Fatal("union with a nil query: want an error")
	}
}

// TestSetOperationsOrderByTheColumnNamed covers a set operation ordered by a
// column the query does not select itself, which is found by its output name: a
// derived item is named after its source column, so ordering by name sorted by
// LENGTH(name).
func TestSetOperationsOrderByTheColumnNamed(t *testing.T) {
	id := MapInto(User_ID, func(r *namedRow) *int64 { return &r.ID })
	length := MapInto(Length(User_Name), func(r *namedRow) *int64 { return &r.ID })
	name := MapInto(User_Name, func(r *namedRow) *string { return &r.Name })

	derived := Select(id, length).From(Users).Union(Select(id, length).From(Users))
	if _, err := derived.OrderBy(User_Name.Desc()).Build(); err == nil || !strings.Contains(err.Error(), "LENGTH") {
		t.Fatalf("ordered by name over LENGTH(name) AS name: Build = %v; want it refused", err)
	}

	plain := Select(id, name).From(Users).Union(Select(id, name).From(Users))
	if _, err := plain.OrderBy(User_Name.Desc()).Build(); err != nil {
		t.Fatalf("ordered by name over the name column: %v", err)
	}
}

// TestBuildRefusesWhatEveryDialectRefuses covers query shapes that built and then
// failed on every dialect, or on PostgreSQL alone, when they ran.
func TestBuildRefusesWhatEveryDialectRefuses(t *testing.T) {
	count := MapInto(Count(User_ID), func(r *user) *int64 { return &r.Version })

	for name, tc := range map[string]struct {
		stage QueryStage[user]
		want  string
	}{
		"aggregate in WHERE":        {Select(User_ID).From(Users).Where(Count(User_ID).GT(Val(int64(1)))), "WHERE cannot use an aggregate"},
		"aggregate in a join":       {Select(User_ID).From(Users).InnerJoin(Orders, Order_UserID.EQ(User_ID), Count(Order_ID).GT(Val(int64(1)))), "cannot use an aggregate"},
		"aggregate in GROUP BY":     {Select(count).From(Users).GroupBy(Count(User_ID)), "GROUP BY cannot use an aggregate"},
		"ordered by an aggregate":   {Select(User_ID).From(Users).OrderBy(Count(User_ID).Asc()), "neither in GROUP BY nor inside an aggregate"},
		"nested aggregates":         {Select(MapInto(Sum(Max(User_Version)), func(r *user) *int64 { return &r.Version })).From(Users), "cannot aggregate an aggregate"},
		"DISTINCT ordered by other": {SelectDistinct(User_Name).From(Users).OrderBy(User_ID.Asc()), "which it does not select"},
		"lock over an outer join":   {Select(User_ID).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID)).ForUpdate(), "row lock cannot cover the LEFT JOIN"},
		"correlated second operand": {Select(User_ID).From(Users).Where(Exists(Select(Order_ID).From(Orders).Union(Select(Order_ID).From(Orders.As("o2")).Correlate(Notes).Where(Order_ID.WithTable(Orders.As("o2")).EQ(Note_ID))))), "Correlate"},
	} {
		if _, err := tc.stage.Build(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Build = %v; want %q", name, err, tc.want)
		}
	}

	if _, err := SelectDistinct(User_Name).From(Users).OrderBy(User_Name.Asc()).Build(); err != nil {
		t.Errorf("DISTINCT ordered by what it selects: %v", err)
	}

	// A soft-delete table under a RIGHT JOIN is a derived table of its live rows,
	// and PostgreSQL infers nothing from a derived table's key.
	byKey := Select(MapInto(User_Name, func(r *order) *string { return &r.Note })).From(Orders).RightJoin(Users, Order_UserID.EQ(User_ID)).GroupBy(User_ID)
	if _, err := byKey.Build(); err == nil || !strings.Contains(err.Error(), "neither in GROUP BY") {
		t.Errorf("grouped by the key of a derived table: Build = %v", err)
	}

	// A correlated subquery may name the enclosing query's table in a JOIN's ON.
	correlated := Select(Order_ID).From(Orders).Correlate(Users).InnerJoin(Notes, Note_ID.EQ(User_ID)).Where(Order_UserID.EQ(User_ID))
	if _, err := Select(User_ID).From(Users).Where(Exists(correlated)).Build(); err != nil {
		t.Errorf("an ON condition on the enclosing query's table: %v", err)
	}
}

// TestGroupedExpressionsWithValuesRunOnPostgres covers a GROUP BY or ORDER BY
// expression with bound values: PostgreSQL numbers each placeholder anew, so the
// selected CASE WHEN x > $1 and the grouped CASE WHEN x > $4 differed and it
// refused the query. A selected expression is grouped and ordered by position.
// A CASE of bound values only is cast on PostgreSQL, which would type it as text.
func TestGroupedExpressionsWithValuesRunOnPostgres(t *testing.T) {
	size := Case(Order_Amount.GT(Val(int64(8))), Val(int64(10))).Else(Val(int64(9))).End()
	q := Select(
		MapInto(size, func(r *order) *int64 { return &r.Amount }),
		MapInto(Count(Order_ID), func(r *order) *int64 { return &r.ID }),
	).From(Orders).GroupBy(size).OrderBy(size.Asc()).MustBuild()

	sql, _ := sqlOf(t, q, onPostgres)
	if !strings.Contains(sql, "GROUP BY 1 ORDER BY 1 ASC") || !strings.Contains(sql, "THEN CAST($2 AS BIGINT) ELSE CAST($3 AS BIGINT)") {
		t.Fatalf("postgres = %s", sql)
	}

	if sql, _ := sqlOf(t, q, onSQLite); strings.Contains(sql, "CAST(") {
		t.Fatalf("sqlite casts the CASE: %s", sql)
	}

	ctx := context.Background()
	rt := newSQLite(t)
	user := seedUsers(t, rt, "a")[0]

	if err := Orders.BatchInsert(ctx, rt, []*order{{UserID: user.ID, Amount: 5}, {UserID: user.ID, Amount: 20}, {UserID: user.ID, Amount: 30}}); err != nil {
		t.Fatal(err)
	}

	rows, err := q.List(ctx, rt)
	if err != nil || len(rows) != 2 || rows[0].Amount != 9 || rows[0].ID != 1 || rows[1].ID != 2 {
		t.Fatalf("grouped rows = %+v, %v", rows, err)
	}
}

// TestPageOrdersAsTheBuilderWould covers Paging.OrderBy, which skipped the checks
// Build runs on the builder's OrderBy: a grouped page ordered by an ungrouped
// column ran on SQLite with an arbitrary row's value, and a broken expression
// rendered ORDER BY  ASC.
func TestPageOrdersAsTheBuilderWould(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	grouped := Select(MapInto(Count(Order_ID), func(r *order) *int64 { return &r.ID })).From(Orders).GroupBy(Order_UserID).MustBuild()

	for name, tc := range map[string]struct {
		ob   OrderBy
		want string
	}{
		"ungrouped column": {Order_Amount.Asc(), "neither in GROUP BY nor inside an aggregate"},
		"broken term":      {Round(Order_Amount, -1).Asc(), "precision cannot be negative"},
	} {
		if _, err := grouped.Page(ctx, rt, Paging{OrderBy: []OrderBy{tc.ob}}); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Page = %v; want %q", name, err, tc.want)
		}
	}
}

// TestZeroValuesReportErrors covers zero values of exported types, which panicked
// with a nil dereference where the rest of the builder reports an error.
func TestZeroValuesReportErrors(t *testing.T) {
	ctx := context.Background()

	var (
		q     Query[user]
		m     Mutation[user]
		stage UpdateStage[user]
		table TableOf[user, int64]
	)

	if _, _, err := q.SQL(onSQLite); err == nil || !strings.Contains(err.Error(), "not built") {
		t.Errorf("zero Query SQL = %v", err)
	}

	if _, err := q.List(ctx, newSQLite(t)); err == nil {
		t.Error("zero Query List: want an error")
	}

	if _, _, err := m.SQL(onSQLite); err == nil {
		t.Error("zero Mutation SQL: want an error")
	}

	if _, err := stage.Set(User_Name, Val("x")).Where(And()).Build(); err == nil || !strings.Contains(err.Error(), "UpdateTable") {
		t.Errorf("zero UpdateStage = %v", err)
	}

	if err := table.Err(); err == nil || table.TableName() != "" {
		t.Errorf("zero TableOf Err = %v", err)
	}

	if _, err := Select(User_ID).From(Users).Search(Searchable(Column[user, string](nil))).Build(); err == nil {
		t.Error("Searchable(nil): want an error")
	}
}

// TestDefineChecksManagedColumnTypes covers managed columns TSQ cannot write, which
// Define accepted and the first write refused: a bool created_at, a time.Time
// tombstone (every row then read as deleted), an int16 tombstone (UnixNano
// truncated, sometimes to zero), and an index both unique and full-text.
func TestDefineChecksManagedColumnTypes(t *testing.T) {
	type stamped struct {
		ID        int64
		CreatedAt bool
	}

	h1 := NewTable[stamped, int64]("t_stamped")
	id1 := NewColumn(h1, "id", "id", func(r *stamped) *int64 { return &r.ID })
	at := NewColumn(h1, "created_at", "created_at", func(r *stamped) *bool { return &r.CreatedAt })

	if err := h1.Define(TableSpec[stamped, int64]{Columns: []BoundColumn[stamped]{id1, at}, PrimaryKey: id1, CreatedAt: at}).Err(); err == nil || !strings.Contains(err.Error(), "must be a time") {
		t.Errorf("bool created_at: %v", err)
	}

	type timeTomb struct {
		ID        int64
		DeletedAt time.Time
	}

	h2 := NewSoftDeleteTable[timeTomb, int64]("t_time_tomb")
	id2 := NewColumn(h2.TableOf, "id", "id", func(r *timeTomb) *int64 { return &r.ID })
	del2 := NewColumn(h2.TableOf, "deleted_at", "deleted_at", func(r *timeTomb) *time.Time { return &r.DeletedAt })

	if err := h2.Define(TableSpec[timeTomb, int64]{Columns: []BoundColumn[timeTomb]{id2, del2}, PrimaryKey: id2}, del2).Err(); err == nil || !strings.Contains(err.Error(), "int64 or uint64") {
		t.Errorf("time.Time tombstone: %v", err)
	}

	type shortTomb struct {
		ID        int64
		DeletedAt int16
	}

	h3 := NewSoftDeleteTable[shortTomb, int64]("t_short_tomb")
	id3 := NewColumn(h3.TableOf, "id", "id", func(r *shortTomb) *int64 { return &r.ID })
	del3 := NewColumn(h3.TableOf, "deleted_at", "deleted_at", func(r *shortTomb) *int16 { return &r.DeletedAt })

	if err := h3.Define(TableSpec[shortTomb, int64]{Columns: []BoundColumn[shortTomb]{id3, del3}, PrimaryKey: id3}, del3).Err(); err == nil {
		t.Error("int16 tombstone: want an error")
	}

	type indexed struct {
		ID   int64
		Body string
	}

	h4 := NewTable[indexed, int64]("t_indexed")
	id4 := NewColumn(h4, "id", "id", func(r *indexed) *int64 { return &r.ID })
	body := NewColumn(h4, "body", "body", func(r *indexed) *string { return &r.Body })

	if err := h4.Define(TableSpec[indexed, int64]{Columns: []BoundColumn[indexed]{id4, body}, PrimaryKey: id4, Indexes: []IndexSpec{{Name: "ft_body", Columns: []string{"body"}, Unique: true, FullText: true}}}).Err(); err == nil || !strings.Contains(err.Error(), "both unique and full-text") {
		t.Errorf("unique full-text index: %v", err)
	}
}
