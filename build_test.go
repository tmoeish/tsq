package tsq

import (
	"strings"
	"testing"
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
		Join(Users.As("u2"), alias.EQ(User_ID)).
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
			stage: Select(User_ID).From(Users).Join(Orders, User_Name.EQ(Val("x"))),
			want:  "must reference orders",
		},
		"join twice": {
			stage: Select(User_ID).From(Users).Join(Orders, Order_UserID.EQ(User_ID)).Join(Orders, Order_UserID.EQ(User_ID)),
			want:  "already in the query",
		},
		"correlate shadows": {
			stage: Select(User_ID).From(Users).Correlate(Users),
			want:  "shadow",
		},
		"offset without limit": {
			stage: Select(User_ID).From(Users).Offset(5),
			want:  "offset requires limit",
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
		Where(...Condition) FilteredStage[user]
	})
	if ok {
		if _, err := where.Where(User_ID.EQ(Val(int64(1)))).Build(); err == nil {
			t.Fatal("expected Where after GroupBy to fail")
		}
	}

	again, ok := grouped.(interface {
		GroupBy(...SQLColumn) GroupedStage[user]
	})
	if !ok {
		t.Fatal("the builder implements GroupBy")
	}

	if _, err := again.GroupBy(User_Name).Build(); err == nil {
		t.Fatal("expected a second GroupBy to fail")
	}
}

func TestDefineReportsInvalidTables(t *testing.T) {
	type row struct{ ID, Other int64 }

	tests := map[string]func() error{
		"no primary key": func() error {
			h := NewTable[row, int64]("t1")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })

			return h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{id}}).Err()
		},
		"primary key not listed": func() error {
			h := NewTable[row, int64]("t2")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })
			other := NewColumn(h, "other", "other", func(r *row) *int64 { return &r.Other })

			return h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{other}, PrimaryKey: id}).Err()
		},
		"foreign column": func() error {
			h := NewTable[row, int64]("t3")
			h2 := NewTable[row, int64]("t4")
			id := NewColumn(h2, "id", "id", func(r *row) *int64 { return &r.ID })

			return h.Define(TableSpec[row, int64]{Columns: []BoundColumn[row]{id}, PrimaryKey: id}).Err()
		},
		"unknown index field": func() error {
			h := NewTable[row, int64]("t5")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })

			return h.Define(TableSpec[row, int64]{
				Columns:    []BoundColumn[row]{id},
				PrimaryKey: id,
				Indexes:    []TableIndex{{Name: "idx_t5_x", Fields: []string{"x"}}},
			}).Err()
		},
		"defined twice": func() error {
			h := NewTable[row, int64]("t6")
			id := NewColumn(h, "id", "id", func(r *row) *int64 { return &r.ID })
			spec := TableSpec[row, int64]{Columns: []BoundColumn[row]{id}, PrimaryKey: id}
			h.Define(spec)

			return h.Define(spec).Err()
		},
		"bad name": func() error {
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
	q := Select(User_ID).From(Users).Join(Orders, Order_UserID.EQ(User_ID)).Where(Order_Note.WithTable(Users).IsNull())
	if _, err := q.Build(); err == nil || !strings.Contains(err.Error(), "does not exist on users") {
		t.Fatalf("Build() error = %v", err)
	}

	// A derived expression has no WithTable to call; rebind the column first.
	rebound := Select(User_ID).From(Users).Join(Users.As("u"), User_ID.EQ(User_ID.WithTable(Users.As("u")))).
		Where(Upper(User_Name.WithTable(Users.As("u"))).IsNull())
	if _, err := rebound.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
}

func TestScalarAndSubqueryRequireTheSelectedColumn(t *testing.T) {
	q := Select(User_ID).From(Users).MustBuild()

	if _, err := q.AsSubquery(User_Version); err == nil {
		t.Fatal("expected a subquery over a different column to be refused")
	}

	if _, err := Select(User_ID, User_Name).From(Users).MustBuild().AsSubquery(User_ID); err == nil {
		t.Fatal("expected a two-column subquery to be refused")
	}

	if _, err := q.AsSubquery(User_ID); err != nil {
		t.Fatalf("AsSubquery() error = %v", err)
	}
}
