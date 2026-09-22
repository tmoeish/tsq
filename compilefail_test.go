package tsq

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// compileFailCases are programs the type system must reject. Each is the body of
// its own function in one generated package, compiled once; every case must
// produce an error on one of its own lines that contains want.
var compileFailCases = []struct {
	name string
	body string
	want string
}{
	{"where twice", `tsq.Select(UserID).From(Users).Where(UserID.EQ(tsq.Val(int64(1)))).Where(UserID.EQ(tsq.Val(int64(2))))`, "Where undefined"},
	{"search twice", `tsq.Select(UserID).From(Users).Search(UserName).Search(UserName)`, "Search undefined"},
	{"having without group by", `tsq.Select(UserID).From(Users).Having(UserID.EQ(tsq.Val(int64(1))))`, "Having undefined"},
	{"join after where", `tsq.Select(UserID).From(Users).Where(UserID.EQ(tsq.Val(int64(1)))).Join(Users)`, "Join undefined"},
	{"lock after group by", `tsq.Select(UserID).From(Users).GroupBy(UserID).ForUpdate()`, "ForUpdate undefined"},
	{"set operation after search", `tsq.Select(UserID).From(Users).Search(UserName).Union(tsq.Select(UserID).From(Users))`, "Union undefined"},
	{"where after order by", `tsq.Select(UserID).From(Users).OrderBy(UserID.Asc()).Where(UserID.EQ(tsq.Val(int64(1))))`, "Where undefined"},
	{"wait mode without lock", `tsq.Select(UserID).From(Users).NoWait()`, "NoWait undefined"},
	{"columns of two owners", `tsq.Select(UserID, OrderID)`, "OrderID"},
	{"select before from twice", `tsq.Select(UserID).From(Users).From(Users)`, "From undefined"},
	{"sql.DB is not an executor", `var db *sql.DB; _, _ = tsq.Select(UserID).From(Users).List(context.Background(), db)`, "missing method needsRuntimeOrWrapExecutor"},
	{"plain values are not args", `_, _ = tsq.Select(UserID).From(Users).List(context.Background(), nil, 1)`, "cannot use 1"},
	{"param of another type", `tsq.Select(UserID).From(Users).Where(UserID.EQ(tsq.NewParam[string]("x")))`, "does not implement tsq.Operand[int64]"},
	{"bind of another type", `_ = UserID.Bind("x")`, `cannot use "x"`},
	{"value of another type", `_ = UserID.EQ(tsq.Val("x"))`, "does not implement tsq.Operand[int64]"},
	{"untyped constant is an int", `_ = UserID.EQ(tsq.Val(1))`, "wrong type for method valueOfType"},
	{"values of another type", `_ = UserID.In(tsq.Vals("x"))`, "does not implement tsq.ListOperand[int64]"},
	{"value list as scalar", `_ = UserID.EQ(tsq.Vals(int64(1)))`, "does not implement tsq.Operand[int64]"},
	{"value methods are gone", `_ = UserID.EQVal(1)`, "EQVal undefined"},
	{"list param as scalar", `_ = UserID.EQ(UserID.ListParam())`, "does not implement tsq.Operand[int64]"},
	{"scalar param as list", `_ = UserID.In(UserID.Param())`, "does not implement tsq.ListOperand[int64]"},
	{"column of another type", `_ = UserID.EQ(UserName)`, "does not implement tsq.Operand[int64]"},
	{"set of another owner", `_ = tsq.UpdateTable(Users).Set(OrderID, UserID)`, "OrderID"},
	{"mutation after where", `_ = tsq.UpdateTable(Users).Set(UserName, tsq.Val("x")).Where(tsq.And()).Where(tsq.And())`, "Where undefined"},
	{"set after where", `_ = tsq.UpdateTable(Users).Set(UserName, tsq.Val("x")).Where(tsq.And()).Set(UserName, tsq.Val("y"))`, "Set undefined"},
	{"result column predicate", `_ = tsq.MapInto(UserID, func(r *Label) *int64 { return nil }).Named("id").EQ(tsq.Val(int64(1)))`, "EQ undefined"},
	{"conditions are sealed", `var _ tsq.Condition = fakeCondition{}`, "does not implement tsq.Condition"},
	{"tables are sealed", `var _ tsq.Table = fakeTable{}`, "does not implement tsq.Table"},
	{"executors are sealed", `var _ tsq.Executor = fakeExecutor{}`, "does not implement tsq.Executor"},
	{"case result of another type", `_ = tsq.Case[string]().When(UserID.EQ(tsq.Val(int64(1))), UserID)`, "does not implement tsq.Operand[string]"},
	{"case value of another type", `_ = tsq.Case[string]().When(UserID.EQ(tsq.Val(int64(1))), tsq.Val(3))`, "does not implement tsq.Operand[string]"},
	{"text function on a number", `_ = tsq.Upper(UserID)`, "does not satisfy tsq.Text"},
	{"numeric function on text", `_ = tsq.Sum(UserName)`, "does not satisfy tsq.Number"},
	{"search on a number", `_ = tsq.Searchable(UserID)`, "does not satisfy ~string"},
	{"pattern of another type", `_ = tsq.Contains(UserName, tsq.Val(3))`, "tsq.Value[int]"},
	{"pattern of a number column", `_ = tsq.Contains(UserID, tsq.Val(int64(3)))`, "does not satisfy ~string"},
	{"pattern as a plain string", `_ = tsq.Contains(UserName, "x")`, "does not implement tsq.Pattern[string]"},
	{"expression is not selectable", `_ = tsq.Select(tsq.Upper(UserName))`, "does not match tsq.BoundColumn"},
	{"expression cannot be rebound", `_ = tsq.Upper(UserName).WithTable(Users)`, "WithTable undefined"},
	{"expression has no parameter", `_ = tsq.Count(UserID).Param()`, "Param undefined"},
	{"null value for a string column", `_ = UserName.EQ(tsq.Val[*string](nil))`, "does not implement tsq.Operand[string]"},
	{"set null on a NOT NULL column", `_ = tsq.UpdateTable(Users).SetNull(UserName)`, "does not implement tsq.NullColumn"},
	{"nullable column value type", `_ = NickName.EQ(tsq.Val(sql.NullString{}))`, "does not implement tsq.Operand[string]"},
	{"upsert key of another table", `_ = Users.Upsert(context.Background(), nil, &User{}, OrderID)`, "does not implement tsq.BoundColumn[User]"},
	{"pattern param variants are gone", `_ = tsq.ContainsParam(UserName, UserName.Param())`, "undefined: tsq.ContainsParam"},
	{"functions are not column methods", `_ = UserName.Upper()`, "Upper undefined"},
	{"key of another type", `_, _ = Users.Get(context.Background(), nil, "x")`, `cannot use "x"`},
	{"delete keys of another type", `_ = Posts.BatchDeleteByPK(context.Background(), nil, []string{"x"})`, "cannot use []string"},
	{"a plain table has no soft delete", `_ = Users.Delete(context.Background(), nil, &User{})`, "Delete undefined"},
	{"a plain table has no restore", `_ = Users.Restore(context.Background(), nil, &User{})`, "Restore undefined"},
	{"a plain table has no deleted scope", `_ = Users.WithDeleted()`, "WithDeleted undefined"},
	{"DeleteFrom names HardDeleteFrom", `_ = tsq.DeleteFrom(Users)`, "missing method needsDeletedAtOrHardDeleteFrom"},
	{"columns rebind through the table", `_ = UserID.As("u")`, "As undefined"},
	{"subquery of another type", `_ = UserID.In(tsq.SelectValue(UserName).From(Users))`, "does not implement tsq.ListOperand[int64]"},
	{"table rows are not a value", `_ = UserID.In(tsq.Select(UserID).From(Users))`, "does not implement tsq.ListOperand[int64]"},
	{"subquery builders are gone", `_, _ = tsq.BuildSubquery(tsq.SelectValue(UserID).From(Users), UserID)`, "undefined: tsq.BuildSubquery"},
	{"from-first entry is gone", `_ = tsq.From[User](Users)`, "undefined: tsq.From"},
	{"update column of another table", `_ = Users.Update(context.Background(), nil, &User{}, OrderID)`, "OrderID"},
	{"set a result column", `_ = tsq.UpdateTable(Users).Set(tsq.MapInto(UserName, func(u *User) *string { return &u.Name }), tsq.Val("x"))`, "does not match tsq.Column[User, T]"},
	{"a literal names tsq.Val", `_ = UserID.EQ(1)`, "missing method needsTsqVal"},
	{"a slice names tsq.Vals", `_ = UserID.In([]int64{1})`, "missing method needsTsqVals"},
	{"a pattern literal names tsq.Val", `_ = tsq.StartsWith(UserName, "x")`, "missing method needsTsqVal"},
	{"sort direction is internal", `_ = tsq.ASC`, "undefined: tsq.ASC"},
	{"columns do not expose their table", `_ = UserID.Table()`, "Table undefined"},
	{"fetch by a column of another table", `_, _ = Users.FetchBy(context.Background(), nil, OrderID, []int64{1})`, "OrderID"},
}

const compileFailPrelude = `package compilefail

import (
	"context"
	"database/sql"

	"github.com/tmoeish/tsq/v5"
)

type User struct {
	ID   int64
	Name string
	Nick sql.NullString
}

type Order struct{ ID int64 }

type Label struct{ ID int64 }

var usersHandle = tsq.NewTable[User, int64]("users")

var (
	UserID   = tsq.NewColumn(usersHandle, "id", "id", func(r *User) *int64 { return &r.ID })
	UserName = tsq.NewColumn(usersHandle, "name", "name", func(r *User) *string { return &r.Name })
	NickName = tsq.NewNullColumn[string](usersHandle, "nick", "nick", func(r *User) *sql.NullString { return &r.Nick })
)

var Users = usersHandle.Define(tsq.TableSpec[User, int64]{Columns: []tsq.BoundColumn[User]{UserID, UserName}, PrimaryKey: UserID})

type Post struct {
	ID        int64
	DeletedAt int64
}

var postsHandle = tsq.NewSoftDeleteTable[Post, int64]("posts")

var (
	PostID        = tsq.NewColumn(postsHandle.TableOf, "id", "id", func(r *Post) *int64 { return &r.ID })
	PostDeletedAt = tsq.NewColumn(postsHandle.TableOf, "deleted_at", "deleted_at", func(r *Post) *int64 { return &r.DeletedAt })
)

var Posts = postsHandle.Define(tsq.TableSpec[Post, int64]{Columns: []tsq.BoundColumn[Post]{PostID, PostDeletedAt}, PrimaryKey: PostID}, PostDeletedAt)

var ordersHandle = tsq.NewTable[Order, int64]("orders")

var OrderID = tsq.NewColumn(ordersHandle, "id", "id", func(r *Order) *int64 { return &r.ID })

type fakeCondition struct{}

func (fakeCondition) Clause() string { return "1 = 1" }

type fakeTable struct{}

func (fakeTable) TableName() string { return "fake" }

type fakeExecutor struct{ *sql.DB }

var (
	_ = context.Background
	_ = Users
	_ = OrderID
	_ = NickName
)
`

var compileErrorLine = regexp.MustCompile(`main\.go:(\d+):\d+: (.*)`)

func TestTypeSystemRejectsInvalidPrograms(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a package")
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	var src strings.Builder

	src.WriteString(compileFailPrelude)

	lines := strings.Count(compileFailPrelude, "\n")
	caseLine := make([]int, len(compileFailCases))

	for i, c := range compileFailCases {
		fmt.Fprintf(&src, "\nfunc case%d() {\n", i)
		lines += 2
		caseLine[i] = lines + 1

		src.WriteString(c.body + "\n}\n")
		lines += 2
	}

	dir, err := os.MkdirTemp(root, "compilefail_")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "vet", "./"+filepath.Base(dir))
	cmd.Dir = root
	output, _ := cmd.CombinedOutput()

	cmd = exec.Command("go", "build", "-gcflags=-e", "./"+filepath.Base(dir))
	cmd.Dir = root
	build, _ := cmd.CombinedOutput()
	output = append(output, build...)

	errorsByLine := map[int][]string{}
	for _, m := range compileErrorLine.FindAllStringSubmatch(string(output), -1) {
		line, _ := strconv.Atoi(m[1])
		errorsByLine[line] = append(errorsByLine[line], m[2])
	}

	for line, msgs := range errorsByLine {
		if line <= strings.Count(compileFailPrelude, "\n") {
			t.Fatalf("the prelude must compile; line %d: %v", line, msgs)
		}
	}

	for i, c := range compileFailCases {
		t.Run(c.name, func(t *testing.T) {
			msgs := errorsByLine[caseLine[i]]
			for _, msg := range msgs {
				if strings.Contains(msg, c.want) {
					return
				}
			}

			t.Fatalf("line %d (%s) compiled or failed differently: %v", caseLine[i], c.body, msgs)
		})
	}
}
