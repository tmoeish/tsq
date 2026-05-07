package tsq

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTypedAPIDoesNotCompileForInvalidResultInputs(t *testing.T) {
	rootDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get workspace dir: %v", err)
	}

	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "result_col_predicate",
			body: `
var resultCol = userID.Into(func(any) any { return nil }, "user_id")
var _ = resultCol.EQVar()
`,
			want: "resultCol.EQVar undefined",
		},
		{
			name: "owned_columns_reject_result_col",
			body: `
var resultCol = userID.Into(func(any) any { return nil }, "user_id")
var _ = tsq.OwnedColumns[userOwner](resultCol)
`,
			want: "does not implement tsq.OwnedColumn",
		},
		{
			name: "owned_columns_reject_wrong_owner",
			body: `
var _ = tsq.OwnedColumns[userOwner](orderID)
`,
			want: "does not implement tsq.OwnedColumn",
		},
		{
			name: "join_cond_reject_wrong_right_owner",
			body: `
var _ = tsq.OnRight[userOwner, orderOwner](productStatus.EQ(1))
`,
			want: "cannot use productStatus.EQ(1)",
		},
		{
			name: "new_col_rejects_non_table_owner",
			body: `
type nonTableOwner struct{}

var _ = tsq.NewCol[nonTableOwner, int]("id", "id", nil)
`,
			want: "nonTableOwner does not satisfy tsq.Table",
		},
		{
			name: "new_col_rejects_wrong_field_pointer_owner",
			body: `
var _ = tsq.NewCol[userOwner, int]("id", "id", func(o *orderOwner) *int { return nil })
`,
			want: "cannot use func(o *orderOwner) *int",
		},
		{
			name: "new_col_rejects_wrong_field_pointer_value",
			body: `
var _ = tsq.NewCol[userOwner, int]("id", "id", func(o *userOwner) *string { return nil })
`,
			want: "cannot use func(o *userOwner) *string",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertCompileFails(t, rootDir, tc.body, tc.want)
		})
	}
}

func TestGeneratedResultBuilderDoesNotCompileForInvalidInputs(t *testing.T) {
	rootDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get workspace dir: %v", err)
	}

	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "join_stage_rejects_skipped_table",
			body: `
var _ = database.UserOrderFromUser().
	LeftJoinOrder(database.UserOrderJoinUserIDToOrderUserID())
`,
			want: "LeftJoinOrder undefined",
		},
		{
			name: "join_rejects_wrong_edge",
			body: `
var _ = database.UserOrderFromUser().
	LeftJoinOrg(database.UserOrderJoinUserOrgIDToOrgID()).
	LeftJoinOrder(database.UserOrderJoinOrderItemIDToItemID())
`,
			want: "cannot use database.UserOrderJoinOrderItemIDToItemID()",
		},
		{
			name: "join_extra_rejects_third_table",
			body: `
var _ = database.UserOrderFromUser().
	LeftJoinOrg(
		database.UserOrderJoinUserOrgIDToOrgID(),
		tsq.OnRight[database.User, database.Org](database.Category_Name.EQ("x")),
	)
`,
			want: "cannot use database.Category_Name.EQ(\"x\")",
		},
		{
			name: "where_rejects_wrong_table",
			body: `
var _ = database.UserOrderFromUser().
	LeftJoinOrg(database.UserOrderJoinUserOrgIDToOrgID()).
	LeftJoinOrder(database.UserOrderJoinUserIDToOrderUserID()).
	LeftJoinItem(database.UserOrderJoinOrderItemIDToItemID()).
	LeftJoinCategory(database.UserOrderJoinItemCategoryIDToCategoryID()).
	SelectUserOrder().
	WhereUser(database.Category_Name.EQ("x"))
`,
			want: "cannot use database.Category_Name.EQ(\"x\")",
		},
		{
			name: "group_by_rejects_result_column",
			body: `
var _ = database.UserOrderFromUser().
	LeftJoinOrg(database.UserOrderJoinUserOrgIDToOrgID()).
	LeftJoinOrder(database.UserOrderJoinUserIDToOrderUserID()).
	LeftJoinItem(database.UserOrderJoinOrderItemIDToItemID()).
	LeftJoinCategory(database.UserOrderJoinItemCategoryIDToCategoryID()).
	SelectUserOrder().
	GroupByUser(database.UserOrder_UserID)
`,
			want: "does not implement tsq.OwnedColumn",
		},
		{
			name: "kw_search_rejects_wrong_table",
			body: `
var _ = database.UserOrderFromUser().
	LeftJoinOrg(database.UserOrderJoinUserOrgIDToOrgID()).
	LeftJoinOrder(database.UserOrderJoinUserIDToOrderUserID()).
	LeftJoinItem(database.UserOrderJoinOrderItemIDToItemID()).
	LeftJoinCategory(database.UserOrderJoinItemCategoryIDToCategoryID()).
	SelectUserOrder().
	KwSearchUser(database.Category_Name)
`,
			want: "does not implement tsq.OwnedColumn",
		},
		{
			name: "kw_search_rejects_result_column",
			body: `
var _ = database.UserOrderFromUser().
	LeftJoinOrg(database.UserOrderJoinUserOrgIDToOrgID()).
	LeftJoinOrder(database.UserOrderJoinUserIDToOrderUserID()).
	LeftJoinItem(database.UserOrderJoinOrderItemIDToItemID()).
	LeftJoinCategory(database.UserOrderJoinItemCategoryIDToCategoryID()).
	SelectUserOrder().
	KwSearchUser(database.UserOrder_UserName)
`,
			want: "does not implement tsq.OwnedColumn",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertCompileFailsSource(t, rootDir, compileFailResultBuilderSource(tc.body), tc.want)
		})
	}
}

func assertCompileFails(t *testing.T, rootDir, body, want string) {
	t.Helper()

	assertCompileFailsSource(t, rootDir, compileFailSource(body), want)
}

func assertCompileFailsSource(t *testing.T, rootDir, source, want string) {
	t.Helper()

	dir, err := os.MkdirTemp(rootDir, "compilefail_")
	if err != nil {
		t.Fatalf("create compile-fail package: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("remove compile-fail package: %v", err)
		}
	})

	writeCompileFailFile(t, filepath.Join(dir, "main.go"), source)

	cmd := exec.Command("go", "test", "./"+filepath.Base(dir))
	cmd.Dir = rootDir
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected compile failure, got success:\n%s", output)
	}

	if !strings.Contains(string(output), want) {
		t.Fatalf("compile error did not contain %q:\n%s", want, output)
	}
}

func compileFailSource(body string) string {
	return `package compilefail

import "github.com/tmoeish/tsq"

type userOwner struct{}
type orderOwner struct{}
type productOwner struct{}

func (userOwner) Table() string { return "users" }

func (userOwner) KwList() []tsq.Column { return nil }

func (orderOwner) Table() string { return "orders" }

func (orderOwner) KwList() []tsq.Column { return nil }

func (productOwner) Table() string { return "products" }

func (productOwner) KwList() []tsq.Column { return nil }

var userID = tsq.NewCol[userOwner, int]("id", "id", nil)
var orderID = tsq.NewCol[orderOwner, int]("id", "id", nil)
var productStatus = tsq.NewCol[productOwner, int]("status", "status", nil)
` + body
}

func compileFailResultBuilderSource(body string) string {
	return `package compilefail

import (
	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/examples/database"
)
` + body
}

func writeCompileFailFile(t *testing.T, path, contents string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
