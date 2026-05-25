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
			name: "col_into_removed",
			body: `
var resultCol = userID.MapInto(func(any) any { return nil }, "user_id")
var _ = resultCol
`,
			want: "userID.MapInto undefined",
		},
		{
			name: "result_col_predicate",
			body: `
var resultCol = tsq.MapInto[userOwner](userID, func(holder *userOwner) *int { return nil }, "user_id")
var _ = resultCol.EQVar()
`,
			want: "resultCol.EQVar undefined",
		},
		{
			name: "column_impl_hidden",
			body: `
var _ tsq.ColumnImpl[userOwner, int]
`,
			want: "undefined: tsq.ColumnImpl",
		},
		{
			name: "projected_column_hidden",
			body: `
var _ tsq.ProjectedColumn[userOwner, int]
`,
			want: "undefined: tsq.ProjectedColumn",
		},
		{
			name: "cond_hidden",
			body: `
var _ tsq.Cond
`,
			want: "undefined: tsq.Cond",
		},
		{
			name: "predicate_hidden",
			body: `
var _ tsq.Predicate[userOwner]
`,
			want: "undefined: tsq.Predicate",
		},
		{
			name: "query_builder_hidden",
			body: `
var _ tsq.QueryBuilder[userOwner]
`,
			want: "undefined: tsq.QueryBuilder",
		},
		{
			name: "input_order_match_hidden",
			body: `
var _ tsq.InputOrderMatch[userOwner, int]
`,
			want: "undefined: tsq.InputOrderMatch",
		},
		{
			name: "default_page_size_hidden",
			body: `
var _ = tsq.DefaultPageSize
`,
			want: "undefined: tsq.DefaultPageSize",
		},
		{
			name: "max_page_size_hidden",
			body: `
var _ = tsq.MaxPageSize
`,
			want: "undefined: tsq.MaxPageSize",
		},
		{
			name: "pretty_json_hidden",
			body: `
var _ = tsq.PrettyJSON
`,
			want: "undefined: tsq.PrettyJSON",
		},
		{
			name: "compact_json_hidden",
			body: `
var _ = tsq.CompactJSON
`,
			want: "undefined: tsq.CompactJSON",
		},
		{
			name: "version_hidden",
			body: `
var _ = tsq.GetVersion()
`,
			want: "undefined: tsq.GetVersion",
		},
		{
			name: "version_info_hidden",
			body: `
var _ tsq.VersionInfo
`,
			want: "undefined: tsq.VersionInfo",
		},
		{
			name: "trace_hidden",
			body: `
var _ = tsq.Trace
`,
			want: "undefined: tsq.Trace",
		},
		{
			name: "trace1_hidden",
			body: `
var _ = tsq.Trace1[int]
`,
			want: "undefined: tsq.Trace1",
		},
		{
			name: "trace_fn_hidden",
			body: `
var _ tsq.TraceFn
`,
			want: "undefined: tsq.TraceFn",
		},
		{
			name: "add_tracer_hidden",
			body: `
var _ = tsq.AddTracer
`,
			want: "undefined: tsq.AddTracer",
		},
		{
			name: "runtime_trace_hidden",
			body: `
var rt *tsq.Runtime
var _ = rt.Trace
`,
			want: "rt.Trace undefined",
		},
		{
			name: "print_sql_hidden",
			body: `
var _ = tsq.PrintSQL
`,
			want: "undefined: tsq.PrintSQL",
		},
		{
			name: "dialect_hidden",
			body: `
var _ tsq.Dialect
`,
			want: "undefined: tsq.Dialect",
		},
		{
			name: "sqlite_dialect_hidden",
			body: `
var _ tsq.SQLiteDialect
`,
			want: "undefined: tsq.SQLiteDialect",
		},
		{
			name: "ddl_column_type_hidden",
			body: `
var _ tsq.DDLColumnType
`,
			want: "undefined: tsq.DDLColumnType",
		},
		{
			name: "table_column_rejects_result_col",
			body: `
var resultCol = tsq.MapInto[userOwner](userID, func(holder *userOwner) *int { return nil }, "user_id")
var _ tsq.TableColumn[userOwner] = resultCol
`,
			want: "does not implement tsq.TableColumn",
		},
		{
			name: "table_column_rejects_wrong_owner",
			body: `
var _ tsq.TableColumn[userOwner] = orderID
`,
			want: "cannot use orderID",
		},
		{
			name: "column_rejects_wrong_owner",
			body: `
var _ tsq.TypedColumn[userOwner, int] = orderID
`,
			want: "cannot use orderID",
		},
		{
			name: "column_rejects_wrong_value",
			body: `
var _ tsq.TypedColumn[userOwner, string] = userID
`,
			want: "cannot use userID",
		},
		{
			name: "select_rejects_wrong_owner",
			body: `
var _ = tsq.Select[userOwner](orderID)
`,
			want: "cannot use orderID",
		},
		{
			name: "select_rejects_mixed_owners",
			body: `
var _ = tsq.Select[userOwner](userID, orderID)
`,
			want: "cannot use orderID",
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
		{
			name: "insert_rejects_non_table_owner",
			body: `
type nonTableOwner struct{}

func (nonTableOwner) TSQOwner() {}

var _ = tsq.Insert[nonTableOwner]
`,
			want: "nonTableOwner does not satisfy tsq.Table",
		},
		{
			name: "chunked_update_rejects_non_table_owner",
			body: `
type nonTableOwner struct{}

func (nonTableOwner) TSQOwner() {}

var _ = tsq.ChunkedUpdate[nonTableOwner]
`,
			want: "nonTableOwner does not satisfy tsq.Table",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertCompileFails(t, rootDir, tc.body, tc.want)
		})
	}
}

func TestStagedQueryBuilderDoesNotCompileForInvalidClauseOrder(t *testing.T) {
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
			name: "select_stage_rejects_having",
			body: `
var _ = tsq.Select[userOwner](userID).Having(userID.EQ(1))
`,
			want: "Having undefined",
		},
		{
			name: "from_stage_rejects_where",
			body: `
var _ = tsq.From[userOwner](userOwner{}).Where(userID.EQ(1))
`,
			want: "Where undefined",
		},
		{
			name: "grouped_stage_rejects_where",
			body: `
var _ = tsq.Select[userOwner](userID).
	From(userOwner{}).
	GroupBy(userID).
	Where(userID.EQ(1))
`,
			want: "Where undefined",
		},
		{
			name: "where_stage_rejects_join",
			body: `
var _ = tsq.Select[userOwner](userID).
	From(userOwner{}).
	Where(userID.EQ(1)).
	LeftJoin(orderOwner{}, userID.EQCol(orderID))
`,
			want: "LeftJoin undefined",
		},
		{
			name: "compound_stage_rejects_where",
			body: `
var _ = tsq.Select[userOwner](userID).
	From(userOwner{}).
	Union(tsq.Select[userOwner](userID).From(userOwner{})).
	Where(userID.EQ(1))
`,
			want: "Where undefined",
		},
		{
			name: "locked_stage_rejects_where",
			body: `
var _ = tsq.Select[userOwner](userID).
	From(userOwner{}).
	ForUpdate().
	Where(userID.EQ(1))
`,
			want: "Where undefined",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertCompileFails(t, rootDir, tc.body, tc.want)
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

import "github.com/tmoeish/tsq/v4"

type userOwner struct{}
type orderOwner struct{}
type productOwner struct{}

func (userOwner) TSQOwner() {}
func (userOwner) Table() string { return "users" }
func (userOwner) Cols() []tsq.SQLColumn { return nil }

func (userOwner) SearchColumns() []tsq.SearchColumn { return nil }
func (userOwner) PrimaryKeys() []string { return nil }
func (userOwner) AutoIncrement() bool { return false }
func (userOwner) VersionColumn() string { return "" }

func (orderOwner) TSQOwner() {}
func (orderOwner) Table() string { return "orders" }
func (orderOwner) Cols() []tsq.SQLColumn { return nil }

func (orderOwner) SearchColumns() []tsq.SearchColumn { return nil }
func (orderOwner) PrimaryKeys() []string { return nil }
func (orderOwner) AutoIncrement() bool { return false }
func (orderOwner) VersionColumn() string { return "" }

func (productOwner) TSQOwner() {}
func (productOwner) Table() string { return "products" }
func (productOwner) Cols() []tsq.SQLColumn { return nil }

func (productOwner) SearchColumns() []tsq.SearchColumn { return nil }
func (productOwner) PrimaryKeys() []string { return nil }
func (productOwner) AutoIncrement() bool { return false }
func (productOwner) VersionColumn() string { return "" }

var userID = tsq.NewCol[userOwner, int]("id", "id", nil)
var orderID = tsq.NewCol[orderOwner, int]("id", "id", nil)
var productStatus = tsq.NewCol[productOwner, int]("status", "status", nil)
` + body
}

func writeCompileFailFile(t *testing.T, path, contents string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
