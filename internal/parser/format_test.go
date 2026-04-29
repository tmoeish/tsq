package parser

import (
	"go/ast"
	goparser "go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFormatLineCommentGroup_PreservesProse(t *testing.T) {
	group := mustParseFirstCommentGroup(t, `package sample

// 订单表
// @TABLE(
//
//	pk="UID,true",
//	idx=[
//	  {fields=["UserID","ItemID"]},
//	  {name="IdxItem", fields=["ItemID"]}
//	],
//	created_at,
//	updated_at,
//	deleted_at,
//	version
//
// )
type Order struct{}
`)

	got, err := formatLineCommentGroup(group)
	if err != nil {
		t.Fatalf("formatLineCommentGroup() error = %v", err)
	}

	want := strings.Join([]string{
		"// 订单表",
		"// @TABLE(",
		"//",
		"//\tpk=\"UID,true\",",
		"//\tversion,",
		"//\tcreated_at,",
		"//\tupdated_at,",
		"//\tdeleted_at,",
		"//\tidx=[",
		"//\t\t{fields=[\"UserID\", \"ItemID\"]},",
		"//\t\t{name=\"IdxItem\", fields=[\"ItemID\"]},",
		"//\t],",
		"//",
		"// )",
	}, "\n")

	if got != want {
		t.Fatalf("unexpected formatted line comment:\n%s", got)
	}
}

func TestFormatBlockCommentGroup_PreservesProse(t *testing.T) {
	group := mustParseFirstCommentGroup(t, `package sample

/*
Item 商品表

	@TABLE(
		ux=[{fields=["Name"]}],
		idx=[{name="IdxCategory", fields=["CategoryID"]}],
		kw=["Name"],
		created_at
	)
*/
type Item struct{}
`)

	got, err := formatBlockCommentGroup(group)
	if err != nil {
		t.Fatalf("formatBlockCommentGroup() error = %v", err)
	}

	want := "/*\n" + strings.Join([]string{
		"Item 商品表",
		"",
		"\t@TABLE(",
		"\t\tcreated_at,",
		"\t\tux=[",
		"\t\t\t{fields=[\"Name\"]},",
		"\t\t],",
		"\t\tidx=[",
		"\t\t\t{name=\"IdxCategory\", fields=[\"CategoryID\"]},",
		"\t\t],",
		"\t\tkw=[\"Name\"],",
		"\t)",
	}, "\n") + "\n*/"

	if got != want {
		t.Fatalf("unexpected formatted block comment:\n%s", got)
	}
}

func TestFormatSourceFile_FormatsStructAnnotationsOnly(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "models.go")
	src := `package sample

// this prose mention should stay untouched: @TABLE(name="noop")
var marker = 1

// 用户表
// @TABLE(
//
//	kw=["Name","Email"],
//	created_at
//
// )
type User struct{}

/*
Item 商品表

	@TABLE(
		ux=[{fields=["Name"]}],
		created_at
	)
*/
type Item struct{}

// @RESULT(name="UserOrder")
type UserOrder struct{}
`

	if err := os.WriteFile(filename, []byte(src), 0o644); err != nil {
		t.Fatalf("failed to write temp source: %v", err)
	}

	changed, err := formatSourceFile(filename)
	if err != nil {
		t.Fatalf("formatSourceFile() error = %v", err)
	}

	if !changed {
		t.Fatal("expected source file to change")
	}

	gotBytes, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("failed to read formatted source: %v", err)
	}

	got := string(gotBytes)
	for _, want := range []string{
		`// this prose mention should stay untouched: @TABLE(name="noop")`,
		"// 用户表\n// @TABLE(\n//\n//\tcreated_at,",
		"//\tkw=[\"Name\", \"Email\"],",
		"Item 商品表\n\n\t@TABLE(",
		"\t\t{fields=[\"Name\"]},",
		"// @RESULT(name=\"UserOrder\")",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected formatted source to contain %q, got:\n%s", want, got)
		}
	}
}

func mustParseFirstCommentGroup(t *testing.T, src string) *ast.CommentGroup {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := goparser.ParseFile(fileSet, "test.go", src, goparser.ParseComments)
	if err != nil {
		t.Fatalf("failed to parse source: %v", err)
	}

	if len(file.Comments) == 0 {
		t.Fatal("expected comment groups")
	}

	return file.Comments[0]
}
