package parser

import (
	"encoding/json"
	"go/ast"
	"reflect"
	"strings"
	"testing"

	"github.com/tmoeish/tsq/v4/internal/genmodel"
)

func TestParseDirectives(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc     string
		comments []string
		want     genmodel.TableMeta
	}{
		{
			desc: "table with every key",
			comments: []string{
				"//tsq:table name=account pk=C1",
				"//tsq:managed version created_at updated_at=MTime deleted_at",
				"//tsq:unique F1,F2 name=U1",
				"//tsq:unique F3",
				"//tsq:index F4 name=I1",
				"//tsq:index F5,F6",
				"//tsq:search foo,bar",
			},
			want: genmodel.TableMeta{
				Table:          "account",
				PK:             "C1",
				AI:             true,
				VersionField:   "Version",
				CreatedAtField: "CreatedAt",
				UpdatedAtField: "MTime",
				DeletedAtField: "DeletedAt",
				UxList: []genmodel.IndexInfo{
					{Name: "U1", Fields: []string{"F1", "F2"}},
					{Name: "ux_account_f3", Fields: []string{"F3"}},
				},
				IdxList: []genmodel.IndexInfo{
					{Name: "I1", Fields: []string{"F4"}},
					{Name: "idx_account_f5_f6", Fields: []string{"F5", "F6"}},
				},
				SearchColumns: []string{"foo", "bar"},
			},
		},
		{
			desc: "primary key defaults to ID and auto-increment",
			comments: []string{
				"//tsq:table name=user",
				"//tsq:managed version created_at updated_at=MTime deleted_at",
			},
			want: genmodel.TableMeta{
				Table:          "user",
				PK:             "ID",
				AI:             true,
				VersionField:   "Version",
				CreatedAtField: "CreatedAt",
				UpdatedAtField: "MTime",
				DeletedAtField: "DeletedAt",
			},
		},
		{
			desc: "assigned turns auto-increment off",
			comments: []string{
				"//tsq:table name=user pk=PK assigned",
			},
			want: genmodel.TableMeta{
				Table: "user",
				PK:    "PK",
				AI:    false,
			},
		},
		{
			desc: "index names are derived when not given",
			comments: []string{
				"//tsq:table",
				"//tsq:unique F1",
				"//tsq:index F2,F3",
			},
			want: genmodel.TableMeta{
				Table:   "user",
				PK:      "ID",
				AI:      true,
				UxList:  []genmodel.IndexInfo{{Name: "ux_user_f1", Fields: []string{"F1"}}},
				IdxList: []genmodel.IndexInfo{{Name: "idx_user_f2_f3", Fields: []string{"F2", "F3"}}},
			},
		},
		{
			desc: "table name defaults to the struct name in snake case",
			comments: []string{
				"//tsq:table pk=PK",
				"//tsq:managed version created_at",
			},
			want: genmodel.TableMeta{
				Table:          "user",
				PK:             "PK",
				AI:             true,
				VersionField:   "Version",
				CreatedAtField: "CreatedAt",
			},
		},
		{
			desc: "result with search",
			comments: []string{
				"//tsq:result",
				"//tsq:search foo,bar",
			},
			want: genmodel.TableMeta{
				SearchColumns: []string{"foo", "bar"},
			},
		},
		{
			desc:     "bare result",
			comments: []string{"//tsq:result"},
			want:     genmodel.TableMeta{},
		},
	}

	structFields := map[string]struct{}{
		"ID": {}, "PK": {}, "C1": {}, "Version": {}, "CreatedAt": {}, "MTime": {}, "DeletedAt": {},
		"F1": {}, "F2": {}, "F3": {}, "F4": {}, "F5": {}, "F6": {}, "foo": {}, "bar": {},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			comments := make([]*ast.Comment, 0, len(tt.comments))
			for _, line := range tt.comments {
				comments = append(comments, &ast.Comment{Text: line})
			}

			info, err := parseDSL("User", []*ast.CommentGroup{{List: comments}}, structFields)
			if err != nil {
				t.Fatalf("parseDSL error: %v", err)
			}

			if info == nil {
				t.Fatal("parseDSL returned nil")
			}

			if !reflect.DeepEqual(info.Table, tt.want.Table) ||
				!reflect.DeepEqual(info.PK, tt.want.PK) ||
				!reflect.DeepEqual(info.AI, tt.want.AI) ||
				!reflect.DeepEqual(info.VersionField, tt.want.VersionField) ||
				!reflect.DeepEqual(info.CreatedAtField, tt.want.CreatedAtField) ||
				!reflect.DeepEqual(info.UpdatedAtField, tt.want.UpdatedAtField) ||
				!reflect.DeepEqual(info.DeletedAtField, tt.want.DeletedAtField) ||
				!reflect.DeepEqual(info.UxList, tt.want.UxList) ||
				!reflect.DeepEqual(info.IdxList, tt.want.IdxList) ||
				!reflect.DeepEqual(info.SearchColumns, tt.want.SearchColumns) {
				t.Errorf("got = %s, want %s", prettyJSON(info), prettyJSON(tt.want))
			}
		})
	}
}

// TestParseDirectivesRejectsBadInput covers the mistakes a directive makes
// possible. Each error must name the line it came from.
func TestParseDirectivesRejectsBadInput(t *testing.T) {
	t.Parallel()

	structFields := map[string]struct{}{"ID": {}, "Name": {}}

	tests := []struct {
		desc     string
		comments []string
		want     string
	}{
		{
			desc:     "no declaration",
			comments: []string{"//tsq:unique Name"},
			want:     "need a //tsq:table or //tsq:result line",
		},
		{
			desc:     "two declarations",
			comments: []string{"//tsq:table", "//tsq:result"},
			want:     "declares //tsq:table or //tsq:result once",
		},
		{
			desc:     "unknown directive",
			comments: []string{"//tsq:table", "//tsq:kw Name"},
			want:     `unknown directive "kw"`,
		},
		{
			desc:     "unknown option",
			comments: []string{"//tsq:table oops=1"},
			want:     `unknown option "oops=1"`,
		},
		{
			desc:     "unknown managed role",
			comments: []string{"//tsq:table", "//tsq:managed touched_at"},
			want:     `unknown managed field "touched_at"`,
		},
		{
			desc:     "index without fields",
			comments: []string{"//tsq:table", "//tsq:index name=idx_x"},
			want:     "needs at least one Go field name",
		},
		{
			desc:     "table key on a result",
			comments: []string{"//tsq:result", "//tsq:managed version"},
			want:     "managed fields belong to a table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			comments := make([]*ast.Comment, 0, len(tt.comments))
			for _, line := range tt.comments {
				comments = append(comments, &ast.Comment{Text: line})
			}

			_, err := parseDSL("User", []*ast.CommentGroup{{List: comments}}, structFields)
			if err == nil {
				t.Fatal("expected an error")
			}

			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected the error to mention %q, got %q", tt.want, err)
			}
		})
	}
}

// TestStructWithoutDirectivesIsNotATable keeps plain structs out of generation.
func TestStructWithoutDirectivesIsNotATable(t *testing.T) {
	t.Parallel()

	cg := []*ast.CommentGroup{{List: []*ast.Comment{
		{Text: "// User is an ordinary struct."},
		{Text: "// It mentions //tsq: nowhere at the start of a line."},
	}}}

	info, err := parseDSL("User", cg, map[string]struct{}{"ID": {}})
	if err != nil {
		t.Fatalf("parseDSL error: %v", err)
	}

	if info != nil {
		t.Fatalf("expected no metadata for a struct without directives, got %+v", info)
	}
}

func prettyJSON(v any) string {
	bs, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		return ""
	}

	return string(bs)
}

func TestExtractDSLContent_IgnoresParenthesesInsideStrings(t *testing.T) {
	content, err := extractDSLContent(`// @TABLE(name="user(test)", search=["name"])`, "@TABLE")
	if err != nil {
		t.Fatalf("extractDSLContent returned error: %v", err)
	}

	if !strings.Contains(content, `name="user(test)"`) {
		t.Fatalf("expected content to preserve parentheses inside strings, got %q", content)
	}
}

func TestExtractDSLContent_ReturnsErrorForMissingBracket(t *testing.T) {
	_, err := extractDSLContent(`// @TABLE(name="user"`, "@TABLE")
	if err == nil {
		t.Fatal("expected malformed TABLE annotation to return an error")
	}

	if !IsErrorType(err, ErrorTypeDSLMissingBracket) {
		t.Fatalf("expected missing bracket error, got %v", err)
	}

	if got := err.Error(); !strings.Contains(got, "@TABLE is missing a closing ')'") {
		t.Fatalf("expected clearer missing closing parenthesis error, got %q", got)
	}
}

func TestExtractDSLContent_ReturnsErrorForArgumentsWithoutBrackets(t *testing.T) {
	_, err := extractDSLContent(`// @TABLE name="user"`, "@TABLE")
	if err == nil {
		t.Fatal("expected TABLE annotation with unbracketed arguments to return an error")
	}

	if !IsErrorType(err, ErrorTypeDSLMissingBracket) {
		t.Fatalf("expected missing bracket error, got %v", err)
	}

	if got := err.Error(); !strings.Contains(got, "@TABLE must be followed by '('") {
		t.Fatalf("expected clearer missing opening parenthesis error, got %q", got)
	}
}

func TestParseDSL_IgnoresAnnotationPrefixes(t *testing.T) {
	cg := []*ast.CommentGroup{{List: []*ast.Comment{{Text: `// @TABLEX(name="user")`}}}}

	info, err := parseDSL("User", cg, map[string]struct{}{"PK": {}})
	if err != nil {
		t.Fatalf("parseDSL returned error for non-annotation prefix: %v", err)
	}

	if info != nil {
		t.Fatalf("expected non-annotation prefix to be ignored, got %#v", info)
	}
}

func TestParseDSL_IgnoresAnnotationMentionsInProse(t *testing.T) {
	cg := []*ast.CommentGroup{{List: []*ast.Comment{{Text: `// This struct can be generated with @TABLE(name="user") later.`}}}}

	info, err := parseDSL("User", cg, map[string]struct{}{"PK": {}})
	if err != nil {
		t.Fatalf("parseDSL returned error for prose annotation mention: %v", err)
	}

	if info != nil {
		t.Fatalf("expected prose annotation mention to be ignored, got %#v", info)
	}
}

func TestParseTableDSL_ReturnsErrorForMalformedAnnotation(t *testing.T) {
	_, err := parseTableDSL("User", `// @TABLE(name="user"`, map[string]struct{}{
		"PK": {},
	})
	if err == nil {
		t.Fatal("expected malformed TABLE annotation to return an error")
	}

	if !IsErrorType(err, ErrorTypeDSLMissingBracket) {
		t.Fatalf("expected missing bracket error, got %v", err)
	}
}

func TestParseResultDSL_ReturnsErrorForMalformedAnnotation(t *testing.T) {
	_, err := parseResultDSL("UserResult", `// @RESULT(name="user"`, map[string]struct{}{
		"PK": {},
	})
	if err == nil {
		t.Fatal("expected malformed Result annotation to return an error")
	}

	if !IsErrorType(err, ErrorTypeDSLMissingBracket) {
		t.Fatalf("expected missing bracket error, got %v", err)
	}
}

func TestGenerateQueryListIncludesFullUniqueSetVariantsOnly(t *testing.T) {
	meta := &genmodel.TableMeta{
		UxList: []genmodel.IndexInfo{
			{Name: "ux_user_email", Fields: []string{"Email"}},
			{Name: "ux_user_org_slug", Fields: []string{"OrgID", "Slug"}},
		},
	}

	generateQueryList(meta)

	assertQuery := func(name string, isSet bool, fields ...string) {
		t.Helper()

		for _, idx := range meta.QueryList {
			if idx.Name == name {
				if idx.IsSet != isSet {
					t.Fatalf("query %s IsSet = %v, want %v", name, idx.IsSet, isSet)
				}
				if !reflect.DeepEqual(idx.Fields, fields) {
					t.Fatalf("query %s fields = %v, want %v", name, idx.Fields, fields)
				}
				return
			}
		}

		t.Fatalf("query %s not found", name)
	}

	assertNoQuery := func(name string) {
		t.Helper()

		for _, idx := range meta.QueryList {
			if idx.Name == name {
				t.Fatalf("unexpected query %s found: %#v", name, idx)
			}
		}
	}

	assertQuery("EmailIn", true, "Email")
	assertNoQuery("Email")

	assertQuery("OrgID", false, "OrgID")
	assertQuery("OrgIDIn", true, "OrgID")
	assertQuery("OrgIDAndSlugIn", true, "OrgID", "Slug")
	assertNoQuery("OrgIDAndSlug")
}
