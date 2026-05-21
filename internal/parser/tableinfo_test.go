package parser

import (
	"encoding/json"
	"go/ast"
	"reflect"
	"strings"
	"testing"

	"github.com/tmoeish/tsq/v4/internal/genmodel"
)

func TestParseAnnotations_DSL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc    string
		comment string
		want    genmodel.TableMeta
	}{
		{
			desc: "典型 TABLE 全写",
			comment: `
			//   @TABLE(   	name="account",   	pk="C1,true",   	version, created_at, updated_at="MTime", deleted_at,   	ux=[   		{name="U1", fields=["F1","F2"]},   		{fields=["F3"]}   	],   	idx=[   		{name="I1", fields=["F4"]},   		{fields=["F5","F6"]}   	],   	search=["foo","bar"]   )`,
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
			desc:    "TABLE 省略主键和简写",
			comment: `// @TABLE( 	name="user", 	version, created_at, updated_at="MTime", deleted_at  )`,
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
			desc:    "Result 注解",
			comment: `// @RESULT(name="UserResult",  search=["foo","bar"]  )`,
			want: genmodel.TableMeta{
				Table:         "UserResult",
				SearchColumns: []string{"foo", "bar"},
			},
		},
		{
			desc:    "TABLE ux 和 idx 无 name",
			comment: `// @TABLE(ux=[{fields=["F1"]}], idx=[{fields=["F2","F3"]}])`,
			want: genmodel.TableMeta{
				Table:   "user",
				PK:      "ID",
				AI:      true,
				UxList:  []genmodel.IndexInfo{{Name: "ux_user_f1", Fields: []string{"F1"}}},
				IdxList: []genmodel.IndexInfo{{Name: "idx_user_f2_f3", Fields: []string{"F2", "F3"}}},
			},
		},
		{
			desc:    "TABLE 省略 name",
			comment: `// @TABLE(pk="PK,true", version, created_at)`,
			want: genmodel.TableMeta{
				Table:          "user",
				PK:             "PK",
				AI:             true,
				VersionField:   "Version",
				CreatedAtField: "CreatedAt",
			},
		},
		{
			desc:    "Result 省略 name",
			comment: `// @RESULT(search=["foo","bar"])`,
			want: genmodel.TableMeta{
				SearchColumns: []string{"foo", "bar"},
			},
		},
		{
			desc:    "Result最简",
			comment: `// @RESULT`,
			want:    genmodel.TableMeta{},
		},
	}

	structFields := map[string]struct{}{
		"ID": {}, "PK": {}, "C1": {}, "Version": {}, "CreatedAt": {}, "MTime": {}, "DeletedAt": {}, "F1": {}, "F2": {}, "F3": {}, "F4": {}, "F5": {}, "F6": {}, "account": {}, "user": {}, "UserResult": {}, "foo": {}, "bar": {}, "Order": {}, "UserID": {},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			cg := []*ast.CommentGroup{{List: []*ast.Comment{{Text: tt.comment}}}}

			info, err := parseDSL("User", cg, structFields)
			if err != nil {
				t.Fatalf("parseDSL error: %v", err)
			}

			if info == nil {
				t.Fatalf("parseDSL returned nil")
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
				infoStr := prettyJSON(info)
				wantStr := prettyJSON(tt.want)
				t.Errorf("got = %s, want %s", infoStr, wantStr)
			}
		})
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
