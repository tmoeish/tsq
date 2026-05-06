package parser

import (
	"go/ast"
	"reflect"
	"strings"
	"testing"

	"github.com/tmoeish/tsq"
)

func TestParseAnnotations_DSL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc    string
		comment string
		want    tsq.TableInfo
	}{
		{
			desc: "典型 TABLE 全写",
			comment: `
			//   @TABLE(   	name="account",   	pk="C1,true",   	version, created_at, updated_at="MTime", deleted_at,   	ux=[   		{name="U1", fields=["F1","F2"]},   		{fields=["F3"]}   	],   	idx=[   		{name="I1", fields=["F4"]},   		{fields=["F5","F6"]}   	],   	kw=["foo","bar"]   )`,
			want: tsq.TableInfo{
				Table:          "account",
				ID:             "C1",
				AI:             true,
				VersionField:   "Version",
				CreatedAtField: "CreatedAt",
				UpdatedAtField: "MTime",
				DeletedAtField: "DeletedAt",
				UxList: []tsq.IndexInfo{
					{Name: "U1", Fields: []string{"F1", "F2"}},
					{Name: "ux_account_f3", Fields: []string{"F3"}},
				},
				IdxList: []tsq.IndexInfo{
					{Name: "I1", Fields: []string{"F4"}},
					{Name: "idx_account_f5_f6", Fields: []string{"F5", "F6"}},
				},
				KwList: []string{"foo", "bar"},
			},
		},
		{
			desc:    "TABLE 省略主键和简写",
			comment: `// @TABLE( 	name="user", 	version, created_at, updated_at="MTime", deleted_at  )`,
			want: tsq.TableInfo{
				Table:          "user",
				ID:             "ID",
				AI:             true,
				VersionField:   "Version",
				CreatedAtField: "CreatedAt",
				UpdatedAtField: "MTime",
				DeletedAtField: "DeletedAt",
			},
		},
		{
			desc:    "Result 注解",
			comment: `// @RESULT(name="UserResult",  kw=["foo","bar"], join=[{left="User.ID", right="Order.UserID"}]  )`,
			want: tsq.TableInfo{
				Table:    "UserResult",
				KwList:   []string{"foo", "bar"},
				JoinList: []tsq.JoinInfo{{Left: "User.ID", Right: "Order.UserID"}},
			},
		},
		{
			desc:    "TABLE ux 和 idx 无 name",
			comment: `// @TABLE(ux=[{fields=["F1"]}], idx=[{fields=["F2","F3"]}])`,
			want: tsq.TableInfo{
				Table:   "user",
				ID:      "ID",
				AI:      true,
				UxList:  []tsq.IndexInfo{{Name: "ux_user_f1", Fields: []string{"F1"}}},
				IdxList: []tsq.IndexInfo{{Name: "idx_user_f2_f3", Fields: []string{"F2", "F3"}}},
			},
		},
		{
			desc:    "TABLE 省略 name",
			comment: `// @TABLE(pk="ID,true", version, created_at)`,
			want: tsq.TableInfo{
				Table:          "user",
				ID:             "ID",
				AI:             true,
				VersionField:   "Version",
				CreatedAtField: "CreatedAt",
			},
		},
		{
			desc:    "Result 省略 name",
			comment: `// @RESULT(kw=["foo","bar"])`,
			want: tsq.TableInfo{
				KwList: []string{"foo", "bar"},
			},
		},
		{
			desc:    "Result最简",
			comment: `// @RESULT`,
			want:    tsq.TableInfo{},
		},
	}

	structFields := map[string]struct{}{
		"ID": {}, "C1": {}, "Version": {}, "CreatedAt": {}, "MTime": {}, "DeletedAt": {}, "F1": {}, "F2": {}, "F3": {}, "F4": {}, "F5": {}, "F6": {}, "account": {}, "user": {}, "UserResult": {}, "foo": {}, "bar": {}, "Order": {}, "UserID": {},
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
				!reflect.DeepEqual(info.ID, tt.want.ID) ||
				!reflect.DeepEqual(info.AI, tt.want.AI) ||
				!reflect.DeepEqual(info.VersionField, tt.want.VersionField) ||
				!reflect.DeepEqual(info.CreatedAtField, tt.want.CreatedAtField) ||
				!reflect.DeepEqual(info.UpdatedAtField, tt.want.UpdatedAtField) ||
				!reflect.DeepEqual(info.DeletedAtField, tt.want.DeletedAtField) ||
				!reflect.DeepEqual(info.UxList, tt.want.UxList) ||
				!reflect.DeepEqual(info.IdxList, tt.want.IdxList) ||
				!reflect.DeepEqual(info.KwList, tt.want.KwList) ||
				!reflect.DeepEqual(info.JoinList, tt.want.JoinList) {
				infoStr := tsq.PrettyJSON(info)
				wantStr := tsq.PrettyJSON(tt.want)
				t.Errorf("got = %s, want %s", infoStr, wantStr)
			}
		})
	}
}

func TestExtractDSLContent_IgnoresParenthesesInsideStrings(t *testing.T) {
	content, err := extractDSLContent(`// @TABLE(name="user(test)", kw=["name"])`, "@TABLE")
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

	info, err := parseDSL("User", cg, map[string]struct{}{"ID": {}})
	if err != nil {
		t.Fatalf("parseDSL returned error for non-annotation prefix: %v", err)
	}

	if info != nil {
		t.Fatalf("expected non-annotation prefix to be ignored, got %#v", info)
	}
}

func TestParseDSL_IgnoresAnnotationMentionsInProse(t *testing.T) {
	cg := []*ast.CommentGroup{{List: []*ast.Comment{{Text: `// This struct can be generated with @TABLE(name="user") later.`}}}}

	info, err := parseDSL("User", cg, map[string]struct{}{"ID": {}})
	if err != nil {
		t.Fatalf("parseDSL returned error for prose annotation mention: %v", err)
	}

	if info != nil {
		t.Fatalf("expected prose annotation mention to be ignored, got %#v", info)
	}
}

func TestParseTableDSL_ReturnsErrorForMalformedAnnotation(t *testing.T) {
	_, err := parseTableDSL("User", `// @TABLE(name="user"`, map[string]struct{}{
		"ID": {},
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
		"ID": {},
	})
	if err == nil {
		t.Fatal("expected malformed Result annotation to return an error")
	}

	if !IsErrorType(err, ErrorTypeDSLMissingBracket) {
		t.Fatalf("expected missing bracket error, got %v", err)
	}
}

// TableInfoMock 用于字段比对
// TableInfoReal 用于真实填充
// parseAnnotationsForTest 用于测试 parseAnnotations

type TableInfoMock struct {
	Table          string
	ID             string
	CustomID       bool
	VersionField   string
	CreatedAtField string
	UpdatedAtField string
	DeletedAtField string
	UxList         []string
	IdxList        []string
	KwList         []string
}

type TableInfoReal struct {
	Table          string
	ID             string
	CustomID       bool
	VersionField   string
	CreatedAtField string
	UpdatedAtField string
	DeletedAtField string
	UxList         []tsq.IndexInfo
	IdxList        []tsq.IndexInfo
	KwList         []string
}

func (r *TableInfoReal) ToMock() TableInfoMock {
	mock := TableInfoMock{
		Table:          r.Table,
		ID:             r.ID,
		CustomID:       r.CustomID,
		VersionField:   r.VersionField,
		CreatedAtField: r.CreatedAtField,
		UpdatedAtField: r.UpdatedAtField,
		DeletedAtField: r.DeletedAtField,
		KwList:         r.KwList,
	}
	for _, ux := range r.UxList {
		mock.UxList = append(mock.UxList, ux.Name+":"+joinFields(ux.Fields))
	}

	for _, idx := range r.IdxList {
		mock.IdxList = append(mock.IdxList, idx.Name+":"+joinFields(idx.Fields))
	}

	return mock
}

// joinFields 只处理 []string，不再递归
func joinFields(fields any) string {
	if v, ok := fields.([]string); ok {
		return strings.Join(v, ",")
	}

	return ""
}

func (m TableInfoMock) Get(key string) any {
	switch key {
	case "Table":
		return m.Table
	case "ID":
		return m.ID
	case "CustomID":
		return m.CustomID
	case "Version":
		return m.VersionField
	case "CreatedAt":
		return m.CreatedAtField
	case "UpdatedAt":
		return m.UpdatedAtField
	case "DeletedAt":
		return m.DeletedAtField
	case "UxList":
		return m.UxList
	case "IdxList":
		return m.IdxList
	case "KwList":
		return m.KwList
	default:
		return nil
	}
}
