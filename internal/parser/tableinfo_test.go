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
			//   @TABLE(   	name="account",   	pk="C1,true",   	v, ct, mt="MTime", dt,   	ux=[   		{name="U1", fields=["F1","F2"]},   		{fields=["F3"]}   	],   	idx=[   		{name="I1", fields=["F4"]},   		{fields=["F5","F6"]}   	],   	kw=["foo","bar"]   )`,
			want: tsq.TableInfo{
				Table: "account",
				ID:    "C1",
				AI:    true,
				V:     "V",
				CT:    "CT",
				MT:    "MTime",
				DT:    "DT",
				UxList: []tsq.IndexInfo{
					{Name: "U1", Fields: []string{"F1", "F2"}},
					{Name: "ux_f3", Fields: []string{"F3"}},
				},
				IdxList: []tsq.IndexInfo{
					{Name: "I1", Fields: []string{"F4"}},
					{Name: "idx_f5_f6", Fields: []string{"F5", "F6"}},
				},
				KwList: []string{"foo", "bar"},
			},
		},
		{
			desc:    "TABLE 省略主键和简写",
			comment: `// @TABLE( 	name="user", 	v, ct, mt="MTime", dt  )`,
			want: tsq.TableInfo{
				Table: "user",
				ID:    "ID",
				AI:    true,
				V:     "V",
				CT:    "CT",
				MT:    "MTime",
				DT:    "DT",
			},
		},
		{
			desc:    "DTO 注解",
			comment: `// @DTO( name="UserDTO",  kw=["foo","bar"]  )`,
			want: tsq.TableInfo{
				Table:  "UserDTO",
				KwList: []string{"foo", "bar"},
			},
		},
		{
			desc:    "TABLE ux 和 idx 无 name",
			comment: `// @TABLE(ux=[{fields=["F1"]}], idx=[{fields=["F2","F3"]}])`,
			want: tsq.TableInfo{
				Table:   "user",
				ID:      "ID",
				AI:      true,
				UxList:  []tsq.IndexInfo{{Name: "ux_f1", Fields: []string{"F1"}}},
				IdxList: []tsq.IndexInfo{{Name: "idx_f2_f3", Fields: []string{"F2", "F3"}}},
			},
		},
		{
			desc:    "TABLE 省略 name",
			comment: `// @TABLE(pk="ID,true", v, ct)`,
			want: tsq.TableInfo{
				Table: "user",
				ID:    "ID",
				AI:    true,
				V:     "V",
				CT:    "CT",
			},
		},
		{
			desc:    "DTO 省略 name",
			comment: `// @DTO(kw=["foo","bar"])`,
			want: tsq.TableInfo{
				KwList: []string{"foo", "bar"},
			},
		},
		{
			desc:    "DTO最简",
			comment: `// @DTO`,
			want:    tsq.TableInfo{},
		},
	}

	structFields := map[string]struct{}{
		"ID": {}, "C1": {}, "V": {}, "CT": {}, "MTime": {}, "DT": {}, "F1": {}, "F2": {}, "F3": {}, "F4": {}, "F5": {}, "F6": {}, "account": {}, "user": {}, "UserDTO": {}, "foo": {}, "bar": {},
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
				!reflect.DeepEqual(info.V, tt.want.V) ||
				!reflect.DeepEqual(info.CT, tt.want.CT) ||
				!reflect.DeepEqual(info.MT, tt.want.MT) ||
				!reflect.DeepEqual(info.DT, tt.want.DT) ||
				!reflect.DeepEqual(info.UxList, tt.want.UxList) ||
				!reflect.DeepEqual(info.IdxList, tt.want.IdxList) ||
				!reflect.DeepEqual(info.KwList, tt.want.KwList) {
				infoStr := tsq.PrettyJSON(info)
				wantStr := tsq.PrettyJSON(tt.want)
				t.Errorf("got = %s, want %s", infoStr, wantStr)
			}
		})
	}
}

// TableInfoMock 用于字段比对
// TableInfoReal 用于真实填充
// parseAnnotationsForTest 用于测试 parseAnnotations

type TableInfoMock struct {
	Table    string
	ID       string
	CustomID bool
	Version  string
	CT       string
	MT       string
	DT       string
	UxList   []string
	IdxList  []string
	KwList   []string
}

type TableInfoReal struct {
	Table    string
	ID       string
	CustomID bool
	Version  string
	CT       string
	MT       string
	DT       string
	UxList   []tsq.IndexInfo
	IdxList  []tsq.IndexInfo
	KwList   []string
}

func (r *TableInfoReal) ToMock() TableInfoMock {
	mock := TableInfoMock{
		Table:    r.Table,
		ID:       r.ID,
		CustomID: r.CustomID,
		Version:  r.Version,
		CT:       r.CT,
		MT:       r.MT,
		DT:       r.DT,
		KwList:   r.KwList,
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
		return m.Version
	case "CT":
		return m.CT
	case "MT":
		return m.MT
	case "DT":
		return m.DT
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
