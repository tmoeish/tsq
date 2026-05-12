package genmodel

import "strings"

type StructInfo struct {
	*TableMeta

	TypeInfo  TypeInfo
	ImportMap map[string]string
	Fields    []FieldInfo
	FieldMap  map[string]FieldInfo

	Recv       string
	TSQVersion string
}

type TypeInfo struct {
	Package  PackageInfo
	TypeName string
}

func (t TypeInfo) String() string {
	if t.Package.Path == "" {
		return t.TypeName
	}

	return t.Package.Path + "." + t.TypeName
}

type PackageInfo struct {
	Path string
	Name string
}

type FieldInfo struct {
	Name      string
	Type      TypeInfo
	Column    string
	JsonTag   string
	Tags      []string
	IsArray   bool
	IsPointer bool
}

func (f FieldInfo) String() string {
	sb := new(strings.Builder)
	sb.WriteString(f.Name)
	sb.WriteString(" ")

	if f.IsArray {
		sb.WriteString("[]")
	}

	if f.IsPointer {
		sb.WriteString("*")
	}

	sb.WriteString(f.Type.String())

	return sb.String()
}

type IndexInfo struct {
	Name       string
	SourceName string
	Fields     []string
	IsSet      bool
}

type IndexFuncNames struct {
	Name              string
	Fields            []string
	ListSetFunc       string
	PageSetFunc       string
	ListActiveSetFunc string
	PageActiveSetFunc string
}

func (s *StructInfo) SetTSQVersion(version string) {
	if s == nil {
		return
	}

	s.TSQVersion = version
}

type TableMeta struct {
	IsResult       bool
	Table          string
	AI             bool
	PK             string
	VersionField   string
	CreatedAtField string
	UpdatedAtField string
	DeletedAtField string
	SearchColumns  []string
	UxList         UxList
	IdxList        IdxList
	QueryList      IdxList
}

type UxList []IndexInfo

type IdxList []IndexInfo
