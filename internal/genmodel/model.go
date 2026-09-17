package genmodel

import "strings"

type StructInfo struct {
	*TableMeta

	TypeInfo     TypeInfo
	Imports      map[string]string
	Fields       []FieldInfo
	FieldsByName map[string]FieldInfo

	Receiver   string
	TSQVersion string

	// Schema is the physical column definition of a table, filled in by the
	// generator after parsing because it needs type information.
	Schema []SchemaColumn
}

// SchemaColumn is one physical column of a table.
type SchemaColumn struct {
	Name          string
	Kind          string
	Bits          int
	Unsigned      bool
	Nullable      bool
	Size          int
	RawType       string
	PrimaryKey    bool
	AutoIncrement bool
	Default       string
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
	JSONTag   string
	Tags      []string
	IsSlice   bool
	IsPointer bool
}

func (f FieldInfo) String() string {
	sb := new(strings.Builder)
	sb.WriteString(f.Name)
	sb.WriteString(" ")

	if f.IsSlice {
		sb.WriteString("[]")
	}

	if f.IsPointer {
		sb.WriteString("*")
	}

	sb.WriteString(f.Type.String())

	return sb.String()
}

type IndexInfo struct {
	Name        string
	IndexName   string
	Fields      []string
	LastFieldIn bool
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
	AutoIncrement  bool
	PrimaryKey     string
	VersionField   string
	CreatedAtField string
	UpdatedAtField string
	DeletedAtField string
	SearchColumns  []string
	Uniques        []IndexInfo
	Indexes        []IndexInfo
	Queries        []IndexInfo
}
