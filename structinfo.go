package tsq

import "strings"

// ================================================
// 代码生成相关数据结构
// ================================================

// StructInfo 表示用于代码生成的结构体信息
type StructInfo struct {
	*TableInfo // 表元数据，如果为 nil 则不是表结构

	// 基础类型信息
	TypeInfo  TypeInfo             // 结构体的类型信息
	ImportMap map[string]string    // 导入映射：pkg path => pkg alias
	Fields    []FieldInfo          // 字段列表（已排序）
	FieldMap  map[string]FieldInfo // 字段映射：field name => FieldInfo

	// 模板渲染辅助字段
	Recv       string // 结构体接收器名称，如 "u" for User
	TSQVersion string // TSQ 版本号
}

// TypeInfo 表示类型信息
type TypeInfo struct {
	Package  PackageInfo // 包信息
	TypeName string      // 类型名称
}

// String 返回类型的字符串表示
func (t TypeInfo) String() string {
	if t.Package.Path == "" {
		return t.TypeName
	}

	return t.Package.Path + "." + t.TypeName
}

// PackageInfo 表示包信息
type PackageInfo struct {
	Path string // 包路径
	Name string // 包名
}

// FieldInfo 表示字段信息
type FieldInfo struct {
	Name      string   // 字段名
	Type      TypeInfo // 字段类型
	Column    string   // 数据库列名
	JsonTag   string   // JSON 标签
	Tags      []string // 其他标签
	IsArray   bool     // 是否为数组类型
	IsPointer bool     // 是否为指针类型
}

// String 返回字段的字符串表示
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

// IndexInfo 表示索引信息
type IndexInfo struct {
	Name   string   // 索引名称
	Fields []string // 字段列表
	IsSet  bool     // 是否为 set 查询
}

// IndexFuncNames 用于模板渲染的批量索引函数名
// 只做命名拼接，不做业务逻辑
// 如 ListByUserIdAndItemIdSet
//
//	PageByUserIdAndItemIdSet
//	ListActiveByUserIdAndItemIdSet
//	PageActiveByUserIdAndItemIdSet
type IndexFuncNames struct {
	Name              string   // 原始索引名
	Fields            []string // 字段名
	ListSetFunc       string   // ListByUserIdAndItemIdSet
	PageSetFunc       string   // PageByUserIdAndItemIdSet
	ListActiveSetFunc string   // ListActiveByUserIdAndItemIdSet
	PageActiveSetFunc string   // PageActiveByUserIdAndItemIdSet
}

// SetTSQVersion 设置 TSQ 版本
func (s *StructInfo) SetTSQVersion(version string) {
	s.TSQVersion = version
}

// TableInfo 表示表的元数据信息
type TableInfo struct {
	Table     string   // 表名
	AI        bool     // ID auto-increment
	ID        string   // 主键字段名
	V         string   // 版本字段名
	CT        string   // 创建时间字段名
	MT        string   // 修改时间字段名
	DT        string   // 删除时间字段名
	KwList    []string // 关键词搜索字段列表
	UxList    UxList   // 唯一约束列表
	IdxList   IdxList  // 索引列表
	QueryList IdxList  // 查询索引列表
}

// UxList 唯一约束列表
type UxList []IndexInfo

// IdxList 索引列表
type IdxList []IndexInfo
