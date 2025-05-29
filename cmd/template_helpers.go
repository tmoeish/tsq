package cmd

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/serenize/snaker"
	"github.com/tmoeish/tsq"
)

// TemplateFuncs 返回模板中可用的函数映射
func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"ToUpper":      strings.ToUpper,
		"ToLower":      strings.ToLower,
		"UpperInitial": upperInitial,
		"LowerInitial": lowerInitial,
		"CamelToSnake": snaker.CamelToSnake,
		"FieldType":    fieldType,
		"PointerType":  pointerType,
		"ListType":     listType,
		"PageRespType": pageRespType,
		"JoinAnd":      joinAnd,
		"Sub1":         sub1,
		"FiledsToCols": FiledsToCols,
	}
}

// upperInitial 将字符串首字母大写
func upperInitial(s string) string {
	if s == "" {
		return s
	}

	return strings.ToUpper(s[:1]) + s[1:]
}

// lowerInitial 将字符串首字母小写
func lowerInitial(s string) string {
	if s == "" {
		return s
	}

	return strings.ToLower(s[:1]) + s[1:]
}

// fieldType 返回字段的Go类型字符串
func fieldType(field tsq.FieldInfo) string {
	pkg := field.Type.Package
	typeName := field.Type.TypeName

	if pkg.Path == "" {
		// 原始类型
		return typeName
	}

	if pkg.Name == "" {
		// 当前包的类型
		return typeName
	}

	// 外部包的类型
	return fmt.Sprintf("%s.%s", pkg.Name, typeName)
}

// pointerType 返回指针类型字符串
func pointerType(typeName string) string {
	return fmt.Sprintf("*%s", typeName)
}

// listType 返回列表类型字符串
func listType(typeName string) string {
	return fmt.Sprintf("[]*%s", typeName)
}

// pageRespType 返回分页响应类型字符串
func pageRespType(typeName string) string {
	return fmt.Sprintf("tsq.PageResp[%s]", typeName)
}

// joinAnd 将字符串切片或字符串用 And 连接
func joinAnd(v any) string {
	switch vv := v.(type) {
	case []string:
		return strings.Join(vv, "And")
	case string:
		return vv // 已经是字符串直接返回
	default:
		return ""
	}
}

// sub1 返回 n-1
func sub1(n int) int {
	return n - 1
}

func FiledsToCols(data *tsq.StructInfo, fields []string) string {
	cols := make([]string, len(fields))
	for i, field := range fields {
		cols[i] = fmt.Sprintf("`%s`", data.FieldMap[field].Column)
	}

	return fmt.Sprintf("%s", strings.Join(cols, ", "))
}
