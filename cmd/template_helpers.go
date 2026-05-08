package cmd

import (
	"fmt"
	"strings"
	"text/template"
	"unicode"

	"github.com/juju/errors"
	"github.com/serenize/snaker"

	"github.com/tmoeish/tsq"
)

const (
	importPathDatabaseSQL = "database/sql"
	importPathTime        = "time"
	nullbioImportPrefix   = "gopkg.in/nullbio/null"
	generatedSQLAlias     = "tsqsql"
	generatedTimeAlias    = "tsqtime"
)

// TemplateFuncs 返回模板中可用的函数映射
func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"ToUpper":                  strings.ToUpper,
		"ToLower":                  strings.ToLower,
		"UpperInitial":             upperInitial,
		"LowerInitial":             lowerInitial,
		"FieldVarName":             fieldVarName,
		"FieldSliceVarName":        fieldSliceVarName,
		"CamelToSnake":             snaker.CamelToSnake,
		"FieldType":                fieldType,
		"PointerType":              pointerType,
		"ListType":                 listType,
		"PageRespType":             pageRespType,
		"JoinAnd":                  joinAnd,
		"Sub1":                     sub1,
		"FieldToCol":               fieldToCol,
		"FieldsToCols":             fieldsToCols,
		"HasImport":                hasImport,
		"NeedsGeneratedTimeImport": needsGeneratedTimeImport,
		"GeneratedSQLRef":          generatedSQLRef,
		"GeneratedTimeRef":         generatedTimeRef,
		"TimestampNowValue":        timestampNowValue,
		"SoftDeleteParamType":      softDeleteParamType,
		"SoftDeleteParamSetExpr":   softDeleteParamSetExpr,
		"SoftDeleteNowValue":       softDeleteNowValue,
		"SoftDeleteActiveExpr":     softDeleteActiveExpr,
		"SoftDeleteActiveCond":     softDeleteActiveCond,
	}
}

// upperInitial 将字符串首字母大写
func upperInitial(s string) string {
	if s == "" {
		return s
	}

	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])

	return string(runes)
}

// lowerInitial 将字符串首字母小写
func lowerInitial(s string) string {
	if s == "" {
		return s
	}

	runes := []rune(s)
	runes[0] = unicode.ToLower(runes[0])

	return string(runes)
}

var goKeywords = map[string]struct{}{
	"break":       {},
	"default":     {},
	"func":        {},
	"interface":   {},
	"select":      {},
	"case":        {},
	"defer":       {},
	"go":          {},
	"map":         {},
	"struct":      {},
	"chan":        {},
	"else":        {},
	"goto":        {},
	"package":     {},
	"switch":      {},
	"const":       {},
	"fallthrough": {},
	"if":          {},
	"range":       {},
	"type":        {},
	"continue":    {},
	"for":         {},
	"import":      {},
	"return":      {},
	"var":         {},
}

func fieldVarName(fieldName string) string {
	name := lowerInitial(fieldName)
	if name == "" {
		name = "v"
	}

	if _, ok := goKeywords[name]; ok {
		return name + "_"
	}

	return name
}

func fieldSliceVarName(fieldName string) string {
	return fieldVarName(fieldName) + "s"
}

// fieldType 返回字段的Go类型字符串
func fieldType(field tsq.FieldInfo) string {
	pkg := field.Type.Package
	typeName := field.Type.TypeName
	fullTypeName := typeName

	if pkg.Path != "" {
		switch pkg.Path {
		case importPathDatabaseSQL:
			fullTypeName = fmt.Sprintf("%s.%s", generatedSQLAlias, typeName)
		case importPathTime:
			fullTypeName = fmt.Sprintf("%s.%s", generatedTimeAlias, typeName)
		default:
			if pkg.Name != "" {
				fullTypeName = fmt.Sprintf("%s.%s", pkg.Name, typeName)
			}
		}
	}

	if field.IsPointer {
		fullTypeName = pointerType(fullTypeName)
	}

	if field.IsArray {
		fullTypeName = listType(fullTypeName)
	}

	return fullTypeName
}

// pointerType 返回指针类型字符串
func pointerType(typeName string) string {
	return fmt.Sprintf("*%s", typeName)
}

// listType 返回列表类型字符串
func listType(typeName string) string {
	return fmt.Sprintf("[]%s", typeName)
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

func fieldToCol(data *tsq.StructInfo, field string) string {
	return fmt.Sprintf("%q", data.FieldMap[field].Column)
}

func fieldsToCols(data *tsq.StructInfo, fields []string) string {
	cols := make([]string, len(fields))
	for i, field := range fields {
		cols[i] = fieldToCol(data, field)
	}

	return strings.Join(cols, ", ")
}

func hasImport(data *tsq.StructInfo, importPath string) bool {
	if data == nil {
		return false
	}

	_, ok := data.ImportMap[importPath]

	return ok
}

func needsGeneratedTimeImport(data *tsq.StructInfo) bool {
	if data == nil {
		return false
	}

	return hasImport(data, importPathTime) || data.CreatedAtField != "" || data.UpdatedAtField != "" || data.DeletedAtField != ""
}

func generatedSQLRef(name string) string {
	return generatedSQLAlias + "." + name
}

func generatedTimeRef(name string) string {
	return generatedTimeAlias + "." + name
}

func managedTimestampKind(field tsq.FieldInfo) string {
	if field.IsArray {
		return ""
	}

	switch {
	case !field.IsPointer && field.Type.Package.Path == importPathTime && field.Type.TypeName == "Time":
		return "time"
	case field.IsPointer && field.Type.Package.Path == importPathTime && field.Type.TypeName == "Time":
		return "time_ptr"
	case !field.IsPointer && field.Type.Package.Path == importPathDatabaseSQL && field.Type.TypeName == "NullTime":
		return "sql_null_time"
	case !field.IsPointer &&
		strings.HasPrefix(field.Type.Package.Path, nullbioImportPrefix) &&
		field.Type.TypeName == "Time":
		return "null_time"
	default:
		return ""
	}
}

func softDeleteKind(field tsq.FieldInfo) string {
	switch managedTimestampKind(field) {
	case "time_ptr", "sql_null_time", "null_time":
		return managedTimestampKind(field)
	}

	if field.IsArray || field.IsPointer || field.Type.Package.Path != "" {
		return ""
	}

	switch field.Type.TypeName {
	case "int64", "uint64":
		return "integer"
	default:
		return ""
	}
}

func validateTimestampField(field tsq.FieldInfo, role string) error {
	if managedTimestampKind(field) != "" {
		return nil
	}

	return errors.Errorf(
		"%s field %s has unsupported type %s; supported types are time.Time, *time.Time, sql.NullTime, null.Time",
		role,
		field.Name,
		fieldType(field),
	)
}

func validateSoftDeleteField(field tsq.FieldInfo) error {
	if softDeleteKind(field) != "" {
		return nil
	}

	return errors.Errorf(
		"deleted_at field %s has unsupported type %s; supported types are int64, uint64, *time.Time, sql.NullTime, null.Time",
		field.Name,
		fieldType(field),
	)
}

func validateManagedFields(data *tsq.StructInfo) error {
	if data == nil || data.TableInfo == nil || data.IsResult {
		return nil
	}

	for _, item := range []struct {
		name string
		role string
	}{
		{name: data.CreatedAtField, role: "created_at"},
		{name: data.UpdatedAtField, role: "updated_at"},
	} {
		if item.name == "" {
			continue
		}

		field, ok := data.FieldMap[item.name]
		if !ok {
			return errors.Errorf("%s field %s not found in %s", item.role, item.name, data.TypeInfo.TypeName)
		}

		if err := validateTimestampField(field, item.role); err != nil {
			return errors.Trace(err)
		}
	}

	if data.DeletedAtField == "" {
		return nil
	}

	field, ok := data.FieldMap[data.DeletedAtField]
	if !ok {
		return errors.Errorf("deleted_at field %s not found in %s", data.DeletedAtField, data.TypeInfo.TypeName)
	}

	if err := validateSoftDeleteField(field); err != nil {
		return errors.Trace(err)
	}

	if len(data.UxList) > 0 && softDeleteKind(field) != "integer" {
		return errors.Errorf(
			"deleted_at field %s in %s cannot use nullable time semantics with unique indexes; use int64 or uint64 tombstones for portable uniqueness",
			data.DeletedAtField,
			data.TypeInfo.TypeName,
		)
	}

	return nil
}

func timestampNowValue(field tsq.FieldInfo) string {
	switch managedTimestampKind(field) {
	case "time":
		return generatedTimeRef("Now()")
	case "time_ptr":
		return fmt.Sprintf("tsq.TimePtr(%s)", generatedTimeRef("Now()"))
	case "sql_null_time":
		return fmt.Sprintf("%s.NullTime{Time: %s, Valid: true}", generatedSQLAlias, generatedTimeRef("Now()"))
	case "null_time":
		return fmt.Sprintf("%s.TimeFrom(%s)", field.Type.Package.Name, generatedTimeRef("Now()"))
	default:
		panic(fmt.Sprintf("unsupported timestamp field type: %s", fieldType(field)))
	}
}

func softDeleteParamType(field tsq.FieldInfo) string {
	return fieldType(field)
}

func softDeleteParamSetExpr(param string, field tsq.FieldInfo) string {
	switch softDeleteKind(field) {
	case "integer":
		return param + " != 0"
	case "time_ptr":
		return param + " != nil"
	case "sql_null_time", "null_time":
		return param + ".Valid"
	default:
		panic(fmt.Sprintf("unsupported deleted_at field type: %s", fieldType(field)))
	}
}

func softDeleteNowValue(field tsq.FieldInfo) string {
	switch softDeleteKind(field) {
	case "integer":
		if field.Type.TypeName == "uint64" {
			return fmt.Sprintf("uint64(%s)", generatedTimeRef("Now().UnixNano()"))
		}

		return generatedTimeRef("Now().UnixNano()")
	case "time_ptr":
		return fmt.Sprintf("tsq.TimePtr(%s)", generatedTimeRef("Now()"))
	case "sql_null_time":
		return fmt.Sprintf("%s.NullTime{Time: %s, Valid: true}", generatedSQLAlias, generatedTimeRef("Now()"))
	case "null_time":
		return fmt.Sprintf("%s.TimeFrom(%s)", field.Type.Package.Name, generatedTimeRef("Now()"))
	default:
		panic(fmt.Sprintf("unsupported deleted_at field type: %s", fieldType(field)))
	}
}

func softDeleteActiveExpr(recv, fieldName string, field tsq.FieldInfo) string {
	target := fmt.Sprintf("%s.%s", recv, fieldName)

	switch softDeleteKind(field) {
	case "integer":
		return target + " == 0"
	case "time_ptr":
		return target + " == nil"
	case "sql_null_time", "null_time":
		return "!" + target + ".Valid"
	default:
		panic(fmt.Sprintf("unsupported deleted_at field type: %s", fieldType(field)))
	}
}

func softDeleteActiveCond(typeName, fieldName string, field tsq.FieldInfo) string {
	col := fmt.Sprintf("%s_%s", typeName, fieldName)

	switch softDeleteKind(field) {
	case "integer":
		return col + ".EQ(0)"
	case "time_ptr", "sql_null_time", "null_time":
		return col + ".IsNull()"
	default:
		panic(fmt.Sprintf("unsupported deleted_at field type: %s", fieldType(field)))
	}
}
