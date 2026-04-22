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
		"CamelToSnake":             snaker.CamelToSnake,
		"FieldType":                fieldType,
		"PointerType":              pointerType,
		"ListType":                 listType,
		"PageRespType":             pageRespType,
		"JoinAnd":                  joinAnd,
		"Sub1":                     sub1,
		"FieldToCol":               FieldToCol,
		"FieldsToCols":             FieldsToCols,
		"HasImport":                HasImport,
		"NeedsGeneratedTimeImport": NeedsGeneratedTimeImport,
		"GeneratedSQLRef":          GeneratedSQLRef,
		"GeneratedTimeRef":         GeneratedTimeRef,
		"TimestampNowValue":        TimestampNowValue,
		"SoftDeleteParamType":      SoftDeleteParamType,
		"SoftDeleteParamSetExpr":   SoftDeleteParamSetExpr,
		"SoftDeleteNowValue":       SoftDeleteNowValue,
		"SoftDeleteActiveExpr":     SoftDeleteActiveExpr,
		"SoftDeleteActiveCond":     SoftDeleteActiveCond,
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

func FieldToCol(data *tsq.StructInfo, field string) string {
	return fmt.Sprintf("%q", data.FieldMap[field].Column)
}

func FieldsToCols(data *tsq.StructInfo, fields []string) string {
	cols := make([]string, len(fields))
	for i, field := range fields {
		cols[i] = FieldToCol(data, field)
	}

	return strings.Join(cols, ", ")
}

func HasImport(data *tsq.StructInfo, importPath string) bool {
	if data == nil {
		return false
	}

	_, ok := data.ImportMap[importPath]

	return ok
}

func NeedsGeneratedTimeImport(data *tsq.StructInfo) bool {
	if data == nil {
		return false
	}

	return HasImport(data, importPathTime) || data.CT != "" || data.MT != "" || data.DT != ""
}

func GeneratedSQLRef(name string) string {
	return generatedSQLAlias + "." + name
}

func GeneratedTimeRef(name string) string {
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
		"dt field %s has unsupported type %s; supported types are int64, uint64, *time.Time, sql.NullTime, null.Time",
		field.Name,
		fieldType(field),
	)
}

func ValidateManagedFields(data *tsq.StructInfo) error {
	if data == nil || data.TableInfo == nil || data.IsDTO {
		return nil
	}

	for _, item := range []struct {
		name string
		role string
	}{
		{name: data.CT, role: "ct"},
		{name: data.MT, role: "mt"},
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

	if data.DT == "" {
		return nil
	}

	field, ok := data.FieldMap[data.DT]
	if !ok {
		return errors.Errorf("dt field %s not found in %s", data.DT, data.TypeInfo.TypeName)
	}

	return errors.Trace(validateSoftDeleteField(field))
}

func TimestampNowValue(field tsq.FieldInfo) string {
	switch managedTimestampKind(field) {
	case "time":
		return GeneratedTimeRef("Now()")
	case "time_ptr":
		return fmt.Sprintf("tsq.TimePtr(%s)", GeneratedTimeRef("Now()"))
	case "sql_null_time":
		return fmt.Sprintf("%s.NullTime{Time: %s, Valid: true}", generatedSQLAlias, GeneratedTimeRef("Now()"))
	case "null_time":
		return fmt.Sprintf("%s.TimeFrom(%s)", field.Type.Package.Name, GeneratedTimeRef("Now()"))
	default:
		panic(fmt.Sprintf("unsupported timestamp field type: %s", fieldType(field)))
	}
}

func SoftDeleteParamType(field tsq.FieldInfo) string {
	return fieldType(field)
}

func SoftDeleteParamSetExpr(param string, field tsq.FieldInfo) string {
	switch softDeleteKind(field) {
	case "integer":
		return param + " != 0"
	case "time_ptr":
		return param + " != nil"
	case "sql_null_time", "null_time":
		return param + ".Valid"
	default:
		panic(fmt.Sprintf("unsupported dt field type: %s", fieldType(field)))
	}
}

func SoftDeleteNowValue(field tsq.FieldInfo) string {
	switch softDeleteKind(field) {
	case "integer":
		if field.Type.TypeName == "uint64" {
			return fmt.Sprintf("uint64(%s)", GeneratedTimeRef("Now().UnixNano()"))
		}

		return GeneratedTimeRef("Now().UnixNano()")
	case "time_ptr":
		return fmt.Sprintf("tsq.TimePtr(%s)", GeneratedTimeRef("Now()"))
	case "sql_null_time":
		return fmt.Sprintf("%s.NullTime{Time: %s, Valid: true}", generatedSQLAlias, GeneratedTimeRef("Now()"))
	case "null_time":
		return fmt.Sprintf("%s.TimeFrom(%s)", field.Type.Package.Name, GeneratedTimeRef("Now()"))
	default:
		panic(fmt.Sprintf("unsupported dt field type: %s", fieldType(field)))
	}
}

func SoftDeleteActiveExpr(recv, fieldName string, field tsq.FieldInfo) string {
	target := fmt.Sprintf("%s.%s", recv, fieldName)

	switch softDeleteKind(field) {
	case "integer":
		return target + " == 0"
	case "time_ptr":
		return target + " == nil"
	case "sql_null_time", "null_time":
		return "!" + target + ".Valid"
	default:
		panic(fmt.Sprintf("unsupported dt field type: %s", fieldType(field)))
	}
}

func SoftDeleteActiveCond(typeName, fieldName string, field tsq.FieldInfo) string {
	col := fmt.Sprintf("%s_%s", typeName, fieldName)

	switch softDeleteKind(field) {
	case "integer":
		return col + ".EQ(0)"
	case "time_ptr", "sql_null_time", "null_time":
		return col + ".IsNull()"
	default:
		panic(fmt.Sprintf("unsupported dt field type: %s", fieldType(field)))
	}
}
