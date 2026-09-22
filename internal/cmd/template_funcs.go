package cmd

import (
	"fmt"
	"strings"
	"text/template"
	"unicode"

	"github.com/tmoeish/tsq/v5/internal/genmodel"
)

const (
	importPathDatabaseSQL = "database/sql"
	importPathTime        = "time"
	nullbioImportPrefix   = "gopkg.in/nullbio/null"
	generatedSQLAlias     = "tsqsql"
	generatedTimeAlias    = "tsqtime"
)

// columnKindRefs spells out each dialect.ColumnKind constant in full, so that
// TestGeneratedCodeReferencesOnlyRealSymbols checks every name generated code can
// reference; a prefix concatenated in the template would hide a renamed constant.
var columnKindRefs = map[string]string{
	"bool":   "tsqdialect.KindBool",
	"bytes":  "tsqdialect.KindBytes",
	"float":  "tsqdialect.KindFloat",
	"int":    "tsqdialect.KindInt",
	"string": "tsqdialect.KindString",
	"time":   "tsqdialect.KindTime",
}

// columnKindRef returns the qualified dialect constant for a column kind.
func columnKindRef(kind string) (string, error) {
	ref, ok := columnKindRefs[kind]
	if !ok {
		return "", fmt.Errorf("unknown column kind %q", kind)
	}

	return ref, nil
}

// funcMap returns the helper functions available to the templates.
func funcMap() template.FuncMap {
	return template.FuncMap{
		"UpperInitial":             upperInitial,
		"FillRef":                  fillRef,
		"ColumnKindRef":            columnKindRef,
		"FieldVarName":             fieldVarName,
		"ColumnType":               columnType,
		"ValueType":                valueType,
		"Fetchable":                fetchable,
		"FieldSliceVarName":        fieldSliceVarName,
		"FieldType":                fieldType,
		"JoinAnd":                  joinAnd,
		"Sub1":                     sub1,
		"FieldToCol":               fieldToCol,
		"IndexFieldsToCols":        indexFieldsToCols,
		"FieldsToCols":             fieldsToCols,
		"NeedsGeneratedTimeImport": needsGeneratedTimeImport,
		"NeedsGeneratedSQLImport":  needsGeneratedSQLImport,
		"SoftDeleteActiveExpr":     softDeleteActiveExpr,
	}
}

// upperInitial upper-cases the first letter of s.
func upperInitial(s string) string {
	if s == "" {
		return s
	}

	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])

	return string(runes)
}

// lowerInitial lower-cases the leading word of an exported Go name, treating a run
// of capitals as one initialism: ID -> id, UID -> uid, URLPath -> urlPath,
// Title -> title.
func lowerInitial(s string) string {
	runes := []rune(s)

	n := 0
	for n < len(runes) && unicode.IsUpper(runes[n]) {
		n++
	}

	// In URLPath the P starts the next word, so it stays upper case.
	if n > 1 && n < len(runes) && unicode.IsLower(runes[n]) {
		n--
	}

	for i := range n {
		runes[i] = unicode.ToLower(runes[i])
	}

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

// generatedIdentifiers are the names a generated lookup already uses: its fixed
// parameters, its receiver, and the package its body calls. A parameter named after
// a field must not take one.
var generatedIdentifiers = map[string]struct{}{"ctx": {}, "db": {}, "t": {}, "tsq": {}}

func fieldVarName(fieldName string) string {
	name := lowerInitial(fieldName)
	if name == "" {
		name = "v"
	}

	if _, ok := goKeywords[name]; ok {
		return name + "_"
	}

	if _, ok := generatedIdentifiers[name]; ok {
		return name + "_"
	}

	return name
}

// columnType is the Go type of the generated column field for f on table type
// owner.
func columnType(owner string, f genmodel.FieldInfo) string {
	if f.NullValue != "" {
		return fmt.Sprintf("tsq.NullColumn[%s, %s]", owner, f.NullValue)
	}

	return fmt.Sprintf("tsq.Column[%s, %s]", owner, fieldType(f))
}

// valueType is the type a query compares f with: its value type when it can be
// NULL, otherwise the field type.
func valueType(f genmodel.FieldInfo) string {
	if f.NullValue != "" {
		return f.NullValue
	}

	return fieldType(f)
}

// fetchable reports whether TableOf.FetchBy can read by f: a NOT NULL field of a
// comparable type.
func fetchable(f genmodel.FieldInfo) bool {
	return f.NullValue == "" && !f.IsSlice
}

func fieldSliceVarName(fieldName string) string {
	return fieldVarName(fieldName) + "s"
}

// fieldType returns the Go type expression for a field.
func fieldType(field genmodel.FieldInfo) string {
	if field.Spelled != "" {
		return field.Spelled
	}

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

	if field.IsSlice {
		fullTypeName = listType(fullTypeName)
	}

	return fullTypeName
}

// pointerType returns the pointer type expression.
func pointerType(typeName string) string {
	return fmt.Sprintf("*%s", typeName)
}

// listType returns the slice type expression.
func listType(typeName string) string {
	return fmt.Sprintf("[]%s", typeName)
}

// joinAnd joins a string slice (or passes a string through) with "And".
func joinAnd(v any) string {
	switch vv := v.(type) {
	case []string:
		return strings.Join(vv, "And")
	case string:
		return vv // already a string
	default:
		return ""
	}
}

// sub1 returns n-1.
func sub1(n int) int {
	return n - 1
}

func fieldToCol(data *genmodel.StructInfo, field string) string {
	return fmt.Sprintf("%q", data.FieldsByName[field].Column)
}

func fieldsToCols(data *genmodel.StructInfo, fields []string) string {
	cols := make([]string, len(fields))
	for i, field := range fields {
		cols[i] = fieldToCol(data, field)
	}

	return strings.Join(cols, ", ")
}

// indexFieldNames are the fields a unique or plain index covers: a soft-delete table
// leads with deleted_at, so a deleted row does not hold a unique value and a scan
// of live rows uses the index. A full-text index takes its fields as written:
// deleted_at is not text, and the live-row filter is the query's.
func indexFieldNames(data *genmodel.StructInfo, fields []string) []string {
	if data == nil || data.DeletedAtField == "" {
		return append([]string(nil), fields...)
	}

	result := make([]string, 0, len(fields)+1)

	result = append(result, data.DeletedAtField)
	for _, field := range fields {
		if field == data.DeletedAtField {
			continue
		}
		result = append(result, field)
	}

	return result
}

func indexFieldsToCols(data *genmodel.StructInfo, fields []string) string {
	return fieldsToCols(data, indexFieldNames(data, fields))
}

func needsGeneratedTimeImport(data *genmodel.StructInfo) bool {
	if data == nil {
		return false
	}

	return fieldsUse(data, importPathTime)
}

// fieldsUse reports whether a field type of data comes from importPath.
func fieldsUse(data *genmodel.StructInfo, importPath string) bool {
	alias := map[string]string{importPathTime: generatedTimeAlias, importPathDatabaseSQL: generatedSQLAlias}[importPath]

	for _, f := range data.Fields {
		if f.Type.Package.Path == importPath ||
			(alias != "" && (strings.Contains(f.NullValue, alias+".") || strings.Contains(f.Spelled, alias+"."))) {
			return true
		}
	}

	return false
}

// needsGeneratedSQLImport reports whether a result's field types render with the
// tsqsql alias. The table template imports it unconditionally for sql.ErrNoRows.
func needsGeneratedSQLImport(data *genmodel.StructInfo) bool {
	return data != nil && fieldsUse(data, importPathDatabaseSQL)
}

func managedTimestampKind(field genmodel.FieldInfo) string {
	if field.IsSlice {
		return ""
	}

	switch {
	case !field.IsPointer && field.Type.Package.Path == importPathTime && field.Type.TypeName == "Time":
		return "time"
	case field.IsPointer && field.Type.Package.Path == importPathTime && field.Type.TypeName == "Time":
		return "time_ptr"
	case !field.IsPointer && field.Type.Package.Path == importPathDatabaseSQL && field.Type.TypeName == "NullTime":
		return "sql_null_time"
	case !field.IsPointer && field.Type.Package.Path == importPathDatabaseSQL && field.Type.TypeName == "Null" &&
		isTimeTypeArg(field):
		// sql.Null[time.Time] is a nullable time like sql.NullTime.
		return "sql_null_time"
	case !field.IsPointer &&
		strings.HasPrefix(field.Type.Package.Path, nullbioImportPrefix) &&
		field.Type.TypeName == "Time":
		return "null_time"
	default:
		return ""
	}
}

// isTimeTypeArg reports whether a generic field's only type argument is time.Time,
// however the source file imports the time package.
func isTimeTypeArg(field genmodel.FieldInfo) bool {
	if len(field.TypeArgPackages) != 1 || field.TypeArgPackages[0].Path != importPathTime {
		return false
	}

	_, name, ok := strings.Cut(field.TypeArgs, ".")

	return ok && name == "Time"
}

func softDeleteKind(field genmodel.FieldInfo) string {
	switch managedTimestampKind(field) {
	case "time_ptr", "sql_null_time", "null_time":
		return managedTimestampKind(field)
	}

	if field.IsSlice || field.IsPointer || field.Type.Package.Path != "" {
		return ""
	}

	switch field.Type.TypeName {
	case "int64", "uint64":
		return "integer"
	default:
		return ""
	}
}

func validateTimestampField(field genmodel.FieldInfo, role string) error {
	if managedTimestampKind(field) != "" {
		return nil
	}

	return fmt.Errorf(
		"%s field %s has unsupported type %s; supported types are time.Time, *time.Time, sql.NullTime, sql.Null[time.Time], null.Time",
		role,
		field.Name,
		fieldType(field),
	)
}

func validateSoftDeleteField(field genmodel.FieldInfo) error {
	if softDeleteKind(field) != "" {
		return nil
	}

	return fmt.Errorf(
		"deleted_at field %s has unsupported type %s; supported types are int64, uint64, *time.Time, sql.NullTime, sql.Null[time.Time], null.Time",
		field.Name,
		fieldType(field),
	)
}

func validateManagedFields(data *genmodel.StructInfo) error {
	if data == nil || data.TableMeta == nil || data.IsResult {
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

		field, ok := data.FieldsByName[item.name]
		if !ok {
			return fmt.Errorf("%s field %s not found in %s", item.role, item.name, data.TypeInfo.TypeName)
		}

		if err := validateTimestampField(field, item.role); err != nil {
			return err
		}
	}

	if data.DeletedAtField == "" {
		return nil
	}

	field, ok := data.FieldsByName[data.DeletedAtField]
	if !ok {
		return fmt.Errorf("deleted_at field %s not found in %s", data.DeletedAtField, data.TypeInfo.TypeName)
	}

	if err := validateSoftDeleteField(field); err != nil {
		return err
	}

	if len(data.Uniques) > 0 && softDeleteKind(field) != "integer" {
		return fmt.Errorf(
			"deleted_at field %s in %s cannot use nullable time semantics with unique indexes; use int64 or uint64 tombstones for portable uniqueness",
			data.DeletedAtField,
			data.TypeInfo.TypeName,
		)
	}

	return nil
}

func softDeleteActiveExpr(recv, fieldName string, field genmodel.FieldInfo) string {
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

// fillRef writes the tsqdialect.Fill constant for a schema column's fill, spelled
// out so generated_symbols_test.go can check it exists.
func fillRef(fill string) string {
	switch fill {
	case "default":
		return "tsqdialect.FillDefault"
	case "generated":
		return "tsqdialect.FillGenerated"
	default:
		return "tsqdialect.FillCaller"
	}
}
