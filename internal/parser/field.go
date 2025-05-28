package parser

import (
	"go/ast"
	"reflect"
	"strings"

	"github.com/juju/errors"
	"github.com/tmoeish/tsq"
)

// parseNamedFields 解析具名字段
func parseNamedFields(
	packageAliases map[string]tsq.PackageInfo,
	currentPkg tsq.PackageInfo,
	structType *ast.StructType,
) (map[string]tsq.FieldInfo, error) {
	fields := make(map[string]tsq.FieldInfo)

	for _, field := range structType.Fields.List {
		// 跳过嵌入字段（没有名称）
		if len(field.Names) == 0 {
			continue
		}

		// 解析字段标签
		fieldTags := parseFieldTags(field.Tag)

		// 跳过不需要的字段
		if shouldSkipField(fieldTags) {
			continue
		}

		for _, name := range field.Names {
			fieldName := name.Name

			// 检查重复字段
			if _, exists := fields[fieldName]; exists {
				return nil, NewDuplicateFieldError(fieldName, "struct")
			}

			// 解析字段类型
			isPointer, isArray, packagePath, typeName, err := parseFieldType(field.Type)
			if err != nil {
				return nil, errors.Trace(err)
			}

			// 解析字段包信息
			typePackage := resolveFieldPackage(packagePath, typeName, packageAliases, currentPkg)

			// 创建字段对象
			field := tsq.FieldInfo{
				Name:      fieldName,
				IsPointer: isPointer,
				IsArray:   isArray,
				Type:      tsq.TypeInfo{Package: typePackage, TypeName: typeName},
				Column:    getColumnName(fieldTags),
				JsonTag:   getJsonTagName(fieldTags, fieldName),
			}

			fields[fieldName] = field
		}
	}

	return fields, nil
}

// FieldTags 表示字段标签信息
type FieldTags struct {
	DB   string
	TSQ  string
	JSON string
}

// parseFieldTags 解析字段标签
func parseFieldTags(tagValue *ast.BasicLit) FieldTags {
	if tagValue == nil {
		return FieldTags{}
	}

	tagString := tagValue.Value
	tags := reflect.StructTag(strings.Trim(tagString, "`"))

	return FieldTags{
		DB:   tags.Get(TagDB),
		TSQ:  tags.Get(TagTSQ),
		JSON: tags.Get(TagJSON),
	}
}

// shouldSkipField 判断是否应该跳过字段
func shouldSkipField(tags FieldTags) bool {
	// 跳过没有 db 和 tsq 标签的字段
	if len(tags.DB)+len(tags.TSQ) == 0 {
		return true
	}

	// 跳过标记为忽略的字段
	if tags.DB == TagIgnore || tags.TSQ == TagIgnore {
		return true
	}

	return false
}

// getColumnName 获取列名
func getColumnName(tags FieldTags) string {
	if tags.TSQ != "" {
		return tags.TSQ
	}

	// 从 db 标签中提取列名（去掉其他选项）
	dbTag := tags.DB
	if idx := strings.Index(dbTag, ","); idx >= 0 {
		dbTag = dbTag[:idx]
	}

	return dbTag
}

// getJsonTagName 获取 JSON 标签名
func getJsonTagName(tags FieldTags, fieldName string) string {
	jsonTag := tags.JSON

	// 提取 JSON 标签名（去掉其他选项）
	if idx := strings.Index(jsonTag, ","); idx >= 0 {
		jsonTag = jsonTag[:idx]
	}

	// 如果 JSON 标签为空，使用字段名（与 Go 的 json 标签保持一致）
	if jsonTag == "" {
		jsonTag = fieldName
	}

	return jsonTag
}

// resolveFieldPackage 解析字段的包信息
func resolveFieldPackage(
	packagePath string,
	typeName string,
	packageAliases map[string]tsq.PackageInfo,
	currentPkg tsq.PackageInfo,
) tsq.PackageInfo {
	if packagePath == "" {
		// 检查是否为原始类型
		if _, isPrimitive := PrimitiveTypes[typeName]; !isPrimitive {
			// 如果不是原始类型，则必须是当前包下的类型
			return currentPkg
		}
		// 原始类型返回空包
		return tsq.PackageInfo{}
	}

	// 使用包别名解析
	return packageAliases[packagePath]
}

// parseEmbeddedFields 解析嵌入字段
func parseEmbeddedFields(
	packageAliases map[string]tsq.PackageInfo,
	currentPkg tsq.PackageInfo,
	structType *ast.StructType,
) (map[tsq.TypeInfo]bool, error) {
	embeddedTypes := make(map[tsq.TypeInfo]bool)

	for _, field := range structType.Fields.List {
		// 只处理嵌入字段（没有名称）
		if len(field.Names) != 0 {
			continue
		}

		// 解析嵌入字段类型
		_, _, packagePath, typeName, err := parseFieldType(field.Type)
		if err != nil {
			return nil, errors.Trace(err)
		}

		var embeddedType tsq.TypeInfo
		if packagePath == "" {
			embeddedType = tsq.TypeInfo{
				Package:  currentPkg,
				TypeName: typeName,
			}
		} else {
			embeddedType = tsq.TypeInfo{
				Package:  packageAliases[packagePath],
				TypeName: typeName,
			}
		}

		// 检查重复的嵌入类型
		if _, exists := embeddedTypes[embeddedType]; exists {
			return nil, NewDuplicateEmbeddedError(embeddedType.TypeName, "struct")
		}

		embeddedTypes[embeddedType] = true
	}

	return embeddedTypes, nil
}

// parseFieldType 解析字段类型表达式
func parseFieldType(
	expr ast.Expr,
) (
	isPointer bool,
	isArray bool,
	packagePath string,
	typeName string,
	err error,
) {
	switch t := expr.(type) {
	case *ast.Ident:
		// 简单标识符：int, string, CustomType
		return false, false, "", t.Name, nil

	case *ast.SelectorExpr:
		// 选择器表达式：pkg.Type
		isPointer, isArray, packagePath, typeName, err := parseSelectorExpr(t)
		return isPointer, isArray, packagePath, typeName, err

	case *ast.ArrayType:
		// 数组类型：[]Type
		isPointer, _, packagePath, typeName, err := parseFieldType(t.Elt)
		return isPointer, true, packagePath, typeName, err

	case *ast.StarExpr:
		// 指针类型：*Type
		_, isArray, packagePath, typeName, err := parseFieldType(t.X)
		return true, isArray, packagePath, typeName, err

	default:
		return false, false, "", "", NewFieldUnsupportedTypeError(t)
	}
}

// parseSelectorExpr 解析选择器表达式
func parseSelectorExpr(
	selExpr *ast.SelectorExpr,
) (
	isPointer bool,
	isArray bool,
	packagePath string,
	typeName string,
	err error,
) {
	if ident, ok := selExpr.X.(*ast.Ident); ok {
		return false, false, ident.Name, selExpr.Sel.Name, nil
	}

	// 如果不是简单的标识符，可能是嵌套的选择器表达式
	// 目前不支持这种情况
	return false, false, "", "", NewFieldInvalidSelectorError(selExpr.X)
}
