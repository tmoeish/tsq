package parser

import (
	"fmt"
	"go/ast"
	"reflect"
	"strings"

	"github.com/tmoeish/tsq/v4/internal/genmodel"
)

// parseNamedFields parses the named (non-embedded) fields of a struct.
func parseNamedFields(
	packageAliases map[string]genmodel.PackageInfo,
	currentPkg genmodel.PackageInfo,
	structType *ast.StructType,
) (map[string]genmodel.FieldInfo, error) {
	fields := make(map[string]genmodel.FieldInfo)

	for _, field := range structType.Fields.List {
		// Skip embedded fields (no name).
		if len(field.Names) == 0 {
			continue
		}

		// Parse field tags.
		fieldTags := parseFieldTags(field.Tag)

		// Skip fields that are not part of the table.
		if shouldSkipField(fieldTags) {
			continue
		}

		for _, name := range field.Names {
			fieldName := name.Name

			// Reject duplicate fields.
			if _, exists := fields[fieldName]; exists {
				return nil, NewDuplicateFieldError(fieldName, "struct")
			}

			// Parse the field type.
			isPointer, isArray, packagePath, typeName, err := parseFieldType(field.Type)
			if err != nil {
				return nil, err
			}

			// Resolve the field's package.
			typePackage, err := resolveFieldPackage(packagePath, typeName, packageAliases, currentPkg)
			if err != nil {
				return nil, err
			}

			// Build the field.
			field := genmodel.FieldInfo{
				Name:      fieldName,
				IsPointer: isPointer,
				IsArray:   isArray,
				Type:      genmodel.TypeInfo{Package: typePackage, TypeName: typeName},
				Column:    getColumnName(fieldTags),
				JsonTag:   getJsonTagName(fieldTags, fieldName),
			}

			fields[fieldName] = field
		}
	}

	return fields, nil
}

// FieldTags holds the parsed struct tags of one field.
type FieldTags struct {
	DB   string
	TSQ  string
	JSON string
}

// parseFieldTags parses the struct tag of a field.
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

// shouldSkipField reports whether a field is excluded from the table.
func shouldSkipField(tags FieldTags) bool {
	// Skip fields with neither db nor tsq tags.
	if len(tags.DB)+len(tags.TSQ) == 0 {
		return true
	}

	// Skip fields explicitly marked as ignored.
	if tags.DB == TagIgnore || tags.TSQ == TagIgnore {
		return true
	}

	return false
}

// getColumnName returns the column name of a field.
func getColumnName(tags FieldTags) string {
	if tags.TSQ != "" {
		return tags.TSQ
	}

	// Take the column name from the db tag, dropping the options.
	dbTag := tags.DB
	if idx := strings.Index(dbTag, ","); idx >= 0 {
		dbTag = dbTag[:idx]
	}

	return dbTag
}

// getJsonTagName returns the JSON name of a field.
func getJsonTagName(tags FieldTags, fieldName string) string {
	jsonTag := tags.JSON

	// Take the name from the json tag, dropping the options.
	if idx := strings.Index(jsonTag, ","); idx >= 0 {
		jsonTag = jsonTag[:idx]
	}

	// Fall back to the field name, matching encoding/json.
	if jsonTag == "" {
		jsonTag = fieldName
	}

	return jsonTag
}

// resolveFieldPackage resolves the package a field type belongs to.
func resolveFieldPackage(
	packagePath string,
	typeName string,
	packageAliases map[string]genmodel.PackageInfo,
	currentPkg genmodel.PackageInfo,
) (genmodel.PackageInfo, error) {
	if packagePath == "" {
		// Primitive type?
		if _, isPrimitive := PrimitiveTypes[typeName]; !isPrimitive {
			// Not primitive, so it must live in the current package.
			return currentPkg, nil
		}
		// Primitive types have no package.
		return genmodel.PackageInfo{}, nil
	}

	// Resolve through the import aliases.
	pkg, ok := packageAliases[packagePath]
	if !ok {
		return genmodel.PackageInfo{}, fmt.Errorf(
			"unknown package alias %q for field type %s",
			packagePath,
			typeName,
		)
	}

	return pkg, nil
}

// parseEmbeddedFields collects the embedded struct types of a struct.
func parseEmbeddedFields(
	packageAliases map[string]genmodel.PackageInfo,
	currentPkg genmodel.PackageInfo,
	structType *ast.StructType,
) (map[genmodel.TypeInfo]bool, error) {
	embeddedTypes := make(map[genmodel.TypeInfo]bool)

	for _, field := range structType.Fields.List {
		// Only embedded fields (no name).
		if len(field.Names) != 0 {
			continue
		}

		// Parse the embedded type.
		_, _, packagePath, typeName, err := parseFieldType(field.Type)
		if err != nil {
			return nil, err
		}

		var embeddedType genmodel.TypeInfo
		if packagePath == "" {
			embeddedType = genmodel.TypeInfo{
				Package:  currentPkg,
				TypeName: typeName,
			}
		} else {
			embeddedType = genmodel.TypeInfo{
				Package:  packageAliases[packagePath],
				TypeName: typeName,
			}
		}

		// Reject duplicate embedded types.
		if _, exists := embeddedTypes[embeddedType]; exists {
			return nil, NewDuplicateEmbeddedError(embeddedType.TypeName, "struct")
		}

		embeddedTypes[embeddedType] = true
	}

	return embeddedTypes, nil
}

// parseFieldType parses a field type expression.
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
		// Plain identifier: int, string, CustomType
		return false, false, "", t.Name, nil

	case *ast.SelectorExpr:
		// Selector: pkg.Type
		isPointer, isArray, packagePath, typeName, err := parseSelectorExpr(t)
		return isPointer, isArray, packagePath, typeName, err

	case *ast.ArrayType:
		// Slice: []Type
		if _, nestedArray := t.Elt.(*ast.ArrayType); nestedArray {
			return false, false, "", "", NewFieldUnsupportedCompositionError("nested slices/arrays are not supported")
		}

		isPointer, _, packagePath, typeName, err := parseFieldType(t.Elt)
		if err != nil {
			return false, false, "", "", err
		}

		return isPointer, true, packagePath, typeName, nil

	case *ast.StarExpr:
		// Pointer: *Type
		switch t.X.(type) {
		case *ast.ArrayType:
			return false, false, "", "", NewFieldUnsupportedCompositionError("pointer-to-slice fields are not supported")
		case *ast.StarExpr:
			return false, false, "", "", NewFieldUnsupportedCompositionError("multi-level pointers are not supported")
		}

		_, isArray, packagePath, typeName, err := parseFieldType(t.X)
		if err != nil {
			return false, false, "", "", err
		}

		if isArray {
			return false, false, "", "", NewFieldUnsupportedCompositionError("pointer-to-slice fields are not supported")
		}

		return true, false, packagePath, typeName, nil

	default:
		return false, false, "", "", NewFieldUnsupportedTypeError(t)
	}
}

// parseSelectorExpr parses a pkg.Type selector expression.
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

	// Anything but a plain identifier would be a nested selector,
	// which is not supported.
	return false, false, "", "", NewFieldInvalidSelectorError(selExpr.X)
}
