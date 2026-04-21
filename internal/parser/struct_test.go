package parser

import (
	"container/list"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/tmoeish/tsq"
)

func Test_parseStructDeclaration(t *testing.T) {
	source := `
package test

type User struct {
	ID   int64  ` + "`" + `db:"id"` + "`" + `
	Name string ` + "`" + `db:"name"` + "`" + `
	BaseModel
}
`

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "test.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 找到 User 结构体
	var structType *ast.StructType

	var structName string

	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}

		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "User" {
				continue
			}

			structType = typeSpec.Type.(*ast.StructType)
			structName = typeSpec.Name.Name

			break
		}
	}

	packageAliases := make(map[string]tsq.PackageInfo)
	currentPkg := tsq.PackageInfo{Path: "test", Name: "test"}
	structMap := make(map[tsq.TypeInfo]*StructInfo)
	parsedPackages := make(map[tsq.PackageInfo]bool)
	pendingPackages := list.New()

	err = parseStructDeclaration(packageAliases, currentPkg, structName, structType, structMap, parsedPackages, pendingPackages)
	if err != nil {
		t.Fatalf("parseStructDeclaration error: %v", err)
	}

	// 验证结构体是否正确解析
	typeInfo := tsq.TypeInfo{Package: currentPkg, TypeName: "User"}

	structInfo, exists := structMap[typeInfo]
	if !exists {
		t.Fatal("User struct not found in structMap")
	}

	// 验证字段
	if len(structInfo.FieldMap) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(structInfo.FieldMap))
	}

	// 验证 ID 字段
	if field, exists := structInfo.FieldMap["ID"]; !exists {
		t.Errorf("ID field not found")
	} else {
		if field.Name != "ID" || field.Type.TypeName != "int64" || field.Column != "id" {
			t.Errorf("ID field incorrect: %+v", field)
		}
	}

	// 验证 Name 字段
	if field, exists := structInfo.FieldMap["Name"]; !exists {
		t.Errorf("Name field not found")
	} else {
		if field.Name != "Name" || field.Type.TypeName != "string" || field.Column != "name" {
			t.Errorf("Name field incorrect: %+v", field)
		}
	}

	// 验证嵌入类型
	if len(structInfo.embeddedTypes) != 1 {
		t.Errorf("Expected 1 embedded type, got %d", len(structInfo.embeddedTypes))
	}

	baseModelType := tsq.TypeInfo{Package: currentPkg, TypeName: "BaseModel"}
	if _, exists := structInfo.embeddedTypes[baseModelType]; !exists {
		t.Errorf("BaseModel not found in embedded types")
	}

	// 验证接收器名称
	if structInfo.Recv != "u" {
		t.Errorf("Expected receiver 'u', got '%s'", structInfo.Recv)
	}
}

func Test_parseStructDeclarationWithErrors(t *testing.T) {
	// 测试重复字段的情况
	source := `
package test

type User struct {
	ID   int64  ` + "`" + `db:"id"` + "`" + `
	ID   string ` + "`" + `db:"id2"` + "`" + ` // 重复字段名
}
`

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "test.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 找到 User 结构体
	var structType *ast.StructType

	var structName string

	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}

		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "User" {
				continue
			}

			structType = typeSpec.Type.(*ast.StructType)
			structName = typeSpec.Name.Name

			break
		}
	}

	packageAliases := make(map[string]tsq.PackageInfo)
	currentPkg := tsq.PackageInfo{Path: "test", Name: "test"}
	structMap := make(map[tsq.TypeInfo]*StructInfo)
	parsedPackages := make(map[tsq.PackageInfo]bool)
	pendingPackages := list.New()

	err = parseStructDeclaration(packageAliases, currentPkg, structName, structType, structMap, parsedPackages, pendingPackages)
	if err == nil {
		t.Fatal("Expected error for duplicate field names")
	}

	// 验证错误类型
	if !IsErrorType(err, ErrorTypeDuplicateField) {
		t.Errorf("Expected ErrorTypeDuplicateField, got different error: %v", err)
	}
}

func Test_genRecv(t *testing.T) {
	tests := []struct {
		typeName string
		expected string
	}{
		{"User", "u"},
		{"UserProfile", "up"},
		{"UserProfileData", "upd"},
		{"user_profile", "up"},
		{"User_Profile_Data", "upd"},
		{"ABC", "abc"},
		{"", ""},
		{"A", "a"},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			result := genRecv(tt.typeName)
			if result != tt.expected {
				t.Errorf("genRecv(%q) = %q, want %q", tt.typeName, result, tt.expected)
			}
		})
	}
}

func TestResolveEmbeddedFields_DetectsCycles(t *testing.T) {
	pkg := tsq.PackageInfo{Path: "test", Name: "test"}
	typeA := tsq.TypeInfo{Package: pkg, TypeName: "A"}
	typeB := tsq.TypeInfo{Package: pkg, TypeName: "B"}

	structA := &StructInfo{
		StructInfo: &tsq.StructInfo{
			TypeInfo: typeA,
			FieldMap: map[string]tsq.FieldInfo{},
		},
		embeddedTypes: map[tsq.TypeInfo]bool{typeB: true},
	}
	structB := &StructInfo{
		StructInfo: &tsq.StructInfo{
			TypeInfo: typeB,
			FieldMap: map[string]tsq.FieldInfo{},
		},
		embeddedTypes: map[tsq.TypeInfo]bool{typeA: true},
	}

	err := resolveEmbeddedFields(structA, map[tsq.TypeInfo]*StructInfo{
		typeA: structA,
		typeB: structB,
	})
	if err == nil {
		t.Fatal("expected cyclic embedded structs to return an error")
	}

	if !IsErrorType(err, ErrorTypeEmbeddedCycle) {
		t.Fatalf("expected embedded cycle error, got %v", err)
	}
}

func TestParseStructDeclaration_DoesNotQueueCurrentPackageForLocalEmbeds(t *testing.T) {
	source := `
package test

type BaseModel struct {
	ID int64 ` + "`" + `db:"id"` + "`" + `
}

type User struct {
	BaseModel
	Name string ` + "`" + `db:"name"` + "`" + `
}
`

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "test.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}

	currentPkg := tsq.PackageInfo{Path: "test", Name: "test"}
	packageAliases := make(map[string]tsq.PackageInfo)
	structMap := make(map[tsq.TypeInfo]*StructInfo)
	parsedPackages := make(map[tsq.PackageInfo]bool)
	pendingPackages := list.New()

	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}

		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}

			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}

			if err := parseStructDeclaration(
				packageAliases,
				currentPkg,
				typeSpec.Name.Name,
				structType,
				structMap,
				parsedPackages,
				pendingPackages,
			); err != nil {
				t.Fatalf("parseStructDeclaration error: %v", err)
			}
		}
	}

	if pendingPackages.Len() != 0 {
		t.Fatalf("expected local embeds to avoid re-queueing current package, got %d queued packages", pendingPackages.Len())
	}
}
