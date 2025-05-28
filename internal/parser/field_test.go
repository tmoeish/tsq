package parser

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/tmoeish/tsq"
)

// Test_parseNamedFields 测试解析具名字段
func Test_parseNamedFields(t *testing.T) {
	source := `
package test

type User struct {
	ID   int64  ` + "`" + `db:"id"` + "`" + `
	Name string ` + "`" + `db:"name"` + "`" + `
	Age  int    ` + "`" + `db:"age"` + "`" + `
	Internal string // 没有标签，应该被跳过
}
`

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "test.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 找到 User 结构体
	var st *ast.StructType

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

			st = typeSpec.Type.(*ast.StructType)

			break
		}
	}

	packageAliases := make(map[string]tsq.PackageInfo)
	currentPkg := tsq.PackageInfo{Path: "test", Name: "test"}

	fields, err := parseNamedFields(packageAliases, currentPkg, st)
	if err != nil {
		t.Fatalf("parseNamedFields error: %v", err)
	}

	// 验证字段数量
	if len(fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(fields))
	}

	// 验证 ID 字段
	if field, exists := fields["ID"]; !exists {
		t.Errorf("ID field not found")
	} else {
		if field.Name != "ID" || field.Type.TypeName != "int64" || field.Column != "id" {
			t.Errorf("ID field incorrect: %+v", field)
		}
	}

	// 验证 Name 字段
	if field, exists := fields["Name"]; !exists {
		t.Errorf("Name field not found")
	} else {
		if field.Name != "Name" || field.Type.TypeName != "string" || field.Column != "name" {
			t.Errorf("Name field incorrect: %+v", field)
		}
	}

	// 验证 Age 字段
	if field, exists := fields["Age"]; !exists {
		t.Errorf("Age field not found")
	} else {
		if field.Name != "Age" || field.Type.TypeName != "int" || field.Column != "age" {
			t.Errorf("Age field incorrect: %+v", field)
		}
	}

	// 验证 Internal 字段不存在
	if _, exists := fields["Internal"]; exists {
		t.Errorf("Internal field should not exist")
	}
}

func Test_parseEmbeddedFields(t *testing.T) {
	source := `
package test

type User struct {
	BaseModel
	*AuditModel
}
`

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "test.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 找到 User 结构体
	var st *ast.StructType

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

			st = typeSpec.Type.(*ast.StructType)

			break
		}
	}

	packageAliases := make(map[string]tsq.PackageInfo)
	currentPkg := tsq.PackageInfo{Path: "test", Name: "test"}

	pkgs, err := parseEmbeddedFields(packageAliases, currentPkg, st)
	if err != nil {
		t.Fatalf("parseEmbeddedFields error: %v", err)
	}

	// 验证嵌入类型数量
	if len(pkgs) != 2 {
		t.Errorf("Expected 2 embedded types, got %d", len(pkgs))
	}

	// 验证 BaseModel
	baseModelType := tsq.TypeInfo{Package: currentPkg, TypeName: "BaseModel"}
	if _, exists := pkgs[baseModelType]; !exists {
		t.Errorf("BaseModel not found in embedded types")
	}

	// 验证 AuditModel
	auditModelType := tsq.TypeInfo{Package: currentPkg, TypeName: "AuditModel"}
	if _, exists := pkgs[auditModelType]; !exists {
		t.Errorf("AuditModel not found in embedded types")
	}
}

func Test_parseFieldType(t *testing.T) {
	tests := []struct {
		name        string
		source      string
		fieldName   string
		isPointer   bool
		isArray     bool
		packagePath string
		typeName    string
		hasError    bool
	}{
		{
			name: "简单类型",
			source: `
package test
type Test struct {
	Field int
}`,
			fieldName:   "Field",
			isPointer:   false,
			isArray:     false,
			packagePath: "",
			typeName:    "int",
		},
		{
			name: "指针类型",
			source: `
package test
type Test struct {
	Field *string
}`,
			fieldName:   "Field",
			isPointer:   true,
			isArray:     false,
			packagePath: "",
			typeName:    "string",
		},
		{
			name: "数组类型",
			source: `
package test
type Test struct {
	Field []int
}`,
			fieldName:   "Field",
			isPointer:   false,
			isArray:     true,
			packagePath: "",
			typeName:    "int",
		},
		{
			name: "指针数组类型",
			source: `
package test
type Test struct {
	Field []*string
}`,
			fieldName:   "Field",
			isPointer:   true,
			isArray:     true,
			packagePath: "",
			typeName:    "string",
		},
		{
			name: "外部包类型",
			source: `
package test
import "time"
type Test struct {
	Field time.Time
}`,
			fieldName:   "Field",
			isPointer:   false,
			isArray:     false,
			packagePath: "time",
			typeName:    "Time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fset := token.NewFileSet()

			file, err := parser.ParseFile(fset, "test.go", tt.source, 0)
			if err != nil {
				t.Fatal(err)
			}

			// 找到 Test 结构体和指定字段
			var field *ast.Field

			for _, decl := range file.Decls {
				genDecl, ok := decl.(*ast.GenDecl)
				if !ok {
					continue
				}

				for _, spec := range genDecl.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || typeSpec.Name.Name != "Test" {
						continue
					}

					st := typeSpec.Type.(*ast.StructType)
					for _, f := range st.Fields.List {
						if len(f.Names) > 0 && f.Names[0].Name == tt.fieldName {
							field = f
							break
						}
					}
				}
			}

			if field == nil {
				t.Fatal("Field not found")
			}

			isPointer, isArray, packagePath, typeName, err := parseFieldType(field.Type)

			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}

				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if isPointer != tt.isPointer {
				t.Errorf("isPointer: expected %v, got %v", tt.isPointer, isPointer)
			}

			if isArray != tt.isArray {
				t.Errorf("isArray: expected %v, got %v", tt.isArray, isArray)
			}

			if packagePath != tt.packagePath {
				t.Errorf("packagePath: expected %q, got %q", tt.packagePath, packagePath)
			}

			if typeName != tt.typeName {
				t.Errorf("typeName: expected %q, got %q", tt.typeName, typeName)
			}
		})
	}
}

func Test_parseFieldTypeErrors(t *testing.T) {
	// 测试错误情况，比如不支持的类型表达式
	tests := []struct {
		name   string
		source string
	}{
		{
			name: "函数类型",
			source: `
package test
type Test struct {
	Field func() int
}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fset := token.NewFileSet()

			file, err := parser.ParseFile(fset, "test.go", tt.source, 0)
			if err != nil {
				t.Fatal(err)
			}

			// 找到 Test 结构体和 Field 字段
			var field *ast.Field

			for _, decl := range file.Decls {
				genDecl, ok := decl.(*ast.GenDecl)
				if !ok {
					continue
				}

				for _, spec := range genDecl.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || typeSpec.Name.Name != "Test" {
						continue
					}

					st := typeSpec.Type.(*ast.StructType)
					for _, f := range st.Fields.List {
						if len(f.Names) > 0 && f.Names[0].Name == "Field" {
							field = f
							break
						}
					}
				}
			}

			if field == nil {
				t.Fatal("Field not found")
			}

			_, _, _, _, err = parseFieldType(field.Type)
			if err == nil {
				t.Errorf("Expected error for unsupported type")
			}
		})
	}
}
