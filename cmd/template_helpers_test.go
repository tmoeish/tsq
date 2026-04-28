package cmd

import (
	"testing"

	"github.com/tmoeish/tsq"
)

func TestFieldToColReturnsUnquotedIdentifier(t *testing.T) {
	info := &tsq.StructInfo{
		FieldMap: map[string]tsq.FieldInfo{
			"Name": {Column: "name"},
		},
	}

	if got := fieldToCol(info, "Name"); got != `"name"` {
		t.Fatalf("expected raw quoted Go string literal, got %q", got)
	}
}

func TestFieldsToColsReturnsCommaSeparatedIdentifiers(t *testing.T) {
	info := &tsq.StructInfo{
		FieldMap: map[string]tsq.FieldInfo{
			"Name":      {Column: "name"},
			"DeletedAt": {Column: "deleted_at"},
		},
	}

	if got := fieldsToCols(info, []string{"DeletedAt", "Name"}); got != `"deleted_at", "name"` {
		t.Fatalf("unexpected columns string: %q", got)
	}
}

func TestValidateManagedFieldsSupportsPointerAndNullTypes(t *testing.T) {
	info := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			CreatedAtField: "CreatedAt",
			UpdatedAtField: "UpdatedAt",
			DeletedAtField: "DeletedAt",
		},
		FieldMap: map[string]tsq.FieldInfo{
			"CreatedAt": {
				Name:      "CreatedAt",
				Type:      tsq.TypeInfo{Package: tsq.PackageInfo{Path: "time", Name: "time"}, TypeName: "Time"},
				IsPointer: true,
			},
			"UpdatedAt": {
				Name: "UpdatedAt",
				Type: tsq.TypeInfo{Package: tsq.PackageInfo{Path: "database/sql", Name: "sql"}, TypeName: "NullTime"},
			},
			"DeletedAt": {
				Name: "DeletedAt",
				Type: tsq.TypeInfo{Package: tsq.PackageInfo{Path: "gopkg.in/nullbio/null.v6", Name: "null"}, TypeName: "Time"},
			},
		},
	}

	if err := validateManagedFields(info); err != nil {
		t.Fatalf("expected managed field validation to pass, got %v", err)
	}
}

func TestValidateManagedFieldsRejectsUnsupportedSoftDeleteType(t *testing.T) {
	info := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			DeletedAtField: "DeletedAt",
		},
		FieldMap: map[string]tsq.FieldInfo{
			"DeletedAt": {
				Name: "DeletedAt",
				Type: tsq.TypeInfo{Package: tsq.PackageInfo{Path: "time", Name: "time"}, TypeName: "Time"},
			},
		},
	}

	if err := validateManagedFields(info); err == nil {
		t.Fatal("expected plain time.Time soft delete field to be rejected")
	}
}

func TestValidateManagedFieldsRejectsNarrowIntegerSoftDeleteType(t *testing.T) {
	info := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			DeletedAtField: "DeletedAt",
		},
		FieldMap: map[string]tsq.FieldInfo{
			"DeletedAt": {
				Name: "DeletedAt",
				Type: tsq.TypeInfo{TypeName: "int8"},
			},
		},
	}

	if err := validateManagedFields(info); err == nil {
		t.Fatal("expected narrow integer soft delete field to be rejected")
	}
}

func TestValidateManagedFieldsRejectsNullableSoftDeleteUniqueIndexes(t *testing.T) {
	info := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			DeletedAtField: "DeletedAt",
			UxList:         []tsq.IndexInfo{{Name: "ux_name", Fields: []string{"Name"}}},
		},
		TypeInfo: tsq.TypeInfo{TypeName: "User"},
		FieldMap: map[string]tsq.FieldInfo{
			"DeletedAt": {
				Name:      "DeletedAt",
				IsPointer: true,
				Type: tsq.TypeInfo{
					Package:  tsq.PackageInfo{Path: "time", Name: "time"},
					TypeName: "Time",
				},
			},
			"Name": {
				Name: "Name",
				Type: tsq.TypeInfo{TypeName: "string"},
			},
		},
	}

	if err := validateManagedFields(info); err == nil {
		t.Fatal("expected nullable soft-delete field with unique indexes to be rejected")
	}
}

func TestValidateManagedFieldsAllowsIntegerSoftDeleteUniqueIndexes(t *testing.T) {
	info := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			DeletedAtField: "DeletedAt",
			UxList:         []tsq.IndexInfo{{Name: "ux_name", Fields: []string{"Name"}}},
		},
		TypeInfo: tsq.TypeInfo{TypeName: "User"},
		FieldMap: map[string]tsq.FieldInfo{
			"DeletedAt": {
				Name: "DeletedAt",
				Type: tsq.TypeInfo{TypeName: "int64"},
			},
			"Name": {
				Name: "Name",
				Type: tsq.TypeInfo{TypeName: "string"},
			},
		},
	}

	if err := validateManagedFields(info); err != nil {
		t.Fatalf("expected integer soft-delete field with unique indexes to pass, got %v", err)
	}
}

func TestFieldTypeUsesGeneratedAliasesForStdlibPackages(t *testing.T) {
	sqlField := tsq.FieldInfo{
		Type: tsq.TypeInfo{
			Package:  tsq.PackageInfo{Path: "database/sql", Name: "sql1"},
			TypeName: "NullTime",
		},
	}
	timeField := tsq.FieldInfo{
		Type: tsq.TypeInfo{
			Package:  tsq.PackageInfo{Path: "time", Name: "time1"},
			TypeName: "Time",
		},
	}

	if got := fieldType(sqlField); got != "tsqsql.NullTime" {
		t.Fatalf("expected generated sql alias, got %q", got)
	}

	if got := fieldType(timeField); got != "tsqtime.Time" {
		t.Fatalf("expected generated time alias, got %q", got)
	}
}

func TestFieldTypePreservesPointerAndSliceModifiers(t *testing.T) {
	ptrField := tsq.FieldInfo{
		IsPointer: true,
		Type: tsq.TypeInfo{
			Package:  tsq.PackageInfo{Path: "time", Name: "time"},
			TypeName: "Time",
		},
	}
	sliceField := tsq.FieldInfo{
		IsArray: true,
		Type: tsq.TypeInfo{
			TypeName: "int64",
		},
	}
	slicePtrField := tsq.FieldInfo{
		IsArray:   true,
		IsPointer: true,
		Type: tsq.TypeInfo{
			Package:  tsq.PackageInfo{Path: "example.com/pkg", Name: "pkg"},
			TypeName: "Thing",
		},
	}

	if got := fieldType(ptrField); got != "*tsqtime.Time" {
		t.Fatalf("expected pointer type to be preserved, got %q", got)
	}

	if got := fieldType(sliceField); got != "[]int64" {
		t.Fatalf("expected slice type to be preserved, got %q", got)
	}

	if got := fieldType(slicePtrField); got != "[]*pkg.Thing" {
		t.Fatalf("expected combined slice/pointer modifiers to be preserved, got %q", got)
	}
}

func TestListTypeProducesValueSlices(t *testing.T) {
	if got := listType("pkg.Thing"); got != "[]pkg.Thing" {
		t.Fatalf("expected listType to build a value slice, got %q", got)
	}
}

func TestInitialHelpersSupportUnicode(t *testing.T) {
	if got := upperInitial("用户"); got != "用户" {
		t.Fatalf("expected upperInitial to preserve valid unicode, got %q", got)
	}

	if got := lowerInitial("用户ID"); got != "用户ID" {
		t.Fatalf("expected lowerInitial to preserve valid unicode, got %q", got)
	}

	if got := lowerInitial("Äpfel"); got != "äpfel" {
		t.Fatalf("expected lowerInitial to lowercase first rune, got %q", got)
	}
}

func TestFieldVarNameAvoidsGoKeywords(t *testing.T) {
	if got := fieldVarName("Type"); got != "type_" {
		t.Fatalf("expected keyword field name to be suffixed, got %q", got)
	}

	if got := fieldSliceVarName("Type"); got != "type_s" {
		t.Fatalf("expected keyword slice field name to be suffixed before pluralization, got %q", got)
	}

	if got := fieldVarName("UserID"); got != "userID" {
		t.Fatalf("expected non-keyword field name to keep lower initial form, got %q", got)
	}
}

func TestTimestampNowValueUsesGeneratedAliases(t *testing.T) {
	sqlField := tsq.FieldInfo{
		Name: "UpdatedAt",
		Type: tsq.TypeInfo{
			Package:  tsq.PackageInfo{Path: "database/sql", Name: "sql"},
			TypeName: "NullTime",
		},
	}
	timePtrField := tsq.FieldInfo{
		Name:      "CreatedAt",
		IsPointer: true,
		Type: tsq.TypeInfo{
			Package:  tsq.PackageInfo{Path: "time", Name: "time"},
			TypeName: "Time",
		},
	}

	if got := timestampNowValue(sqlField); got != "tsqsql.NullTime{Time: tsqtime.Now(), Valid: true}" {
		t.Fatalf("unexpected sql null time expression: %q", got)
	}

	if got := timestampNowValue(timePtrField); got != "tsq.TimePtr(tsqtime.Now())" {
		t.Fatalf("unexpected time pointer expression: %q", got)
	}
}
