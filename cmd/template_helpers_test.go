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

	if got := FieldToCol(info, "Name"); got != `"name"` {
		t.Fatalf("expected raw quoted Go string literal, got %q", got)
	}
}

func TestFieldsToColsReturnsCommaSeparatedIdentifiers(t *testing.T) {
	info := &tsq.StructInfo{
		FieldMap: map[string]tsq.FieldInfo{
			"Name": {Column: "name"},
			"DT":   {Column: "dt"},
		},
	}

	if got := FieldsToCols(info, []string{"DT", "Name"}); got != `"dt", "name"` {
		t.Fatalf("unexpected columns string: %q", got)
	}
}

func TestValidateManagedFieldsSupportsPointerAndNullTypes(t *testing.T) {
	info := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			CT: "CT",
			MT: "MT",
			DT: "DT",
		},
		FieldMap: map[string]tsq.FieldInfo{
			"CT": {
				Name:      "CT",
				Type:      tsq.TypeInfo{Package: tsq.PackageInfo{Path: "time", Name: "time"}, TypeName: "Time"},
				IsPointer: true,
			},
			"MT": {
				Name: "MT",
				Type: tsq.TypeInfo{Package: tsq.PackageInfo{Path: "database/sql", Name: "sql"}, TypeName: "NullTime"},
			},
			"DT": {
				Name: "DT",
				Type: tsq.TypeInfo{Package: tsq.PackageInfo{Path: "gopkg.in/nullbio/null.v6", Name: "null"}, TypeName: "Time"},
			},
		},
	}

	if err := ValidateManagedFields(info); err != nil {
		t.Fatalf("expected managed field validation to pass, got %v", err)
	}
}

func TestValidateManagedFieldsRejectsUnsupportedSoftDeleteType(t *testing.T) {
	info := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			DT: "DT",
		},
		FieldMap: map[string]tsq.FieldInfo{
			"DT": {
				Name: "DT",
				Type: tsq.TypeInfo{Package: tsq.PackageInfo{Path: "time", Name: "time"}, TypeName: "Time"},
			},
		},
	}

	if err := ValidateManagedFields(info); err == nil {
		t.Fatal("expected plain time.Time soft delete field to be rejected")
	}
}

func TestValidateManagedFieldsRejectsNarrowIntegerSoftDeleteType(t *testing.T) {
	info := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			DT: "DT",
		},
		FieldMap: map[string]tsq.FieldInfo{
			"DT": {
				Name: "DT",
				Type: tsq.TypeInfo{TypeName: "int8"},
			},
		},
	}

	if err := ValidateManagedFields(info); err == nil {
		t.Fatal("expected narrow integer soft delete field to be rejected")
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

func TestTimestampNowValueUsesGeneratedAliases(t *testing.T) {
	sqlField := tsq.FieldInfo{
		Name: "MT",
		Type: tsq.TypeInfo{
			Package:  tsq.PackageInfo{Path: "database/sql", Name: "sql"},
			TypeName: "NullTime",
		},
	}
	timePtrField := tsq.FieldInfo{
		Name:      "CT",
		IsPointer: true,
		Type: tsq.TypeInfo{
			Package:  tsq.PackageInfo{Path: "time", Name: "time"},
			TypeName: "Time",
		},
	}

	if got := TimestampNowValue(sqlField); got != "tsqsql.NullTime{Time: tsqtime.Now(), Valid: true}" {
		t.Fatalf("unexpected sql null time expression: %q", got)
	}

	if got := TimestampNowValue(timePtrField); got != "tsq.TimePtr(tsqtime.Now())" {
		t.Fatalf("unexpected time pointer expression: %q", got)
	}
}
