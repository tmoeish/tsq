package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmoeish/tsq"
)

func TestValidateDTOFieldsRejectsUnknownTargetField(t *testing.T) {
	dto := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		TypeInfo:  tsq.TypeInfo{TypeName: "UserDTO"},
		Fields: []tsq.FieldInfo{
			{Name: "UserName", Column: "User.Missing"},
		},
	}

	structsByName := map[string]*tsq.StructInfo{
		"User": {
			TableInfo: &tsq.TableInfo{Table: "user"},
			FieldMap: map[string]tsq.FieldInfo{
				"ID": {Name: "ID", Column: "id"},
			},
		},
	}

	if err := validateDTOFields(dto, structsByName); err == nil {
		t.Fatal("expected invalid DTO reference to return an error")
	}
}

func TestValidateDTOFieldsRejectsNormalizedReferenceCollisions(t *testing.T) {
	dto := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		TypeInfo:  tsq.TypeInfo{TypeName: "UserDTO"},
		Fields: []tsq.FieldInfo{
			{Name: "A", Column: "User.Profile_ID"},
			{Name: "B", Column: "User_Profile.ID"},
		},
	}

	structsByName := map[string]*tsq.StructInfo{
		"User": {
			TableInfo: &tsq.TableInfo{Table: "user"},
			FieldMap: map[string]tsq.FieldInfo{
				"Profile_ID": {Name: "Profile_ID", Column: "profile_id"},
			},
		},
		"User_Profile": {
			TableInfo: &tsq.TableInfo{Table: "user_profile"},
			FieldMap: map[string]tsq.FieldInfo{
				"ID": {Name: "ID", Column: "id"},
			},
		},
	}

	if err := validateDTOFields(dto, structsByName); err == nil {
		t.Fatal("expected normalized DTO reference collision to return an error")
	}
}

func TestNormalizeDTOColumnsUpdatesFieldMap(t *testing.T) {
	dto := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		Fields: []tsq.FieldInfo{
			{Name: "UserID", Column: "User.ID"},
		},
		FieldMap: map[string]tsq.FieldInfo{
			"UserID": {Name: "UserID", Column: "User.ID"},
		},
	}

	normalizeDTOColumns(dto)

	if got := dto.Fields[0].Column; got != "User_ID" {
		t.Fatalf("expected DTO field column to be normalized, got %q", got)
	}

	if got := dto.FieldMap["UserID"].Column; got != "User_ID" {
		t.Fatalf("expected DTO field map column to be normalized, got %q", got)
	}
}

func TestValidateGeneratedFilenameCollisionsRejectsCaseConflicts(t *testing.T) {
	list := []*tsq.StructInfo{
		{
			TableInfo: &tsq.TableInfo{Table: "user"},
			TypeInfo:  tsq.TypeInfo{TypeName: "User"},
			Fields:    []tsq.FieldInfo{{Name: "ID"}},
		},
		{
			TableInfo: &tsq.TableInfo{Table: "user_lower"},
			TypeInfo:  tsq.TypeInfo{TypeName: "user"},
			Fields:    []tsq.FieldInfo{{Name: "ID"}},
		},
	}

	if err := validateGeneratedFilenameCollisions(list); err == nil {
		t.Fatal("expected case-insensitive filename collision to return an error")
	}
}

func TestTableTemplateOrErrPreservesErrNoRows(t *testing.T) {
	want := `if err == {{ GeneratedSQLRef "ErrNoRows" }} {`
	if count := strings.Count(defaultTableTpl, want); count < 4 {
		t.Fatalf("expected table template to preserve sql.ErrNoRows in every OrErr helper, count=%d", count)
	}
}

func TestResolveTemplateTextUsesFallbackWithoutLeakingPreviousOverride(t *testing.T) {
	dir := t.TempDir()
	overridePath := filepath.Join(dir, "custom.tmpl")
	if err := os.WriteFile(overridePath, []byte("custom-template"), 0o644); err != nil {
		t.Fatalf("failed to write override template: %v", err)
	}

	override, err := resolveTemplateText(overridePath, defaultTableTpl, "template")
	if err != nil {
		t.Fatalf("expected override template to load, got %v", err)
	}

	if override != "custom-template" {
		t.Fatalf("unexpected override template: %q", override)
	}

	fallback, err := resolveTemplateText("", defaultTableTpl, "template")
	if err != nil {
		t.Fatalf("expected fallback template to load, got %v", err)
	}

	if fallback != defaultTableTpl {
		t.Fatal("expected empty override path to return embedded template")
	}

	if fallback == override {
		t.Fatal("expected fallback template to ignore previous override content")
	}
}
