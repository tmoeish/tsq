package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrateCmdHelpDocumentsInputsAndBehavior(t *testing.T) {
	buf := new(bytes.Buffer)
	MigrateCmd.SetOut(buf)
	MigrateCmd.SetErr(buf)

	if err := MigrateCmd.Help(); err != nil {
		t.Fatalf("expected migrate help to render, got %v", err)
	}

	help := buf.String()
	for _, want := range []string{
		"module import path",
		"relative directory",
		"absolute directory",
		"//tsq: directive lines",
		"the legacy syntax is not read by",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("expected migrate help to mention %q, got:\n%s", want, help)
		}
	}
}

// TestMigrateCmdConvertsLegacyAnnotations checks the whole shape a converted
// struct ends up with: the prose above the annotation survives, the keys become
// one directive per concern, and a derived index name is left derived.
func TestMigrateCmdConvertsLegacyAnnotations(t *testing.T) {
	dir := t.TempDir()

	writeTestFile(t, filepath.Join(dir, "go.mod"), "module example.com/migratetest\n\ngo 1.24.2\n")
	writeTestFile(t, filepath.Join(dir, "model.go"), `package migratetest

// 用户表
// @TABLE(
//
//	name="user",
//	pk="UID,false",
//	version,
//	updated_at="MTime",
//	ux=[
//		{name="ux_user_email", fields=["Email"]},
//	],
//	idx=[
//		{fields=["OrgID", "Status"]},
//	],
//	search=["Name","Email"],
//
// )
type User struct {
	UID    int64
	Name   string
	Email  string
	OrgID  int64
	Status int
	MTime  int64
	Ver    int64
}
`)

	buf := new(bytes.Buffer)
	MigrateCmd.SetOut(buf)
	MigrateCmd.SetErr(buf)
	MigrateCmd.SetArgs([]string{dir})

	if err := MigrateCmd.Execute(); err != nil {
		t.Fatalf("MigrateCmd.Execute() error = %v", err)
	}

	if got := buf.String(); !strings.Contains(got, filepath.Join(dir, "model.go")) {
		t.Fatalf("expected migrate output to list the changed file, got %q", got)
	}

	migrated, err := os.ReadFile(filepath.Join(dir, "model.go"))
	if err != nil {
		t.Fatalf("failed to read migrated file: %v", err)
	}

	got := string(migrated)

	for _, want := range []string{
		"// 用户表\n//tsq:table name=user pk=UID assigned",
		"//tsq:managed version updated_at=MTime",
		"//tsq:unique Email name=ux_user_email",
		"//tsq:index OrgID,Status",
		"//tsq:search Name,Email",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected migrated file to contain %q, got:\n%s", want, got)
		}
	}

	if strings.Contains(got, "@TABLE") {
		t.Fatalf("expected the legacy annotation to be gone, got:\n%s", got)
	}
}

func TestMigrateArgsRejectsMissingOrExtraPackagePaths(t *testing.T) {
	args := exactOnePackageArgFor("migrate")

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing", args: nil, want: "tsq migrate expects exactly one package path, got 0"},
		{name: "extra", args: []string{"./a", "./b"}, want: "tsq migrate expects exactly one package path, got 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := args(nil, tc.args)
			if err == nil {
				t.Fatal("expected exact arg validation to fail")
			}

			if err.Error() != tc.want {
				t.Fatalf("unexpected arg error %q, want %q", err.Error(), tc.want)
			}
		})
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
}
