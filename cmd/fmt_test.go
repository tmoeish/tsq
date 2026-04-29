package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFmtCmdHelpDocumentsInputsAndBehavior(t *testing.T) {
	buf := new(bytes.Buffer)
	FmtCmd.SetOut(buf)
	FmtCmd.SetErr(buf)

	if err := FmtCmd.Help(); err != nil {
		t.Fatalf("expected fmt help to render, got %v", err)
	}

	help := buf.String()
	for _, want := range []string{
		"module import path",
		"relative directory",
		"absolute directory",
		"@TABLE / @RESULT",
		"keeps other comment text untouched",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("expected fmt help to mention %q, got:\n%s", want, help)
		}
	}
}

func TestFmtCmdFormatsPackage(t *testing.T) {
	dir := t.TempDir()

	writeTestFile(t, filepath.Join(dir, "go.mod"), "module example.com/fmttest\n\ngo 1.24.2\n")
	writeTestFile(t, filepath.Join(dir, "model.go"), `package fmttest

// 用户表
// @TABLE(
//
//	kw=["Name","Email"],
//	created_at
//
// )
type User struct {
	Name  string
	Email string
}
`)

	buf := new(bytes.Buffer)
	FmtCmd.SetOut(buf)
	FmtCmd.SetErr(buf)
	FmtCmd.SetArgs([]string{dir})

	if err := FmtCmd.Execute(); err != nil {
		t.Fatalf("FmtCmd.Execute() error = %v", err)
	}

	if got := buf.String(); !strings.Contains(got, filepath.Join(dir, "model.go")) {
		t.Fatalf("expected fmt output to list changed file, got %q", got)
	}

	formatted, err := os.ReadFile(filepath.Join(dir, "model.go"))
	if err != nil {
		t.Fatalf("failed to read formatted file: %v", err)
	}

	for _, want := range []string{
		"// @TABLE(",
		"//\tcreated_at,",
		"//\tkw=[\"Name\", \"Email\"],",
	} {
		if !strings.Contains(string(formatted), want) {
			t.Fatalf("expected formatted file to contain %q, got:\n%s", want, string(formatted))
		}
	}
}

func TestFmtArgsRejectsMissingOrExtraPackagePaths(t *testing.T) {
	args := exactOnePackageArgFor("fmt")

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing", args: nil, want: "tsq fmt expects exactly one package path, got 0"},
		{name: "extra", args: []string{"./a", "./b"}, want: "tsq fmt expects exactly one package path, got 2"},
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
