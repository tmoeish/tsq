package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tmoeish/tsq/v4/internal/buildinfo"
)

// resetVersionFlags returns VersionCmd to its pre-parse state.
//
// VersionCmd is a package-level singleton whose flags are bound once in init, so
// state survives Execute. Clearing the Go variables is not enough: the mutually
// exclusive group is enforced from each flag's Changed bit, so a --short left set
// by an earlier case makes the next case fail as if both flags had been passed.
func resetVersionFlags(t *testing.T) {
	t.Helper()

	versionShortFlag = false
	versionJSONFlag = false

	for _, name := range []string{"short", "json"} {
		VersionCmd.Flags().Lookup(name).Changed = false
	}

	VersionCmd.SetArgs(nil)
}

// runVersion executes VersionCmd with args and returns everything it wrote.
func runVersion(t *testing.T, args ...string) string {
	t.Helper()

	resetVersionFlags(t)
	t.Cleanup(func() { resetVersionFlags(t) })

	var out bytes.Buffer

	VersionCmd.SetOut(&out)
	VersionCmd.SetErr(&out)
	VersionCmd.SetArgs(args)

	if err := VersionCmd.Execute(); err != nil {
		t.Fatalf("version %v: %v", args, err)
	}

	return out.String()
}

func TestVersionReportsEveryBuildInfoField(t *testing.T) {
	got := runVersion(t)
	info := buildinfo.Current()

	for _, want := range []string{
		info.Version,
		info.BuildTime,
		info.GitCommit,
		info.GitBranch,
		info.GoVersion,
		info.Platform + "/" + info.Arch,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("version output is missing %q:\n%s", want, got)
		}
	}
}

func TestVersionShortPrintsOnlyTheVersion(t *testing.T) {
	got := strings.TrimSpace(runVersion(t, "--short"))

	if want := buildinfo.Version(); got != want {
		t.Errorf("version --short = %q, want %q", got, want)
	}
}

func TestVersionJSONRoundTrips(t *testing.T) {
	var got buildinfo.Info
	if err := json.Unmarshal([]byte(runVersion(t, "--json")), &got); err != nil {
		t.Fatalf("version --json is not valid JSON: %v", err)
	}

	if want := *buildinfo.Current(); got != want {
		t.Errorf("version --json = %+v, want %+v", got, want)
	}
}

func TestVersionRejectsShortWithJSON(t *testing.T) {
	resetVersionFlags(t)
	t.Cleanup(func() { resetVersionFlags(t) })

	var out bytes.Buffer

	VersionCmd.SetOut(&out)
	VersionCmd.SetErr(&out)
	VersionCmd.SetArgs([]string{"--short", "--json"})

	if err := VersionCmd.Execute(); err == nil {
		t.Fatal("version --short --json should be rejected; the two pick different formats")
	}
}
