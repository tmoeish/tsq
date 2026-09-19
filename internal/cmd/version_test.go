package cmd

import (
	"bytes"
	"encoding/json"
	"flag"
	"strings"
	"testing"

	"github.com/tmoeish/tsq/v5/internal/buildinfo"
)

// resetVersionFlags clears the arguments of VersionCmd. Its flags are declared
// afresh on every Execute, so no parse state survives between runs.
func resetVersionFlags(t *testing.T) {
	t.Helper()

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

// TestFlagsMayFollowArguments keeps "tsq gen ./pkg --check" working as it did
// under cobra: the standard flag package alone stops at the first argument.
func TestFlagsMayFollowArguments(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	check := fs.Bool("check", false, "")
	verbose := fs.Bool("v", false, "")

	args, err := parseInterspersed(fs, []string{"./pkg", "--check", "-v", "--", "-literal"})
	if err != nil || !*check || !*verbose || strings.Join(args, " ") != "./pkg -literal" {
		t.Fatalf("parseInterspersed = %q, check=%v v=%v, %v", args, *check, *verbose, err)
	}
}
