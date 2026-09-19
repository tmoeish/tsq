package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tmoeish/tsq/v5/internal/buildinfo"
)

func TestRunDispatchesCommands(t *testing.T) {
	for _, tt := range []struct {
		args []string
		want string
	}{
		{nil, "Commands:"},
		{[]string{"--help"}, "gen "},
		{[]string{"--version"}, "tsq version " + buildinfo.Version()},
		{[]string{"help", "gen"}, "tsq gen <package-or-dir> [flags]"},
		{[]string{"help", "version"}, "--short"},
	} {
		var out bytes.Buffer
		if err := run(tt.args, &out); err != nil {
			t.Fatalf("run(%q) error = %v", tt.args, err)
		}

		if !strings.Contains(out.String(), tt.want) {
			t.Errorf("run(%q) = %q, want it to contain %q", tt.args, out.String(), tt.want)
		}
	}
}

func TestRunRejectsUnknownInput(t *testing.T) {
	for _, args := range [][]string{{"nope"}, {"help", "nope"}, {"gen"}, {"version", "extra"}, {"version", "--short", "--json"}, {"gen", "--nope", "."}} {
		if err := run(args, new(bytes.Buffer)); err == nil {
			t.Errorf("run(%q) succeeded, want an error", args)
		}
	}
}
