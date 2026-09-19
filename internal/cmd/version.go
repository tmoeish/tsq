package cmd

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/tmoeish/tsq/v5/internal/buildinfo"
)

var (
	versionShortFlag bool
	versionJSONFlag  bool
)

// VersionCmd reports the version and build provenance of this tsq binary.
var VersionCmd = &Command{
	Name:  "version",
	Usage: "version [--short | --json]",
	Short: "Show tsq version and build information",
	Long: `Show the version and build provenance of this tsq binary.

Build time, commit and branch are injected at link time. A binary built without
them - "go install", "go run", or a plain "go build" - reports "unknown" for the
three, and falls back to the version compiled into the sources.`,
	flags: func(fs *flag.FlagSet) {
		fs.BoolVar(&versionShortFlag, "short", false, "print only the version string")
		fs.BoolVar(&versionJSONFlag, "json", false, "print build information as JSON")
	},
	run: func(cmd *Command, args []string) error {
		if len(args) > 0 {
			return fmt.Errorf("tsq version takes no arguments, got %q", args)
		}

		if versionShortFlag && versionJSONFlag {
			return errors.New("--short and --json cannot be used together")
		}

		info := buildinfo.Current()
		out := cmd.OutOrStdout()

		switch {
		case versionShortFlag:
			_, err := fmt.Fprintln(out, info.Version)

			return err

		case versionJSONFlag:
			encoder := json.NewEncoder(out)
			encoder.SetIndent("", "  ")

			return encoder.Encode(info)
		}

		return writeVersionTable(out, info)
	},
}

func writeVersionTable(out io.Writer, info *buildinfo.Info) error {
	if _, err := fmt.Fprintf(out, "TSQ %s\n", info.Version); err != nil {
		return err
	}

	rows := [][2]string{
		{"build time", info.BuildTime},
		{"commit", info.GitCommit},
	}

	// Tag-triggered release builds check out a detached HEAD, so the branch the
	// linker sees is literally "HEAD". That row carries no information; the commit
	// above is the authoritative provenance. Only print a branch when it names one.
	if branchIsInformative(info.GitBranch) {
		rows = append(rows, [2]string{"branch", info.GitBranch})
	}

	rows = append(rows,
		[2]string{"go", info.GoVersion},
		[2]string{"platform", info.Platform + "/" + info.Arch},
	)

	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, row := range rows {
		if _, err := fmt.Fprintf(table, "  %s\t%s\n", row[0], row[1]); err != nil {
			return err
		}
	}

	return table.Flush()
}

func branchIsInformative(branch string) bool {
	switch branch {
	case "", "HEAD", "unknown":
		return false
	default:
		return true
	}
}
