package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/tmoeish/tsq/v4/internal/buildinfo"
)

var (
	versionShortFlag bool
	versionJSONFlag  bool
)

func init() {
	VersionCmd.Flags().BoolVar(&versionShortFlag, "short", false, "print only the version string")
	VersionCmd.Flags().BoolVar(&versionJSONFlag, "json", false, "print build information as JSON")
	VersionCmd.MarkFlagsMutuallyExclusive("short", "json")
}

// VersionCmd reports the version and build provenance of this tsq binary.
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show tsq version and build information",
	Long: `Show the version and build provenance of this tsq binary.

Build time, commit and branch are injected at link time. A binary built without
them - "go install", "go run", or a plain "go build" - reports "unknown" for the
three, and falls back to the version compiled into the sources.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
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

	table := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, row := range [][2]string{
		{"build time", info.BuildTime},
		{"commit", info.GitCommit},
		{"branch", info.GitBranch},
		{"go", info.GoVersion},
		{"platform", info.Platform + "/" + info.Arch},
	} {
		if _, err := fmt.Fprintf(table, "  %s\t%s\n", row[0], row[1]); err != nil {
			return err
		}
	}

	return table.Flush()
}
