package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/tmoeish/tsq/v5/internal/parser"
)

// MigrateCmd rewrites legacy @TABLE / @RESULT annotations as //tsq: directives.
var MigrateCmd = &cobra.Command{
	Use:   "migrate <package-or-dir>",
	Short: "Rewrite legacy @TABLE / @RESULT annotations as //tsq: directives",
	Long: `Rewrite the annotations of one Go package into the v5 directive syntax.

Accepted inputs:
  - module import path: github.com/acme/project/internal/database
  - relative directory: ./internal/database
  - absolute directory: /path/to/project/internal/database

What it does:
  - reads each legacy annotation with the v4 DSL parser, so anything the v4
    generator accepted converts exactly
  - replaces the annotation block with //tsq: directive lines, keeping the prose
    that preceded it
  - leaves an index name out when v4 would have derived the same one

Run it once per package, review the diff, then delete it from your workflow:
the legacy syntax is not read by ` + "`tsq gen`" + ` any more.`,
	Example: "  tsq migrate ./internal/database",
	Args:    exactOnePackageArgFor("migrate"),
	RunE: func(cmd *cobra.Command, args []string) error {
		changed, err := parser.MigratePackage(args[0])
		if err != nil {
			return err
		}

		for _, filename := range changed {
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), filename); err != nil {
				return err
			}
		}

		return nil
	},
}
