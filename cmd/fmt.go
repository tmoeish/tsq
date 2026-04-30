package cmd

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/spf13/cobra"

	"github.com/tmoeish/tsq/internal/parser"
)

// FmtCmd rewrites TSQ DSL annotations into a canonical format.
var FmtCmd = &cobra.Command{
	Use:   "fmt <package-or-dir>",
	Short: "Format TSQ DSL annotations in one Go package",
	Long: `Format TSQ DSL annotations for one Go package.

Accepted inputs:
  - module import path: github.com/acme/project/internal/database
  - relative directory: ./internal/database
  - absolute directory: /path/to/project/internal/database

Formatting behavior:
  - rewrites only @TABLE / @RESULT annotations attached to struct declarations
  - keeps surrounding prose text while tightening spacing around annotations
  - normalizes key order, indentation, blank lines, commas, and string quoting`,
	Example: "  tsq fmt ./examples/database\n  tsq fmt github.com/tmoeish/tsq/examples/database",
	Args:    exactOnePackageArgFor("fmt"),
	RunE: func(cmd *cobra.Command, args []string) error {
		changed, err := parser.FormatPackage(args[0])
		if err != nil {
			return errors.Trace(err)
		}

		for _, filename := range changed {
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), filename); err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	},
}
