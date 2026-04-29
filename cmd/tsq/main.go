package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/cmd"
)

var rootCmd = &cobra.Command{
	Use:   "tsq [command]",
	Short: "type safe query",
	Long:  "tsq is a tool for generating type safe query go code",
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
	},
	Version:       tsq.Version,
	SilenceErrors: true,
	SilenceUsage:  true,
}

func init() {
	rootCmd.AddCommand(cmd.FmtCmd)
	rootCmd.AddCommand(cmd.GenCmd)
	rootCmd.AddCommand(cmd.VersionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, formatCLIError(os.Stderr, err))

		os.Exit(1)
	}
}

func formatCLIError(file *os.File, err error) string {
	label := "Error"
	if term.IsTerminal(int(file.Fd())) {
		label = "\033[1;31mError\033[0m"
	}

	return fmt.Sprintf("%s: %s", label, err.Error())
}
