package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
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
	Version: tsq.Version,
}

func init() {
	rootCmd.AddCommand(cmd.GenCmd)
	rootCmd.AddCommand(cmd.VersionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprint(os.Stderr, err.Error())
	}
}
