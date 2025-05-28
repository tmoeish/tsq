package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tmoeish/tsq"
)

// VersionCmd 显示 tsq 的版本信息
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "显示 tsq 的版本信息",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("tsq 版本: %s\n", tsq.Version)
		fmt.Printf("构建时间: %s\n", tsq.BuildTime)
	},
}
