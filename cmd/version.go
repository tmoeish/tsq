package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/tmoeish/tsq/v4/internal/buildinfo"
)

// VersionCmd 显示 tsq 的版本信息
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "显示 tsq 的版本信息",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("tsq 版本: %s\n", buildinfo.Version())
		fmt.Printf("构建时间: %s\n", buildinfo.BuildTime())
	},
}
