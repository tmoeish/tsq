package tsq

import (
	"fmt"
	"runtime"
)

// ================================================
// 版本信息变量
// ================================================

// Version 是构建时通过 ldflags 注入的版本信息
// 使用方式: go build -ldflags "-X github.com/tmoeish/tsq.Version=v1.0.0"
var Version = "dev"

// BuildTime 是构建时通过 ldflags 注入的构建时间
// 使用方式: go build -ldflags "-X github.com/tmoeish/tsq.BuildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
var BuildTime = "unknown"

// GitCommit 是构建时通过 ldflags 注入的 Git 提交哈希
// 使用方式: go build -ldflags "-X github.com/tmoeish/tsq.GitCommit=$(git rev-parse HEAD)"
var GitCommit = "unknown"

// GitBranch 是构建时通过 ldflags 注入的 Git 分支名
// 使用方式: go build -ldflags "-X github.com/tmoeish/tsq.GitBranch=$(git rev-parse --abbrev-ref HEAD)"
var GitBranch = "unknown"

// ================================================
// 版本信息结构体
// ================================================

// VersionInfo contains comprehensive version information
type VersionInfo struct {
	Version   string `json:"version"`    // 应用版本
	BuildTime string `json:"build_time"` // 构建时间
	GitCommit string `json:"git_commit"` // Git 提交哈希
	GitBranch string `json:"git_branch"` // Git 分支
	GoVersion string `json:"go_version"` // Go 版本
	Platform  string `json:"platform"`   // 平台信息
	Arch      string `json:"arch"`       // 架构信息
}

// ================================================
// 版本信息函数
// ================================================

// GetVersion returns the current version string
func GetVersion() string {
	return Version
}

// GetBuildTime returns the build time string
func GetBuildTime() string {
	return BuildTime
}

// GetGitCommit returns the git commit hash
func GetGitCommit() string {
	return GitCommit
}

// GetGitBranch returns the git branch name
func GetGitBranch() string {
	return GitBranch
}

// GetVersionInfo returns comprehensive version information
func GetVersionInfo() *VersionInfo {
	return &VersionInfo{
		Version:   Version,
		BuildTime: BuildTime,
		GitCommit: GitCommit,
		GitBranch: GitBranch,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS,
		Arch:      runtime.GOARCH,
	}
}

// String returns a formatted version string
func (v *VersionInfo) String() string {
	return fmt.Sprintf("TSQ %s (built %s from %s@%s with %s on %s/%s)",
		v.Version, v.BuildTime, v.GitBranch, v.GitCommit[:8], v.GoVersion, v.Platform, v.Arch)
}

// ShortString returns a short version string
func (v *VersionInfo) ShortString() string {
	return fmt.Sprintf("TSQ %s", v.Version)
}

// ================================================
// 便捷函数
// ================================================

// PrintVersion prints version information to stdout
func PrintVersion() {
	fmt.Println(GetVersionInfo().String())
}

// PrintVersionJSON prints version information as JSON to stdout
func PrintVersionJSON() {
	fmt.Println(PrettyJSON(GetVersionInfo()))
}
