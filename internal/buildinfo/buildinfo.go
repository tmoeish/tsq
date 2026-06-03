package buildinfo

import (
	"fmt"
	"runtime"
)

// version is injected at build time via ldflags.
var version = "v4.1.23"

// buildTime is injected at build time via ldflags.
var buildTime = "unknown"

// gitCommit is injected at build time via ldflags.
var gitCommit = "unknown"

// gitBranch is injected at build time via ldflags.
var gitBranch = "unknown"

// Info contains build and runtime version metadata for the tsq CLI and generator.
type Info struct {
	Version   string `json:"version"`
	BuildTime string `json:"build_time"`
	GitCommit string `json:"git_commit"`
	GitBranch string `json:"git_branch"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
	Arch      string `json:"arch"`
}

func Version() string {
	return version
}

func BuildTime() string {
	return buildTime
}

func GitCommit() string {
	return gitCommit
}

func GitBranch() string {
	return gitBranch
}

func Current() *Info {
	return &Info{
		Version:   version,
		BuildTime: buildTime,
		GitCommit: gitCommit,
		GitBranch: gitBranch,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS,
		Arch:      runtime.GOARCH,
	}
}

func (v *Info) String() string {
	if v == nil {
		return "TSQ unknown"
	}

	commit := v.GitCommit
	if len(commit) > 8 {
		commit = commit[:8]
	}

	return fmt.Sprintf("TSQ %s (built %s from %s@%s with %s on %s/%s)",
		v.Version, v.BuildTime, v.GitBranch, commit, v.GoVersion, v.Platform, v.Arch)
}

func (v *Info) ShortString() string {
	if v == nil {
		return "TSQ unknown"
	}

	return fmt.Sprintf("TSQ %s", v.Version)
}
