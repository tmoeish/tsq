package buildinfo

import "runtime"

// version is injected at build time via ldflags.
var version = "v4.10.0"

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

// Version is the version this binary was built as.
func Version() string {
	return version
}

// Current returns the build and runtime metadata of this binary.
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
