package buildinfo

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	originalVersion := version
	defer func() { version = originalVersion }()

	currentVersion := Version()
	if currentVersion != "v4.1.3" && currentVersion != "dev" {
		t.Errorf("Expected version to be 'v4.1.3' or 'dev', got '%s'", Version())
	}

	version = "v1.2.3"

	if Version() != "v1.2.3" {
		t.Errorf("Expected version 'v1.2.3', got '%s'", Version())
	}
}

func TestBuildTime(t *testing.T) {
	originalBuildTime := buildTime
	defer func() { buildTime = originalBuildTime }()

	if BuildTime() != "unknown" {
		t.Errorf("Expected default build time 'unknown', got '%s'", BuildTime())
	}

	buildTime = "2023-12-25T10:30:00Z"

	if BuildTime() != "2023-12-25T10:30:00Z" {
		t.Errorf("Expected build time '2023-12-25T10:30:00Z', got '%s'", BuildTime())
	}
}

func TestGitCommit(t *testing.T) {
	originalGitCommit := gitCommit
	defer func() { gitCommit = originalGitCommit }()

	if GitCommit() != "unknown" {
		t.Errorf("Expected default git commit 'unknown', got '%s'", GitCommit())
	}

	gitCommit = "abc123def456"

	if GitCommit() != "abc123def456" {
		t.Errorf("Expected git commit 'abc123def456', got '%s'", GitCommit())
	}
}

func TestGitBranch(t *testing.T) {
	originalGitBranch := gitBranch
	defer func() { gitBranch = originalGitBranch }()

	if GitBranch() != "unknown" {
		t.Errorf("Expected default git branch 'unknown', got '%s'", GitBranch())
	}

	gitBranch = "main"

	if GitBranch() != "main" {
		t.Errorf("Expected git branch 'main', got '%s'", GitBranch())
	}
}

func TestCurrent(t *testing.T) {
	originalVersion := version
	originalBuildTime := buildTime
	originalGitCommit := gitCommit
	originalGitBranch := gitBranch

	defer func() {
		version = originalVersion
		buildTime = originalBuildTime
		gitCommit = originalGitCommit
		gitBranch = originalGitBranch
	}()

	version = "v1.0.0"
	buildTime = "2023-12-25T10:30:00Z"
	gitCommit = "abc123def456"
	gitBranch = "main"

	info := Current()

	if info == nil {
		t.Fatal("Current() returned nil")
	}

	if info.Version != "v1.0.0" {
		t.Errorf("Expected version 'v1.0.0', got '%s'", info.Version)
	}

	if info.BuildTime != "2023-12-25T10:30:00Z" {
		t.Errorf("Expected build time '2023-12-25T10:30:00Z', got '%s'", info.BuildTime)
	}

	if info.GitCommit != "abc123def456" {
		t.Errorf("Expected git commit 'abc123def456', got '%s'", info.GitCommit)
	}

	if info.GitBranch != "main" {
		t.Errorf("Expected git branch 'main', got '%s'", info.GitBranch)
	}

	if info.GoVersion != runtime.Version() {
		t.Errorf("Expected Go version '%s', got '%s'", runtime.Version(), info.GoVersion)
	}

	if info.Platform != runtime.GOOS {
		t.Errorf("Expected platform '%s', got '%s'", runtime.GOOS, info.Platform)
	}

	if info.Arch != runtime.GOARCH {
		t.Errorf("Expected arch '%s', got '%s'", runtime.GOARCH, info.Arch)
	}
}

func TestInfo_String(t *testing.T) {
	originalVersion := version
	originalBuildTime := buildTime
	originalGitCommit := gitCommit
	originalGitBranch := gitBranch

	defer func() {
		version = originalVersion
		buildTime = originalBuildTime
		gitCommit = originalGitCommit
		gitBranch = originalGitBranch
	}()

	version = "v1.0.0"
	buildTime = "2023-12-25T10:30:00Z"
	gitCommit = "abc123def456789"
	gitBranch = "main"

	info := Current()
	result := info.String()

	expectedComponents := []string{
		"TSQ v1.0.0",
		"built 2023-12-25T10:30:00Z",
		"from main@abc123de",
		runtime.Version(),
		runtime.GOOS,
		runtime.GOARCH,
	}

	for _, component := range expectedComponents {
		if !strings.Contains(result, component) {
			t.Errorf("Expected string to contain '%s', got '%s'", component, result)
		}
	}
}

func TestInfo_StringHandlesShortCommit(t *testing.T) {
	info := &Info{
		Version:   "dev",
		BuildTime: "unknown",
		GitCommit: "short",
		GitBranch: "main",
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS,
		Arch:      runtime.GOARCH,
	}

	result := info.String()
	if !strings.Contains(result, "main@short") {
		t.Fatalf("expected formatted string to include full short commit, got %q", result)
	}
}

func TestInfo_ShortString(t *testing.T) {
	originalVersion := version
	defer func() { version = originalVersion }()

	version = "v1.0.0"

	info := Current()
	result := info.ShortString()

	expected := "TSQ v1.0.0"
	if result != expected {
		t.Errorf("Expected short string '%s', got '%s'", expected, result)
	}
}

func TestInfo_NilReceiverHelpers(t *testing.T) {
	var info *Info

	if got := info.String(); got != "TSQ unknown" {
		t.Fatalf("expected nil String fallback, got %q", got)
	}

	if got := info.ShortString(); got != "TSQ unknown" {
		t.Fatalf("expected nil ShortString fallback, got %q", got)
	}
}

func TestInfo_JSONSerialization(t *testing.T) {
	originalVersion := version
	originalBuildTime := buildTime
	originalGitCommit := gitCommit
	originalGitBranch := gitBranch

	defer func() {
		version = originalVersion
		buildTime = originalBuildTime
		gitCommit = originalGitCommit
		gitBranch = originalGitBranch
	}()

	version = "v1.0.0"
	buildTime = "2023-12-25T10:30:00Z"
	gitCommit = "abc123def456"
	gitBranch = "main"

	info := Current()

	jsonData, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Failed to marshal Info to JSON: %v", err)
	}

	var unmarshaled Info

	err = json.Unmarshal(jsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal Info from JSON: %v", err)
	}

	if unmarshaled.Version != info.Version {
		t.Errorf("version mismatch after JSON round-trip: expected '%s', got '%s'", info.Version, unmarshaled.Version)
	}

	if unmarshaled.BuildTime != info.BuildTime {
		t.Errorf("buildTime mismatch after JSON round-trip: expected '%s', got '%s'", info.BuildTime, unmarshaled.BuildTime)
	}

	if unmarshaled.GitCommit != info.GitCommit {
		t.Errorf("gitCommit mismatch after JSON round-trip: expected '%s', got '%s'", info.GitCommit, unmarshaled.GitCommit)
	}

	if unmarshaled.GitBranch != info.GitBranch {
		t.Errorf("gitBranch mismatch after JSON round-trip: expected '%s', got '%s'", info.GitBranch, unmarshaled.GitBranch)
	}

	if unmarshaled.GoVersion != info.GoVersion {
		t.Errorf("GoVersion mismatch after JSON round-trip: expected '%s', got '%s'", info.GoVersion, unmarshaled.GoVersion)
	}

	if unmarshaled.Platform != info.Platform {
		t.Errorf("Platform mismatch after JSON round-trip: expected '%s', got '%s'", info.Platform, unmarshaled.Platform)
	}

	if unmarshaled.Arch != info.Arch {
		t.Errorf("Arch mismatch after JSON round-trip: expected '%s', got '%s'", info.Arch, unmarshaled.Arch)
	}
}

func TestInfo_DefaultValues(t *testing.T) {
	originalVersion := version
	originalBuildTime := buildTime
	originalGitCommit := gitCommit
	originalGitBranch := gitBranch

	defer func() {
		version = originalVersion
		buildTime = originalBuildTime
		gitCommit = originalGitCommit
		gitBranch = originalGitBranch
	}()

	version = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
	gitBranch = "unknown"

	info := Current()

	if info.Version != "dev" {
		t.Errorf("Expected default version 'dev', got '%s'", info.Version)
	}

	if info.BuildTime != "unknown" {
		t.Errorf("Expected default build time 'unknown', got '%s'", info.BuildTime)
	}

	if info.GitCommit != "unknown" {
		t.Errorf("Expected default git commit 'unknown', got '%s'", info.GitCommit)
	}

	if info.GitBranch != "unknown" {
		t.Errorf("Expected default git branch 'unknown', got '%s'", info.GitBranch)
	}

	if info.GoVersion == "" {
		t.Error("GoVersion should not be empty")
	}

	if info.Platform == "" {
		t.Error("Platform should not be empty")
	}

	if info.Arch == "" {
		t.Error("Arch should not be empty")
	}
}

func TestInfo_StringWithShortCommit(t *testing.T) {
	originalVersion := version
	originalBuildTime := buildTime
	originalGitCommit := gitCommit
	originalGitBranch := gitBranch

	defer func() {
		version = originalVersion
		buildTime = originalBuildTime
		gitCommit = originalGitCommit
		gitBranch = originalGitBranch
	}()

	version = "v1.0.0"
	buildTime = "2023-12-25T10:30:00Z"
	gitCommit = "abc12345"
	gitBranch = "main"

	info := Current()
	result := info.String()

	if !strings.Contains(result, "main@abc12345") {
		t.Errorf("Expected string to contain 'main@abc12345', got '%s'", result)
	}
}

func TestInfo_StringWithLongCommit(t *testing.T) {
	originalVersion := version
	originalBuildTime := buildTime
	originalGitCommit := gitCommit
	originalGitBranch := gitBranch

	defer func() {
		version = originalVersion
		buildTime = originalBuildTime
		gitCommit = originalGitCommit
		gitBranch = originalGitBranch
	}()

	version = "v1.0.0"
	buildTime = "2023-12-25T10:30:00Z"
	gitCommit = "abc123def456789012345678901234567890"
	gitBranch = "feature/test"

	info := Current()
	result := info.String()

	if !strings.Contains(result, "feature/test@abc123de") {
		t.Errorf("Expected string to contain 'feature/test@abc123de', got '%s'", result)
	}
}
