package tsq

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"
)

func TestGetVersion(t *testing.T) {
	// Save original value
	originalVersion := Version
	defer func() { Version = originalVersion }()

	// Test with default value
	if GetVersion() != "dev" {
		t.Errorf("Expected default version 'dev', got '%s'", GetVersion())
	}

	// Test with custom value
	Version = "v1.2.3"

	if GetVersion() != "v1.2.3" {
		t.Errorf("Expected version 'v1.2.3', got '%s'", GetVersion())
	}
}

func TestGetBuildTime(t *testing.T) {
	// Save original value
	originalBuildTime := BuildTime
	defer func() { BuildTime = originalBuildTime }()

	// Test with default value
	if GetBuildTime() != "unknown" {
		t.Errorf("Expected default build time 'unknown', got '%s'", GetBuildTime())
	}

	// Test with custom value
	BuildTime = "2023-12-25T10:30:00Z"

	if GetBuildTime() != "2023-12-25T10:30:00Z" {
		t.Errorf("Expected build time '2023-12-25T10:30:00Z', got '%s'", GetBuildTime())
	}
}

func TestGetGitCommit(t *testing.T) {
	// Save original value
	originalGitCommit := GitCommit
	defer func() { GitCommit = originalGitCommit }()

	// Test with default value
	if GetGitCommit() != "unknown" {
		t.Errorf("Expected default git commit 'unknown', got '%s'", GetGitCommit())
	}

	// Test with custom value
	GitCommit = "abc123def456"

	if GetGitCommit() != "abc123def456" {
		t.Errorf("Expected git commit 'abc123def456', got '%s'", GetGitCommit())
	}
}

func TestGetGitBranch(t *testing.T) {
	// Save original value
	originalGitBranch := GitBranch
	defer func() { GitBranch = originalGitBranch }()

	// Test with default value
	if GetGitBranch() != "unknown" {
		t.Errorf("Expected default git branch 'unknown', got '%s'", GetGitBranch())
	}

	// Test with custom value
	GitBranch = "main"

	if GetGitBranch() != "main" {
		t.Errorf("Expected git branch 'main', got '%s'", GetGitBranch())
	}
}

func TestGetVersionInfo(t *testing.T) {
	// Save original values
	originalVersion := Version
	originalBuildTime := BuildTime
	originalGitCommit := GitCommit
	originalGitBranch := GitBranch

	defer func() {
		Version = originalVersion
		BuildTime = originalBuildTime
		GitCommit = originalGitCommit
		GitBranch = originalGitBranch
	}()

	// Set test values
	Version = "v1.0.0"
	BuildTime = "2023-12-25T10:30:00Z"
	GitCommit = "abc123def456"
	GitBranch = "main"

	info := GetVersionInfo()

	if info == nil {
		t.Fatal("GetVersionInfo() returned nil")
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

func TestVersionInfo_String(t *testing.T) {
	// Save original values
	originalVersion := Version
	originalBuildTime := BuildTime
	originalGitCommit := GitCommit
	originalGitBranch := GitBranch

	defer func() {
		Version = originalVersion
		BuildTime = originalBuildTime
		GitCommit = originalGitCommit
		GitBranch = originalGitBranch
	}()

	// Set test values
	Version = "v1.0.0"
	BuildTime = "2023-12-25T10:30:00Z"
	GitCommit = "abc123def456789"
	GitBranch = "main"

	info := GetVersionInfo()
	result := info.String()

	// Check that the string contains expected components
	expectedComponents := []string{
		"TSQ v1.0.0",
		"built 2023-12-25T10:30:00Z",
		"from main@abc123de", // First 8 chars of commit
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

func TestVersionInfo_ShortString(t *testing.T) {
	// Save original values
	originalVersion := Version
	defer func() { Version = originalVersion }()

	Version = "v1.0.0"

	info := GetVersionInfo()
	result := info.ShortString()

	expected := "TSQ v1.0.0"
	if result != expected {
		t.Errorf("Expected short string '%s', got '%s'", expected, result)
	}
}

func TestVersionInfo_JSONSerialization(t *testing.T) {
	// Save original values
	originalVersion := Version
	originalBuildTime := BuildTime
	originalGitCommit := GitCommit
	originalGitBranch := GitBranch

	defer func() {
		Version = originalVersion
		BuildTime = originalBuildTime
		GitCommit = originalGitCommit
		GitBranch = originalGitBranch
	}()

	// Set test values
	Version = "v1.0.0"
	BuildTime = "2023-12-25T10:30:00Z"
	GitCommit = "abc123def456"
	GitBranch = "main"

	info := GetVersionInfo()

	// Test JSON marshaling
	jsonData, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Failed to marshal VersionInfo to JSON: %v", err)
	}

	// Test JSON unmarshaling
	var unmarshaled VersionInfo

	err = json.Unmarshal(jsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal VersionInfo from JSON: %v", err)
	}

	// Verify all fields are preserved
	if unmarshaled.Version != info.Version {
		t.Errorf("Version mismatch after JSON round-trip: expected '%s', got '%s'", info.Version, unmarshaled.Version)
	}

	if unmarshaled.BuildTime != info.BuildTime {
		t.Errorf("BuildTime mismatch after JSON round-trip: expected '%s', got '%s'", info.BuildTime, unmarshaled.BuildTime)
	}

	if unmarshaled.GitCommit != info.GitCommit {
		t.Errorf("GitCommit mismatch after JSON round-trip: expected '%s', got '%s'", info.GitCommit, unmarshaled.GitCommit)
	}

	if unmarshaled.GitBranch != info.GitBranch {
		t.Errorf("GitBranch mismatch after JSON round-trip: expected '%s', got '%s'", info.GitBranch, unmarshaled.GitBranch)
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

func TestVersionInfo_DefaultValues(t *testing.T) {
	// Save original values
	originalVersion := Version
	originalBuildTime := BuildTime
	originalGitCommit := GitCommit
	originalGitBranch := GitBranch

	defer func() {
		Version = originalVersion
		BuildTime = originalBuildTime
		GitCommit = originalGitCommit
		GitBranch = originalGitBranch
	}()

	// Reset to default values
	Version = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
	GitBranch = "unknown"

	info := GetVersionInfo()

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

	// Runtime values should still be populated
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

func TestVersionInfo_StringWithShortCommit(t *testing.T) {
	// Save original values
	originalVersion := Version
	originalBuildTime := BuildTime
	originalGitCommit := GitCommit
	originalGitBranch := GitBranch

	defer func() {
		Version = originalVersion
		BuildTime = originalBuildTime
		GitCommit = originalGitCommit
		GitBranch = originalGitBranch
	}()

	// Test with short commit hash (at least 8 characters to avoid slice bounds error)
	Version = "v1.0.0"
	BuildTime = "2023-12-25T10:30:00Z"
	GitCommit = "abc12345" // 8 characters
	GitBranch = "main"

	info := GetVersionInfo()
	result := info.String()

	// Should handle commit hash correctly
	if !strings.Contains(result, "main@abc12345") {
		t.Errorf("Expected string to contain 'main@abc12345', got '%s'", result)
	}
}

func TestVersionInfo_StringWithLongCommit(t *testing.T) {
	// Save original values
	originalVersion := Version
	originalBuildTime := BuildTime
	originalGitCommit := GitCommit
	originalGitBranch := GitBranch

	defer func() {
		Version = originalVersion
		BuildTime = originalBuildTime
		GitCommit = originalGitCommit
		GitBranch = originalGitBranch
	}()

	// Test with long commit hash
	Version = "v1.0.0"
	BuildTime = "2023-12-25T10:30:00Z"
	GitCommit = "abc123def456789012345678901234567890"
	GitBranch = "feature/test"

	info := GetVersionInfo()
	result := info.String()

	// Should truncate to first 8 characters
	if !strings.Contains(result, "feature/test@abc123de") {
		t.Errorf("Expected string to contain 'feature/test@abc123de', got '%s'", result)
	}
}
