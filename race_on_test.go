//go:build race

package tsq

// raceEnabled reports a -race build. Single-goroutine tests that spend their time
// inside the transpiled SQLite driver run about forty times slower under the race
// detector and have no concurrency for it to check.
const raceEnabled = true
