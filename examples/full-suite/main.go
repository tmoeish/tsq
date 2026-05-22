package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/tmoeish/tsq/v4/examples/academy"
)

// full-suite prints the entire teaching path and ends with the comprehensive
// LearningJourney Result output used by the final examples README.
func main() {
	rt, cleanup, err := academy.OpenSQLiteExampleDB()
	if err != nil {
		slog.Error("open example db", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	summary, err := academy.RunFullSuite(context.Background(), rt)
	if err != nil {
		slog.Error("run full suite", "error", err)
		os.Exit(1)
	}

	output, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		slog.Error("marshal full suite", "error", err)
		os.Exit(1)
	}

	fmt.Println(string(output))
}
