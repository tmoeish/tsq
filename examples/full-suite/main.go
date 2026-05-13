package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/juju/errors"

	"github.com/tmoeish/tsq/examples/academy"
)

// full-suite prints the entire teaching path and ends with the comprehensive
// LearningJourney Result output used by the final examples README.
func main() {
	engine, cleanup, err := academy.OpenSQLiteExampleDB()
	if err != nil {
		slog.Error("open example db", "error", errors.ErrorStack(err))
		os.Exit(1)
	}
	defer cleanup()

	summary, err := academy.RunFullSuite(context.Background(), engine)
	if err != nil {
		slog.Error("run full suite", "error", errors.ErrorStack(err))
		os.Exit(1)
	}

	output, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		slog.Error("marshal full suite", "error", errors.ErrorStack(err))
		os.Exit(1)
	}

	fmt.Println(string(output))
}
