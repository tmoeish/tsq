package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/tmoeish/tsq/v4/examples/academy"
)

// quickstart prints the three beginner Academy demos as JSON so the output can be
// matched directly against quickstart/README.md while reading the example code.
func main() {
	rt, cleanup, err := academy.OpenSQLiteExampleDB()
	if err != nil {
		slog.Error("open example db", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	summary, err := academy.RunQuickstart(context.Background(), rt)
	if err != nil {
		slog.Error("run quickstart", "error", err)
		os.Exit(1)
	}

	output, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		slog.Error("marshal quickstart", "error", err)
		os.Exit(1)
	}

	fmt.Println(string(output))
}
