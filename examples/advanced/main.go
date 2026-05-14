package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/tmoeish/tsq/examples/academy"
)

// advanced prints the focused feature demos as JSON: each top-level field maps to
// one advanced TSQ capability described in advanced/README.md.
func main() {
	engine, cleanup, err := academy.OpenSQLiteExampleDB()
	if err != nil {
		slog.Error("open example db", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	summary, err := academy.RunAdvanced(context.Background(), engine)
	if err != nil {
		slog.Error("run advanced", "error", err)
		os.Exit(1)
	}

	output, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		slog.Error("marshal advanced", "error", err)
		os.Exit(1)
	}

	fmt.Println(string(output))
}
