package main

import (
	"context"
	"testing"

	"github.com/tmoeish/tsq/v4/examples/academy"
)

func TestAdvanced(t *testing.T) {
	rt, cleanup, err := academy.OpenSQLiteExampleDB()
	if err != nil {
		t.Fatalf("open example db: %v", err)
	}
	t.Cleanup(cleanup)

	summary, err := academy.RunAdvanced(context.Background(), rt)
	if err != nil {
		t.Fatalf("run advanced: %v", err)
	}

	if summary.Alias.PrerequisiteTitle == "" {
		t.Fatal("expected alias demo to return prerequisite title")
	}

	if len(summary.Aggregate) == 0 {
		t.Fatal("expected aggregate demo to return metrics")
	}

	if len(summary.InVar.Titles) == 0 {
		t.Fatal("expected dynamic IN demo to return titles")
	}

	if len(summary.Subquery.LearnersInDataTrack) == 0 {
		t.Fatal("expected subquery demo to return learners")
	}

	if len(summary.Case.Labels) == 0 {
		t.Fatal("expected case demo to return labels")
	}

	if summary.CTE.Total == 0 || len(summary.CTE.Titles) == 0 {
		t.Fatal("expected cte demo to return rows")
	}

	if len(summary.SetOps.UnionTitles) == 0 || len(summary.SetOps.StarterTitles) == 0 {
		t.Fatal("expected set ops demo to return rows")
	}

	if summary.Chunked.Before != summary.Chunked.After {
		t.Fatalf("expected chunked demo to leave enrollment count unchanged, got before=%d after=%d",
			summary.Chunked.Before, summary.Chunked.After)
	}
}
