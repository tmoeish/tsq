package tsq

import (
	"context"
)

func TraceDB(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	return fn(ctx)
}
