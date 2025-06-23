package tsq

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/juju/errors"
)

// ================================================
// 追踪类型定义
// ================================================

// Fn represents a function that can be traced
type Fn func(ctx context.Context) error

// Tracer is a middleware function that wraps another function for tracing/monitoring
type Tracer func(next Fn) Fn

// ================================================
// 追踪管理
// ================================================

// Global tracer registry
var tracers []Tracer

// AddTracer adds a tracer to the global registry
func AddTracer(tracer Tracer) {
	tracers = append(tracers, tracer)
}

// ClearTracers clears all registered tracers
func ClearTracers() {
	tracers = nil
}

// GetTracers returns all registered tracers
func GetTracers() []Tracer {
	result := make([]Tracer, len(tracers))
	copy(result, tracers)

	return result
}

// ================================================
// 追踪执行
// ================================================

// Trace executes a function with all registered tracers applied
// Tracers are applied in reverse order (LIFO) so the last added tracer wraps all others
func Trace(ctx context.Context, fn func(ctx context.Context) error) error {
	// Apply tracers in reverse order to create proper middleware chain
	wrappedFn := fn
	for i := len(tracers) - 1; i >= 0; i-- {
		wrappedFn = tracers[i](wrappedFn)
	}

	return wrappedFn(ctx)
}

func Trace1[T any](
	ctx context.Context,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	// Apply tracers in reverse order to create proper middleware chain
	var result T

	wrappedFn := func(ctx context.Context) error {
		var err error

		result, err = fn(ctx)
		if err != nil {
			return err
		}

		return nil
	}

	for i := len(tracers) - 1; i >= 0; i-- {
		wrappedFn = tracers[i](wrappedFn)
	}

	return result, wrappedFn(ctx)
}

// ================================================
// 内置追踪器
// ================================================

func PrintCost(next Fn) Fn {
	return func(ctx context.Context) error {
		start := time.Now()
		err := next(ctx)

		duration := time.Since(start)

		// You can customize this to use your preferred logging library
		if err != nil {
			// Log error with timing
			slog.Info("cost", "duration", duration, "error", errors.ErrorStack(err))
		} else {
			// Log success with timing
			slog.Info("cost", "duration", duration)
		}

		return err
	}
}

func PrintError(next Fn) Fn {
	return func(ctx context.Context) error {
		err := next(ctx)
		if err != nil {
			slog.Error("error", "error", errors.ErrorStack(err))
		}

		return err
	}
}

type contextKey string

const (
	printSQL contextKey = "printSQL"
)

func PrintSQL(next Fn) Fn {
	return func(ctx context.Context) error {
		return next(context.WithValue(ctx, printSQL, true))
	}
}

// ================================================
// 工具函数
// ================================================

// PrettyJSON returns indented JSON string of obj.
// Returns empty string if marshaling fails.
func PrettyJSON(obj any) string {
	bs, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return ""
	}

	return string(bs)
}

// CompactJSON returns compact JSON string of obj.
// Returns empty string if marshaling fails.
func CompactJSON(obj any) string {
	bs, err := json.Marshal(obj)
	if err != nil {
		return ""
	}

	return string(bs)
}
