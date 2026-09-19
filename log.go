package tsq

import (
	"context"
	"encoding/json"
	"log/slog"
)

// Logger is the subset of *slog.Logger the runtime writes to.
type Logger interface {
	Enabled(ctx context.Context, level slog.Level) bool
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}

func defaultRuntimeLogger() Logger {
	return slog.Default()
}

func (r *Runtime) warn(msg string, args ...any) {
	r.log(context.Background(), slog.LevelWarn, msg, args...)
}

func (r *Runtime) info(msg string, args ...any) {
	r.log(context.Background(), slog.LevelInfo, msg, args...)
}

func (r *Runtime) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if r == nil || r.logger == nil {
		return
	}

	logWith(ctx, r.logger, level, msg, args...)
}

func logWith(ctx context.Context, logger Logger, level slog.Level, msg string, args ...any) {
	if logger == nil || !logger.Enabled(ctx, level) {
		return
	}

	attrs := make([]slog.Attr, 0, len(args)/2)
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}

		attrs = append(attrs, slog.Any(key, args[i+1]))
	}

	logger.LogAttrs(ctx, level, msg, attrs...)
}

// logForExecutor routes execution-time diagnostics to the runtime's configured
// Logger when the executor belongs to one, and to slog.Default() otherwise.
func logForExecutor(ctx context.Context, exec Executor, level slog.Level, msg string, args ...any) {
	if rt := runtimeForExecutor(exec); rt != nil && rt.logger != nil {
		rt.log(ctx, level, msg, args...)
		return
	}

	logWith(ctx, slog.Default(), level, msg, args...)
}

// logSQLForExecutor logs a rendered statement and its bound arguments when the
// executor belongs to a runtime constructed with WithSQLLogging. Executors
// that carry no runtime (a bare *sql.DB, a WrapExecutor result) have no place to
// read the setting from, so they never log.
//
// Both guards run before compactJSON so that marshalling the arguments is only paid
// for when the record is actually going to be emitted.
func logSQLForExecutor(ctx context.Context, exec Executor, operation, sqlText string, args []any) {
	rt := runtimeForExecutor(exec)
	if rt == nil || !rt.logSQL || rt.logger == nil {
		return
	}

	if !rt.logger.Enabled(ctx, slog.LevelDebug) {
		return
	}

	rt.log(ctx, slog.LevelDebug, operation, "sql", sqlText, "args", compactJSON(args))
}

// compactJSON marshals a value to compact JSON, returning an empty string when the
// value cannot be marshalled. It is used for diagnostics only, never for SQL.
func compactJSON(obj any) string {
	bs, err := json.Marshal(obj)
	if err != nil {
		return ""
	}

	return string(bs)
}
