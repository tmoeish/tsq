package tsq

import "github.com/tmoeish/tsq/v4/dialect"

// wrappedExecutor wraps a standard SQL executor with dialect information.
type wrappedExecutor struct {
	SQLExecutor
	dialect      dialect.Dialect
	traceManager *traceManager
}

func (w wrappedExecutor) tsqDialect() dialect.Dialect {
	return w.dialect
}

func (w wrappedExecutor) tsqTraceManager() *traceManager {
	return w.traceManager
}

// WrapExecutor wraps a SQLExecutor with dialect information.
func WrapExecutor(exec SQLExecutor, sqlDialect dialect.Dialect) SQLExecutor {
	return wrapExecutor(exec, sqlDialect, nil)
}

func wrapExecutor(exec SQLExecutor, sqlDialect dialect.Dialect, tm *traceManager) SQLExecutor {
	if exec == nil {
		return nil
	}

	if tm == nil {
		if provider, ok := exec.(traceProvider); ok {
			tm = provider.tsqTraceManager()
		}
	}

	if provider, ok := exec.(dialectProvider); ok && provider.tsqDialect() == sqlDialect {
		if tm == nil {
			return exec
		}

		if traceExec, ok := exec.(traceProvider); ok && traceExec.tsqTraceManager() == tm {
			return exec
		}
	}

	if provider, ok := exec.(dialectProvider); ok && provider.tsqDialect() == sqlDialect {
		return exec
	}

	return wrappedExecutor{
		SQLExecutor:  exec,
		dialect:      sqlDialect,
		traceManager: tm,
	}
}
