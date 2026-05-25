package tsq

import "github.com/tmoeish/tsq/v4/dialect"

// wrappedExecutor wraps a standard SQL executor with dialect information.
type wrappedExecutor struct {
	SQLExecutor
	dialect dialect.Dialect
	runtime *Runtime
}

func (w wrappedExecutor) tsqDialect() dialect.Dialect {
	return w.dialect
}

func (w wrappedExecutor) tsqRuntime() *Runtime {
	return w.runtime
}

// WrapExecutor wraps a SQLExecutor with dialect information.
func WrapExecutor(exec SQLExecutor, sqlDialect dialect.Dialect) SQLExecutor {
	return wrapExecutor(exec, sqlDialect, nil)
}

func wrapExecutor(exec SQLExecutor, sqlDialect dialect.Dialect, rt *Runtime) SQLExecutor {
	if exec == nil {
		return nil
	}

	if rt == nil {
		if provider, ok := exec.(traceProvider); ok {
			rt = provider.tsqRuntime()
		}
	}

	if provider, ok := exec.(dialectProvider); ok && provider.tsqDialect() == sqlDialect {
		if rt == nil {
			return exec
		}

		if traceExec, ok := exec.(traceProvider); ok && traceExec.tsqRuntime() == rt {
			return exec
		}
	}

	if provider, ok := exec.(dialectProvider); ok && provider.tsqDialect() == sqlDialect {
		return exec
	}

	return wrappedExecutor{
		SQLExecutor: exec,
		dialect:     sqlDialect,
		runtime:     rt,
	}
}
