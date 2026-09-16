package tsq

import "github.com/tmoeish/tsq/v5/dialect"

// wrappedExecutor wraps a standard SQL executor with dialect information.
type wrappedExecutor struct {
	Executor
	dialect dialect.Dialect
	runtime *Runtime
}

func (w wrappedExecutor) tsqDialect() dialect.Dialect {
	return w.dialect
}

func (w wrappedExecutor) tsqRuntime() *Runtime {
	return w.runtime
}

// WrapExecutor wraps a Executor with dialect information.
func WrapExecutor(exec Executor, sqlDialect dialect.Dialect) Executor {
	return wrapExecutor(exec, sqlDialect, nil)
}

func wrapExecutor(exec Executor, sqlDialect dialect.Dialect, rt *Runtime) Executor {
	if exec == nil {
		return nil
	}

	if rt == nil {
		if provider, ok := exec.(traceProvider); ok {
			rt = provider.tsqRuntime()
		}
	}

	// Reuse the executor only when it already carries both the dialect and the runtime
	// being asked for. A second copy of this condition used to follow, returning exec
	// unwrapped whenever the dialect matched, which made the runtime attachment above
	// unreachable: the wrapper was never applied to add a runtime to an executor that
	// lacked one.
	if provider, ok := exec.(dialectProvider); ok && provider.tsqDialect() == sqlDialect {
		traceExec, carriesRuntime := exec.(traceProvider)
		if rt == nil || (carriesRuntime && traceExec.tsqRuntime() == rt) {
			return exec
		}
	}

	return wrappedExecutor{
		Executor: exec,
		dialect:  sqlDialect,
		runtime:  rt,
	}
}
