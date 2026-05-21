package tsq

import "github.com/tmoeish/tsq/v4/dialect"

// wrappedExecutor wraps a standard SQL executor with dialect information.
type wrappedExecutor struct {
	SQLExecutor
	dialect dialect.Dialect
}

func (w wrappedExecutor) tsqDialect() dialect.Dialect {
	return w.dialect
}

// WrapExecutor wraps a SQLExecutor with dialect information.
func WrapExecutor(exec SQLExecutor, sqlDialect dialect.Dialect) SQLExecutor {
	if exec == nil {
		return nil
	}

	if provider, ok := exec.(dialectProvider); ok && provider.tsqDialect() == sqlDialect {
		return exec
	}

	return wrappedExecutor{SQLExecutor: exec, dialect: sqlDialect}
}
