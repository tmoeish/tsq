package tsq

// wrappedExecutor wraps a standard SQL executor with dialect information.
type wrappedExecutor struct {
	SQLExecutor
	dialect Dialect
}

// TSQDialect exposes the wrapped executor's dialect to renderer helpers.
func (w wrappedExecutor) TSQDialect() Dialect {
	return w.dialect
}

// WrapExecutor wraps a SQLExecutor with dialect information.
func WrapExecutor(exec SQLExecutor, dialect Dialect) SQLExecutor {
	if exec == nil {
		return nil
	}

	if provider, ok := exec.(dialectProvider); ok && provider.TSQDialect() == dialect {
		return exec
	}

	return wrappedExecutor{SQLExecutor: exec, dialect: dialect}
}
