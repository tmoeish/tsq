// Package tsq provides type-safe SQL query helpers and code generation utilities.
package tsq

import "gopkg.in/gorp.v2"

type wrappedExecutor struct {
	gorp.SqlExecutor
	dialect gorp.Dialect
}

func (w wrappedExecutor) TSQDialect() gorp.Dialect {
	return w.dialect
}

func WrapExecutor(exec gorp.SqlExecutor, dialect gorp.Dialect) gorp.SqlExecutor {
	if exec == nil {
		return nil
	}

	if _, ok := exec.(dialectProvider); ok {
		return exec
	}

	return wrappedExecutor{
		SqlExecutor: exec,
		dialect:     dialect,
	}
}

func WrapDBMapExecutor(exec gorp.SqlExecutor, db *gorp.DbMap) gorp.SqlExecutor {
	if db == nil {
		return exec
	}

	return WrapExecutor(exec, db.Dialect)
}
