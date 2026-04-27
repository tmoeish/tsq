package database

import (
	"github.com/tmoeish/tsq"
)

// mustBuild is a package-private helper that builds a query and panics on error.
// It is only used at initialization time in generated code.
func mustBuild(qb *tsq.QueryBuilder) *tsq.Query {
	q, err := qb.Build()
	if err != nil {
		panic(err)
	}
	return q
}
