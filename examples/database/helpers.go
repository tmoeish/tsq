package database

import (
	"github.com/tmoeish/tsq"
)

// mustBuild is a package-private helper for building queries at init time.
// Since MustBuild() is no longer in the public API of tsq.QueryBuilder,
// we need this wrapper for generated code compatibility.
// This function is only used in generated *_tsq.go files.
func mustBuild(qb *tsq.QueryBuilder) *tsq.Query {
	q, err := qb.Build()
	if err != nil {
		panic(err)
	}
	return q
}
