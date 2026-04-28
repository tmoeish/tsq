package tsq

import (
	"strings"

	"github.com/juju/errors"
)

type cteProvider interface {
	cteDefinition() cteDefinition
}

type cteDefinition struct {
	name                  string
	spec                  QuerySpec
	allowCartesianProduct bool
}

type cteTable struct {
	name                  string
	spec                  QuerySpec
	allowCartesianProduct bool
	buildErr              error
}

// CTE creates a reusable non-recursive WITH/CTE table handle from a query.
// Rebind existing columns to the returned table via RebindColumn or Col.WithTable
// to reference the CTE output columns in outer queries.
func CTE(name string, query *QueryBuilder) Table {
	name = strings.TrimSpace(name)
	if name == "" {
		return cteTable{buildErr: errors.New("cte name cannot be empty")}
	}

	if query == nil {
		return cteTable{
			name:     name,
			buildErr: errors.New("cte query builder cannot be nil"),
		}
	}

	query = query.ensureInitialized()
	if query.buildErr != nil {
		return cteTable{
			name:     name,
			buildErr: errors.Trace(query.buildErr),
		}
	}

	return cteTable{
		name:                  name,
		spec:                  cloneQuerySpec(query.spec),
		allowCartesianProduct: query.allowCartesianProduct,
	}
}

func (t cteTable) Table() string {
	return t.name
}

func (t cteTable) KwList() []Column {
	return nil
}

func (t cteTable) PhysicalTable() string {
	return t.name
}

func (t cteTable) buildError() error {
	return t.buildErr
}

func (t cteTable) cteDefinition() cteDefinition {
	return cteDefinition{
		name:                  t.name,
		spec:                  cloneQuerySpec(t.spec),
		allowCartesianProduct: t.allowCartesianProduct,
	}
}
