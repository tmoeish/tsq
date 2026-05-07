package tsq

import (
	"strings"

	"github.com/juju/errors"
)

type cteProvider interface {
	cteDefinition() cteDefinition
}

type cteDefinition struct {
	name          string
	selectCount   int
	keywordCount  int
	validate      func() error
	buildBody     func(bool) (string, []any)
	listTables    func() map[string]Table
	pageTables    func() map[string]Table
	collectNested func(*cteCollector, bool) error
}

type cteTable struct {
	name     string
	def      cteDefinition
	buildErr error
}

// CTE creates a reusable non-recursive WITH/CTE table handle from a query.
// Rebind existing columns to the returned table via RebindColumn or Col.WithTable
// to reference the CTE output columns in outer queries.
func CTE[O Owner](name string, query *QueryBuilder[O]) Table {
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
		name: name,
		def:  newCTEDefinition(name, cloneQuerySpec(query.spec)),
	}
}

func (cteTable) TSQOwner() {}

func (t cteTable) Table() string {
	return t.name
}

func (t cteTable) KwList() []SearchColumn {
	return nil
}

func (t cteTable) PhysicalTable() string {
	return t.name
}

func (t cteTable) buildError() error {
	return errors.Trace(t.buildErr)
}

func (t cteTable) cteDefinition() cteDefinition {
	return t.def
}

func newCTEDefinition[O Owner](name string, spec QuerySpec[O]) cteDefinition {
	cloned := cloneQuerySpec(spec)

	return cteDefinition{
		name:         name,
		selectCount:  len(cloned.Selects),
		keywordCount: len(cloned.KeywordSearch),
		validate: func() error {
			if err := cloned.validateJoinGraph(); err != nil {
				return errors.Trace(err)
			}

			if err := cloned.validateSetOperations(); err != nil {
				return errors.Trace(err)
			}

			return nil
		},
		buildBody: func(useKeyword bool) (string, []any) {
			return cloned.buildListBodySQL(useKeyword)
		},
		listTables: func() map[string]Table {
			return cloned.listQueryTables()
		},
		pageTables: func() map[string]Table {
			return cloned.pageQueryTables()
		},
		collectNested: func(c *cteCollector, useKeyword bool) error {
			return collectCTEFromSpec(c, cloned, useKeyword)
		},
	}
}
