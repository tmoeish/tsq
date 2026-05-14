package tsq

import (
	"errors"
	"strings"
)

type cteProvider interface {
	cteDefinition() cteDefinition
}

type cteDefinition struct {
	name          string
	selectCount   int
	keywordCount  int
	cols          []SQLColumn
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
func CTE[O Owner](name string, query completeQueryStage[O]) Table {
	name = strings.TrimSpace(name)
	if name == "" {
		return cteTable{buildErr: errors.New("cte name cannot be empty")}
	}

	if query == nil || query.core() == nil {
		return cteTable{
			name:     name,
			buildErr: errors.New("cte query builder cannot be nil"),
		}
	}

	core := ensureQueryBuilderCore(query.core(), builderPhaseBase)
	if core.buildErr != nil {
		return cteTable{
			name:     name,
			buildErr: core.buildErr,
		}
	}

	return cteTable{
		name: name,
		def:  newCTEDefinition(name, cloneQuerySpec(core.spec)),
	}
}

func (cteTable) TSQOwner() {}

func (t cteTable) Table() string {
	return t.name
}

func (t cteTable) SearchColumns() []SearchColumn {
	return nil
}

func (t cteTable) Cols() []SQLColumn {
	if len(t.def.cols) == 0 {
		return nil
	}

	return AliasColumns(t.def.cols, t)
}

func (cteTable) PrimaryKeys() []string {
	return nil
}

func (cteTable) AutoIncrement() bool {
	return false
}

func (cteTable) VersionColumn() string {
	return ""
}

func (t cteTable) PhysicalTable() string {
	return t.name
}

func (t cteTable) buildError() error {
	return t.buildErr
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
		cols:         SQLColumns(cloned.Selects...),
		validate: func() error {
			if err := cloned.validateJoinGraph(); err != nil {
				return err
			}

			if err := cloned.validateSetOperations(); err != nil {
				return err
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
