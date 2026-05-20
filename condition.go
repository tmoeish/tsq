package tsq

import (
	"maps"
	"strings"
)

// And combines conditions with SQL AND.
func And(conds ...Condition) Condition {
	if len(conds) == 0 {
		return rawCondition("1 = 1")
	}

	tables := make(map[string]Table)
	clauses := make([]string, 0, len(conds))

	for _, c := range conds {
		clause, condTables, _, err := validateConditionInput(c)
		if err != nil {
			return conditionImpl{buildErr: err}
		}

		maps.Copy(tables, condTables)

		clauses = append(clauses, clause)
	}

	return conditionImpl{
		tables: tables,
		expr:   "(" + strings.Join(clauses, " AND ") + ")",
		args:   collectConditionArgs(conds...),
	}
}

// Or combines conditions with SQL OR.
func Or(conds ...Condition) Condition {
	if len(conds) == 0 {
		return rawCondition("1 = 0")
	}

	tables := make(map[string]Table)
	clauses := make([]string, 0, len(conds))

	for _, c := range conds {
		clause, condTables, _, err := validateConditionInput(c)
		if err != nil {
			return conditionImpl{buildErr: err}
		}

		maps.Copy(tables, condTables)

		clauses = append(clauses, clause)
	}

	return conditionImpl{
		tables: tables,
		expr:   "(" + strings.Join(clauses, " OR ") + ")",
		args:   collectConditionArgs(conds...),
	}
}

// Condition is the runtime view of a rendered SQL predicate.
type Condition interface {
	Tables() map[string]Table // Tables returns the tables referenced by the predicate.
	Clause() string           // Clause returns the predicate SQL in canonical form.
	Args() []any              // Args returns the bind arguments captured by the predicate.
}

type rawConditionClauser interface {
	rawClause() string
}

type conditionImpl struct {
	tables   map[string]Table
	expr     string
	args     []any
	buildErr error
}

// Predicate carries a runtime condition together with its owner type.
type Predicate[O Owner] struct {
	conditionImpl
}

func pred[O Owner](cond conditionImpl) Predicate[O] {
	return Predicate[O]{conditionImpl: cond}
}

// Tables returns the tables referenced by the condition, keyed by logical table name.
func (c conditionImpl) Tables() map[string]Table {
	return cloneTableMap(c.tables)
}

// Clause returns the canonical SQL fragment for the condition.
func (c conditionImpl) Clause() string {
	return renderCanonicalSQL(c.expr)
}

func (c conditionImpl) rawClause() string {
	return c.expr
}

// Args returns the bind arguments captured by the condition.
func (c conditionImpl) Args() []any {
	return append([]any(nil), c.args...)
}

func (c conditionImpl) buildError() error {
	return c.buildErr
}

func rawCondition(expr string) conditionImpl {
	return conditionImpl{
		tables: map[string]Table{},
		expr:   expr,
	}
}

func collectConditionArgs(conds ...Condition) []any {
	var result []any

	for _, cond := range conds {
		if cond == nil {
			continue
		}

		result = append(result, cond.Args()...)
	}

	return result
}
