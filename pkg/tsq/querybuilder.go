package tsq

import (
	"fmt"
	"strings"
)

type QueryBuilder struct {
	selectFields         []IColumn
	selectTables         map[string]Table
	selectFieldFullNames []string

	conditions       []ICond
	conditionTables  map[string]Table
	conditionClauses []string

	kwFields         []IColumn
	kwTables         map[string]Table
	kwFieldFullNames []string

	leftJoin *leftJoin
}

func Select(fields ...IColumn) *QueryBuilder {
	tables := make(map[string]Table)
	var fuleNames []string
	for _, f := range fields {
		fuleNames = append(fuleNames, f.FullName())
		tables[f.Table().Table()] = f.Table()
	}

	return &QueryBuilder{
		selectFields:         fields,
		selectTables:         tables,
		selectFieldFullNames: fuleNames,

		conditionTables: make(map[string]Table),
		kwTables:        make(map[string]Table),
	}
}

// LeftJoin quals to `FROM left.Table, right.Table ON left=right`.
func (qb *QueryBuilder) LeftJoin(left IColumn, right IColumn) *QueryBuilder {
	qb.leftJoin = &leftJoin{
		left:  left,
		right: right,
	}
	qb.selectTables[left.Table().Table()] = left.Table()
	qb.selectTables[right.Table().Table()] = right.Table()
	return qb
}

type leftJoin struct {
	left  IColumn
	right IColumn
}

func (qb *QueryBuilder) Where(conds ...ICond) *QueryBuilder {
	var clauses []string
	for _, c := range conds {
		for tn, t := range c.Tables() {
			qb.selectTables[tn] = t
		}
		clauses = append(clauses, c.Clause())
	}

	qb.conditions = conds
	qb.conditionClauses = clauses

	return qb
}

func (qb *QueryBuilder) And(conds ...ICond) *QueryBuilder {
	qb.conditions = append(qb.conditions, conds...)
	for _, c := range conds {
		for tn, t := range c.Tables() {
			qb.conditionTables[tn] = t
		}
		qb.conditionClauses = append(qb.conditionClauses, c.Clause())
	}

	return qb
}

func (qb *QueryBuilder) AndIf(ok bool, conds ...ICond) *QueryBuilder {
	if ok {
		qb.And(conds...)
	}
	return qb
}

func (qb *QueryBuilder) KwSearch(fields ...IColumn) *QueryBuilder {
	tables := make(map[string]Table)
	var fullNames []string
	for _, f := range fields {
		tables[f.Table().Table()] = f.Table()
		fullNames = append(fullNames, f.FullName())
	}

	qb.kwFields = fields
	qb.kwTables = tables
	qb.kwFieldFullNames = fullNames

	return qb
}

func (qb *QueryBuilder) buildCntQuery() string {
	return "SELECT COUNT(0)" + qb.buildListFrom() + qb.buildListWhere()
}

func (qb *QueryBuilder) buildListQuery() string {
	return qb.buildSelect() + qb.buildListFrom() + qb.buildListWhere()
}

func (qb *QueryBuilder) buildKwCntQuery() string {
	return "SELECT COUNT(0)" + qb.buildPageFrom() + qb.buildPageWhere()
}

func (qb *QueryBuilder) buildKwListQuery() string {
	return qb.buildSelect() + qb.buildPageFrom() + qb.buildPageWhere()
}

func (qb *QueryBuilder) buildSelect() string {
	return "SELECT " + strings.Join(qb.selectFieldFullNames, ",\n\t")
}

func (qb *QueryBuilder) buildListWhere() string {
	clauses := make([]string, len(qb.conditions))
	for i, v := range qb.conditions {
		clauses[i] = v.Clause()
	}

	var where string
	if len(qb.conditions) > 0 {
		where = "\nWHERE " + strings.Join(clauses, "\n\tAND ")
	}

	return where
}

func (qb *QueryBuilder) buildPageWhere() string {
	if len(qb.kwFields) == 0 {
		return qb.buildListWhere()
	}

	clauses := make([]string, len(qb.conditions)+1)
	for i, v := range qb.conditions {
		clauses[i] = v.Clause()
	}
	or := make([]ICond, len(qb.kwFields))
	for i, f := range qb.kwFields {
		or[i] = Cond{
			tables: map[string]Table{
				f.Table().Table(): f.Table(),
			},
			clause: f.FullName() + " LIKE ?",
		}
	}
	clauses[len(qb.conditions)] = Or(or...).Clause()

	var where string
	if len(qb.conditions) > 0 {
		where = "\nWHERE " + strings.Join(clauses, "\n\tAND ")
	}

	return where
}

func (qb *QueryBuilder) buildLeftJoinFrom() string {
	return fmt.Sprintf(
		"\nFROM `%s` LEFT JOIN `%s` ON %s=%s",
		qb.leftJoin.left.Table().Table(),
		qb.leftJoin.right.Table().Table(),
		qb.leftJoin.left.FullName(),
		qb.leftJoin.right.FullName(),
	)
}

func (qb *QueryBuilder) buildListFrom() string {
	if qb.leftJoin != nil {
		return qb.buildLeftJoinFrom()
	}

	tables := qb.listQueryTables()
	var froms []string
	for i := range tables {
		froms = append(froms, fmt.Sprintf("`%s`", i))
	}

	return "\nFROM " + strings.Join(froms, ", ")
}

func (qb *QueryBuilder) buildPageFrom() string {
	if qb.leftJoin != nil {
		return qb.buildLeftJoinFrom()
	}

	if len(qb.kwFields) == 0 {
		return qb.buildListFrom()
	}

	tables := qb.pageQueryTables()
	var froms []string
	for i := range tables {
		froms = append(froms, fmt.Sprintf("`%s`", i))
	}

	return "\nFROM " + strings.Join(froms, ", ")
}

func (qb *QueryBuilder) listQueryTables() map[string]Table {
	tables := make(map[string]Table)
	for _, f := range qb.selectFields {
		tables[f.Table().Table()] = f.Table()
	}
	for _, cond := range qb.conditions {
		for _, t := range cond.Tables() {
			tables[t.Table()] = t
		}
	}

	return tables
}

func (qb *QueryBuilder) pageQueryTables() map[string]Table {
	tables := qb.listQueryTables()
	for _, f := range qb.kwFields {
		tables[f.Table().Table()] = f.Table()
	}
	return tables
}
