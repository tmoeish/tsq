package tsq

import (
	"strings"
)

// ================================================
// 查询构建器结构体和工厂方法
// ================================================

// JoinType represents different types of SQL joins
type JoinType string

const (
	LeftJoinType  JoinType = "LEFT JOIN"
	InnerJoinType JoinType = "INNER JOIN"
	RightJoinType JoinType = "RIGHT JOIN"
	FullJoinType  JoinType = "FULL JOIN"
	CrossJoinType JoinType = "CROSS JOIN"
)

// QueryBuilder builds SQL queries with type safety
type QueryBuilder struct {
	// Select 相关字段
	selectCols         []Column
	selectTables       map[string]Table
	selectColFullnames []string

	// 条件相关字段
	conditions       []Condition
	conditionTables  map[string]Table
	conditionClauses []string

	// 关键词搜索相关字段
	kwCols   []Column
	kwTables map[string]Table

	// JOIN 相关字段
	joins []join

	// GROUP BY 和 HAVING 相关字段
	groupByCols      []Column
	havingConditions []Condition
}

// join represents any type of JOIN operation
type join struct {
	joinType JoinType
	left     Column
	right    Column
	table    Table // for CROSS JOIN, only table is needed
}

// Select creates a new QueryBuilder with the specified columns
func Select(cols ...Column) *QueryBuilder {
	tables := make(map[string]Table, len(cols))
	colFullnames := make([]string, 0, len(cols))

	for _, col := range cols {
		colFullnames = append(colFullnames, col.QualifiedName())
		tables[col.Table().Table()] = col.Table()
	}

	return &QueryBuilder{
		selectCols:         cols,
		selectColFullnames: colFullnames,
		selectTables:       tables,
		conditionTables:    make(map[string]Table),
		kwTables:           make(map[string]Table),
		joins:              make([]join, 0),
		groupByCols:        make([]Column, 0),
		havingConditions:   make([]Condition, 0),
	}
}

// ================================================
// JOIN 方法 - 增强版
// ================================================

// LeftJoin adds a LEFT JOIN clause. Equivalent to `FROM left.Table LEFT JOIN right.Table ON left=right`.
func (qb *QueryBuilder) LeftJoin(left Column, right Column) *QueryBuilder {
	qb.joins = append(qb.joins, join{
		joinType: LeftJoinType,
		left:     left,
		right:    right,
	})
	qb.selectTables[left.Table().Table()] = left.Table()
	qb.selectTables[right.Table().Table()] = right.Table()

	return qb
}

// InnerJoin adds an INNER JOIN clause
func (qb *QueryBuilder) InnerJoin(left Column, right Column) *QueryBuilder {
	qb.joins = append(qb.joins, join{
		joinType: InnerJoinType,
		left:     left,
		right:    right,
	})
	qb.selectTables[left.Table().Table()] = left.Table()
	qb.selectTables[right.Table().Table()] = right.Table()

	return qb
}

// RightJoin adds a RIGHT JOIN clause
func (qb *QueryBuilder) RightJoin(left Column, right Column) *QueryBuilder {
	qb.joins = append(qb.joins, join{
		joinType: RightJoinType,
		left:     left,
		right:    right,
	})
	qb.selectTables[left.Table().Table()] = left.Table()
	qb.selectTables[right.Table().Table()] = right.Table()

	return qb
}

// FullJoin adds a FULL JOIN clause
func (qb *QueryBuilder) FullJoin(left Column, right Column) *QueryBuilder {
	qb.joins = append(qb.joins, join{
		joinType: FullJoinType,
		left:     left,
		right:    right,
	})
	qb.selectTables[left.Table().Table()] = left.Table()
	qb.selectTables[right.Table().Table()] = right.Table()

	return qb
}

// CrossJoin adds a CROSS JOIN clause
func (qb *QueryBuilder) CrossJoin(table Table) *QueryBuilder {
	qb.joins = append(qb.joins, join{
		joinType: CrossJoinType,
		table:    table,
	})
	qb.selectTables[table.Table()] = table

	return qb
}

// ================================================
// GROUP BY 和 HAVING 方法
// ================================================

// GroupBy adds GROUP BY clause with the specified columns
func (qb *QueryBuilder) GroupBy(cols ...Column) *QueryBuilder {
	qb.groupByCols = append(qb.groupByCols, cols...)

	// 添加表到 selectTables
	for _, col := range cols {
		qb.selectTables[col.Table().Table()] = col.Table()
	}

	return qb
}

// Having adds HAVING clause with the specified conditions
func (qb *QueryBuilder) Having(conds ...Condition) *QueryBuilder {
	qb.havingConditions = append(qb.havingConditions, conds...)

	// 添加条件中涉及的表
	for _, cond := range conds {
		for tn, t := range cond.Tables() {
			qb.selectTables[tn] = t
		}
	}

	return qb
}

// ================================================
// 条件方法
// ================================================

// Where sets the WHERE conditions for the query
func (qb *QueryBuilder) Where(conds ...Condition) *QueryBuilder {
	clauses := make([]string, 0, len(conds))

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

// And adds additional conditions with AND logic
func (qb *QueryBuilder) And(conds ...Condition) *QueryBuilder {
	qb.conditions = append(qb.conditions, conds...)

	for _, c := range conds {
		for tn, t := range c.Tables() {
			qb.conditionTables[tn] = t
		}

		qb.conditionClauses = append(qb.conditionClauses, c.Clause())
	}

	return qb
}

// AndIf conditionally adds conditions with AND logic
func (qb *QueryBuilder) AndIf(ok bool, conds ...Condition) *QueryBuilder {
	if ok {
		return qb.And(conds...)
	}

	return qb
}

// ================================================
// 关键词搜索方法
// ================================================

// KwSearch sets the keyword search columns
func (qb *QueryBuilder) KwSearch(cols ...Column) *QueryBuilder {
	qb.kwCols = cols
	qb.kwTables = make(map[string]Table, len(cols))

	for _, col := range cols {
		qb.kwTables[col.Table().Table()] = col.Table()
	}

	return qb
}

// ================================================
// SQL 构建方法
// ================================================

// buildCntSQL builds the COUNT query SQL
func (qb *QueryBuilder) buildCntSQL() string {
	return "SELECT COUNT(1) " + qb.buildListFrom() + qb.buildListWhere()
}

// buildListSQL builds the main SELECT query SQL
func (qb *QueryBuilder) buildListSQL() string {
	return qb.buildSelect() + qb.buildListFrom() + qb.buildListWhere() + qb.buildGroupBy() + qb.buildHaving()
}

// buildKwCntSQL builds the keyword search COUNT query SQL
func (qb *QueryBuilder) buildKwCntSQL() string {
	return "SELECT COUNT(1) " + qb.buildPageFrom() + qb.buildPageWhere()
}

// buildKwListSQL builds the keyword search SELECT query SQL
func (qb *QueryBuilder) buildKwListSQL() string {
	return qb.buildSelect() + qb.buildPageFrom() + qb.buildPageWhere() + qb.buildGroupBy() + qb.buildHaving()
}

// buildSelect builds the SELECT clause
func (qb *QueryBuilder) buildSelect() string {
	return "SELECT " + strings.Join(qb.selectColFullnames, ", ")
}

// buildGroupBy builds the GROUP BY clause
func (qb *QueryBuilder) buildGroupBy() string {
	if len(qb.groupByCols) == 0 {
		return ""
	}

	groupByExprs := make([]string, 0, len(qb.groupByCols))
	for _, col := range qb.groupByCols {
		groupByExprs = append(groupByExprs, col.QualifiedName())
	}

	return " GROUP BY " + strings.Join(groupByExprs, ", ")
}

// buildHaving builds the HAVING clause
func (qb *QueryBuilder) buildHaving() string {
	if len(qb.havingConditions) == 0 {
		return ""
	}

	havingClauses := make([]string, 0, len(qb.havingConditions))
	for _, cond := range qb.havingConditions {
		havingClauses = append(havingClauses, cond.Clause())
	}

	if len(havingClauses) == 1 {
		return " HAVING " + havingClauses[0]
	}

	return " HAVING (" + strings.Join(havingClauses, " AND ") + ")"
}

// buildListWhere builds the WHERE clause for list queries
func (qb *QueryBuilder) buildListWhere() string {
	if len(qb.conditionClauses) == 0 {
		return ""
	}

	if len(qb.conditionClauses) == 1 {
		return " WHERE " + qb.conditionClauses[0]
	}

	return " WHERE (" + strings.Join(qb.conditionClauses, " AND ") + ")"
}

// buildPageWhere builds the WHERE clause for page queries (with keyword search)
func (qb *QueryBuilder) buildPageWhere() string {
	clauses := make([]string, 0, len(qb.conditionClauses)+1)

	// Add existing conditions
	clauses = append(clauses, qb.conditionClauses...)

	// Add keyword search condition if keyword columns are defined
	if len(qb.kwCols) > 0 {
		kwClauses := make([]string, 0, len(qb.kwCols))
		for _, col := range qb.kwCols {
			kwClauses = append(kwClauses, col.QualifiedName()+" LIKE ?")
		}

		if len(kwClauses) > 0 {
			clauses = append(clauses, "("+strings.Join(kwClauses, " OR ")+")")
		}
	}

	if len(clauses) == 0 {
		return ""
	}

	if len(clauses) == 1 {
		return " WHERE " + clauses[0]
	}

	return " WHERE (" + strings.Join(clauses, " AND ") + ")"
}

// buildJoinFrom builds the FROM clause with JOINs
func (qb *QueryBuilder) buildJoinFrom() string {
	if len(qb.joins) == 0 {
		return ""
	}

	var fromBuilder strings.Builder

	// Start with the first table
	firstJoin := qb.joins[0]
	if firstJoin.joinType == CrossJoinType {
		fromBuilder.WriteString(" FROM `")
		fromBuilder.WriteString(firstJoin.table.Table())
		fromBuilder.WriteString("`")
	} else {
		fromBuilder.WriteString(" FROM `")
		fromBuilder.WriteString(firstJoin.left.Table().Table())
		fromBuilder.WriteString("`")
	}

	// Add all joins
	for _, j := range qb.joins {
		fromBuilder.WriteString(" ")
		fromBuilder.WriteString(string(j.joinType))

		if j.joinType == CrossJoinType {
			fromBuilder.WriteString(" `")
			fromBuilder.WriteString(j.table.Table())
			fromBuilder.WriteString("`")
		} else {
			fromBuilder.WriteString(" `")
			fromBuilder.WriteString(j.right.Table().Table())
			fromBuilder.WriteString("` ON ")
			fromBuilder.WriteString(j.left.QualifiedName())
			fromBuilder.WriteString(" = ")
			fromBuilder.WriteString(j.right.QualifiedName())
		}
	}

	return fromBuilder.String()
}

// buildListFrom builds the FROM clause for list queries
func (qb *QueryBuilder) buildListFrom() string {
	if len(qb.joins) > 0 {
		return qb.buildJoinFrom()
	}

	tables := qb.listQueryTables()
	if len(tables) == 0 {
		return ""
	}

	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		tableNames = append(tableNames, "`"+name+"`")
	}

	return " FROM " + strings.Join(tableNames, ", ")
}

// buildPageFrom builds the FROM clause for page queries (with keyword search)
func (qb *QueryBuilder) buildPageFrom() string {
	if len(qb.joins) > 0 {
		return qb.buildJoinFrom()
	}

	tables := qb.pageQueryTables()
	if len(tables) == 0 {
		return ""
	}

	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		tableNames = append(tableNames, "`"+name+"`")
	}

	return " FROM " + strings.Join(tableNames, ", ")
}

// listQueryTables returns all tables involved in list queries
func (qb *QueryBuilder) listQueryTables() map[string]Table {
	tables := make(map[string]Table)

	// Add select tables
	for name, table := range qb.selectTables {
		tables[name] = table
	}

	// Add condition tables
	for name, table := range qb.conditionTables {
		tables[name] = table
	}

	return tables
}

// pageQueryTables returns all tables involved in page queries (including keyword search)
func (qb *QueryBuilder) pageQueryTables() map[string]Table {
	tables := qb.listQueryTables()

	// Add keyword search tables
	for name, table := range qb.kwTables {
		tables[name] = table
	}

	return tables
}
