package tsq

import (
	"sort"
	"strings"

	"github.com/juju/errors"
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

	buildErr error
}

func newQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		selectCols:         make([]Column, 0),
		selectColFullnames: make([]string, 0),
		selectTables:       make(map[string]Table),
		conditionTables:    make(map[string]Table),
		kwTables:           make(map[string]Table),
		joins:              make([]join, 0),
		groupByCols:        make([]Column, 0),
		havingConditions:   make([]Condition, 0),
	}
}

func (qb *QueryBuilder) ensureInitialized() *QueryBuilder {
	if qb == nil {
		qb = newQueryBuilder()
		qb.buildErr = errors.New("query builder cannot be nil")
		return qb
	}

	if qb.selectTables == nil {
		qb.selectTables = make(map[string]Table)
	}

	if qb.conditionTables == nil {
		qb.conditionTables = make(map[string]Table)
	}

	if qb.kwTables == nil {
		qb.kwTables = make(map[string]Table)
	}

	if qb.selectCols == nil {
		qb.selectCols = make([]Column, 0)
	}

	if qb.selectColFullnames == nil {
		qb.selectColFullnames = make([]string, 0)
	}

	if qb.joins == nil {
		qb.joins = make([]join, 0)
	}

	if qb.groupByCols == nil {
		qb.groupByCols = make([]Column, 0)
	}

	if qb.havingConditions == nil {
		qb.havingConditions = make([]Condition, 0)
	}

	return qb
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
	qb := newQueryBuilder()
	qb.selectCols = make([]Column, 0, len(cols))
	qb.selectColFullnames = make([]string, 0, len(cols))
	qb.selectTables = make(map[string]Table, len(cols))

	qb.addSelectColumns(cols...)

	return qb
}

func (qb *QueryBuilder) setBuildError(err error) {
	if qb == nil || err == nil || qb.buildErr != nil {
		return
	}

	qb.buildErr = err
}

func (qb *QueryBuilder) addSelectColumns(cols ...Column) {
	for _, col := range cols {
		table, err := validateColumnInput(col)
		if err != nil {
			qb.setBuildError(err)
			continue
		}

		qb.selectCols = append(qb.selectCols, col)
		qb.selectColFullnames = append(qb.selectColFullnames, rawColumnQualifiedName(col))
		qb.selectTables[table.Table()] = table
	}
}

func (qb *QueryBuilder) addQueryColumn(cols *[]Column, tables map[string]Table, col Column) {
	table, err := validateColumnInput(col)
	if err != nil {
		qb.setBuildError(err)
		return
	}

	*cols = append(*cols, col)
	tables[table.Table()] = table
}

func (qb *QueryBuilder) addCondition(target *[]Condition, clauses *[]string, tables map[string]Table, cond Condition) {
	clause, condTables, err := validateConditionInput(cond)
	if err != nil {
		qb.setBuildError(err)
		return
	}

	*target = append(*target, cond)
	if clauses != nil {
		*clauses = append(*clauses, clause)
	}
	for tn, t := range condTables {
		tables[tn] = t
	}
}

// ================================================
// JOIN 方法 - 增强版
// ================================================

// LeftJoin adds a LEFT JOIN clause. Equivalent to `FROM left.Table LEFT JOIN right.Table ON left=right`.
func (qb *QueryBuilder) LeftJoin(left Column, right Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	leftTable, err := validateColumnInput(left)
	if err != nil {
		qb.setBuildError(err)
		return qb
	}

	rightTable, err := validateColumnInput(right)
	if err != nil {
		qb.setBuildError(err)
		return qb
	}

	qb.joins = append(qb.joins, join{
		joinType: LeftJoinType,
		left:     left,
		right:    right,
	})
	qb.selectTables[leftTable.Table()] = leftTable
	qb.selectTables[rightTable.Table()] = rightTable

	return qb
}

// InnerJoin adds an INNER JOIN clause
func (qb *QueryBuilder) InnerJoin(left Column, right Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	leftTable, err := validateColumnInput(left)
	if err != nil {
		qb.setBuildError(err)
		return qb
	}

	rightTable, err := validateColumnInput(right)
	if err != nil {
		qb.setBuildError(err)
		return qb
	}

	qb.joins = append(qb.joins, join{
		joinType: InnerJoinType,
		left:     left,
		right:    right,
	})
	qb.selectTables[leftTable.Table()] = leftTable
	qb.selectTables[rightTable.Table()] = rightTable

	return qb
}

// RightJoin adds a RIGHT JOIN clause
func (qb *QueryBuilder) RightJoin(left Column, right Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	leftTable, err := validateColumnInput(left)
	if err != nil {
		qb.setBuildError(err)
		return qb
	}

	rightTable, err := validateColumnInput(right)
	if err != nil {
		qb.setBuildError(err)
		return qb
	}

	qb.joins = append(qb.joins, join{
		joinType: RightJoinType,
		left:     left,
		right:    right,
	})
	qb.selectTables[leftTable.Table()] = leftTable
	qb.selectTables[rightTable.Table()] = rightTable

	return qb
}

// FullJoin adds a FULL JOIN clause
func (qb *QueryBuilder) FullJoin(left Column, right Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	leftTable, err := validateColumnInput(left)
	if err != nil {
		qb.setBuildError(err)
		return qb
	}

	rightTable, err := validateColumnInput(right)
	if err != nil {
		qb.setBuildError(err)
		return qb
	}

	qb.joins = append(qb.joins, join{
		joinType: FullJoinType,
		left:     left,
		right:    right,
	})
	qb.selectTables[leftTable.Table()] = leftTable
	qb.selectTables[rightTable.Table()] = rightTable

	return qb
}

// CrossJoin adds a CROSS JOIN clause
func (qb *QueryBuilder) CrossJoin(table Table) *QueryBuilder {
	qb = qb.ensureInitialized()

	if isNilValue(table) {
		qb.setBuildError(errors.New("cross join table cannot be nil"))
		return qb
	}

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
	qb = qb.ensureInitialized()

	for _, col := range cols {
		qb.addQueryColumn(&qb.groupByCols, qb.selectTables, col)
	}

	return qb
}

// Having adds HAVING clause with the specified conditions
func (qb *QueryBuilder) Having(conds ...Condition) *QueryBuilder {
	qb = qb.ensureInitialized()

	for _, cond := range conds {
		qb.addCondition(&qb.havingConditions, nil, qb.selectTables, cond)
	}

	return qb
}

// ================================================
// 条件方法
// ================================================

// Where sets the WHERE conditions for the query
func (qb *QueryBuilder) Where(conds ...Condition) *QueryBuilder {
	qb = qb.ensureInitialized()

	clauses := make([]string, 0, len(conds))
	conditionTables := make(map[string]Table, len(conds))
	conditions := make([]Condition, 0, len(conds))

	for _, c := range conds {
		qb.addCondition(&conditions, &clauses, conditionTables, c)
	}

	qb.conditions = conditions
	qb.conditionClauses = clauses
	qb.conditionTables = conditionTables

	return qb
}

// And adds additional conditions with AND logic
func (qb *QueryBuilder) And(conds ...Condition) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.conditionTables == nil {
		qb.conditionTables = make(map[string]Table)
	}

	for _, c := range conds {
		qb.addCondition(&qb.conditions, &qb.conditionClauses, qb.conditionTables, c)
	}

	return qb
}

// AndIf conditionally adds conditions with AND logic
func (qb *QueryBuilder) AndIf(ok bool, conds ...Condition) *QueryBuilder {
	qb = qb.ensureInitialized()

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
	qb = qb.ensureInitialized()

	qb.kwCols = make([]Column, 0, len(cols))
	qb.kwTables = make(map[string]Table, len(cols))

	for _, col := range cols {
		qb.addQueryColumn(&qb.kwCols, qb.kwTables, col)
	}

	return qb
}

// ================================================
// SQL 构建方法
// ================================================

// buildCntSQL builds the COUNT query SQL
func (qb *QueryBuilder) buildCntSQL() string {
	if qb.requiresWrappedCount() {
		return qb.wrapCountSQL(qb.buildListSQL())
	}

	return "SELECT COUNT(1) " + qb.buildListFrom() + qb.buildListWhere()
}

// buildListSQL builds the main SELECT query SQL
func (qb *QueryBuilder) buildListSQL() string {
	return qb.buildSelect() + qb.buildListFrom() + qb.buildListWhere() + qb.buildGroupBy() + qb.buildHaving()
}

// buildKwCntSQL builds the keyword search COUNT query SQL
func (qb *QueryBuilder) buildKwCntSQL() string {
	if qb.requiresWrappedCount() {
		return qb.wrapCountSQL(qb.buildKwListSQL())
	}

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
		groupByExprs = append(groupByExprs, rawColumnQualifiedName(col))
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
		havingClauses = append(havingClauses, conditionClause(cond))
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
			kwClauses = append(kwClauses, rawColumnQualifiedName(col)+" LIKE ?")
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
func (qb *QueryBuilder) buildJoinFrom(allTables map[string]Table) string {
	if len(qb.joins) == 0 {
		return ""
	}

	var fromBuilder strings.Builder
	includedTables := make(map[string]bool)

	firstJoin := qb.joins[0]
	baseTable := ""

	if firstJoin.joinType == CrossJoinType {
		baseTable = qb.crossJoinBaseTable(firstJoin.table.Table(), allTables)
	} else {
		baseTable = firstJoin.left.Table().Table()
	}

	fromBuilder.WriteString(" FROM ")
	fromBuilder.WriteString(rawIdentifier(baseTable))
	includedTables[baseTable] = true

	for _, j := range qb.joins {
		if j.joinType == CrossJoinType {
			if includedTables[j.table.Table()] {
				continue
			}
			fromBuilder.WriteString(" ")
			fromBuilder.WriteString(string(j.joinType))
			fromBuilder.WriteString(" ")
			fromBuilder.WriteString(rawIdentifier(j.table.Table()))
			includedTables[j.table.Table()] = true
		} else {
			if includedTables[j.right.Table().Table()] {
				continue
			}
			fromBuilder.WriteString(" ")
			fromBuilder.WriteString(string(j.joinType))
			fromBuilder.WriteString(" ")
			fromBuilder.WriteString(rawIdentifier(j.right.Table().Table()))
			fromBuilder.WriteString(" ON ")
			fromBuilder.WriteString(rawColumnQualifiedName(j.left))
			fromBuilder.WriteString(" = ")
			fromBuilder.WriteString(rawColumnQualifiedName(j.right))
			includedTables[j.left.Table().Table()] = true
			includedTables[j.right.Table().Table()] = true
		}
	}

	return fromBuilder.String()
}

// buildListFrom builds the FROM clause for list queries
func (qb *QueryBuilder) buildListFrom() string {
	tables := qb.listQueryTables()
	if len(qb.joins) > 0 {
		return qb.buildJoinFrom(tables)
	}

	if len(tables) == 0 {
		return ""
	}

	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		tableNames = append(tableNames, rawIdentifier(name))
	}
	sort.Strings(tableNames)

	return " FROM " + strings.Join(tableNames, ", ")
}

// buildPageFrom builds the FROM clause for page queries (with keyword search)
func (qb *QueryBuilder) buildPageFrom() string {
	tables := qb.pageQueryTables()
	if len(qb.joins) > 0 {
		return qb.buildJoinFrom(tables)
	}

	if len(tables) == 0 {
		return ""
	}

	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		tableNames = append(tableNames, rawIdentifier(name))
	}
	sort.Strings(tableNames)

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

func (qb *QueryBuilder) requiresWrappedCount() bool {
	return len(qb.groupByCols) > 0 || len(qb.havingConditions) > 0
}

func (qb *QueryBuilder) wrapCountSQL(inner string) string {
	return "SELECT COUNT(1) FROM (" + inner + ") AS _tsq_cnt"
}

func (qb *QueryBuilder) crossJoinBaseTable(joinTable string, allTables map[string]Table) string {
	for _, col := range qb.selectCols {
		tableName := col.Table().Table()
		if tableName != joinTable {
			return tableName
		}
	}

	tableNames := make([]string, 0, len(allTables))
	for name := range allTables {
		if name == joinTable {
			continue
		}

		tableNames = append(tableNames, name)
	}

	sort.Strings(tableNames)
	if len(tableNames) > 0 {
		return tableNames[0]
	}

	return joinTable
}

func (qb *QueryBuilder) validateJoinGraph() error {
	if len(qb.joins) == 0 {
		return nil
	}

	allTables := qb.pageQueryTables()
	introduced := make(map[string]struct{}, len(qb.joins)+1)

	firstJoin := qb.joins[0]
	if firstJoin.joinType == CrossJoinType {
		baseTable := qb.crossJoinBaseTable(firstJoin.table.Table(), allTables)
		introduced[baseTable] = struct{}{}
	} else {
		introduced[firstJoin.left.Table().Table()] = struct{}{}
	}

	for _, j := range qb.joins {
		switch j.joinType {
		case CrossJoinType:
			tableName := j.table.Table()
			if _, exists := introduced[tableName]; exists {
				return errors.Errorf("table %s is already present in join graph", tableName)
			}

			introduced[tableName] = struct{}{}
		default:
			leftTable := j.left.Table().Table()
			rightTable := j.right.Table().Table()

			if _, exists := introduced[leftTable]; !exists {
				return errors.Errorf("join left table %s is not connected to the current FROM/JOIN graph", leftTable)
			}

			if _, exists := introduced[rightTable]; exists {
				return errors.Errorf("join right table %s is already present; aliases are required for repeated joins", rightTable)
			}

			introduced[rightTable] = struct{}{}
		}
	}

	for tableName := range allTables {
		if _, exists := introduced[tableName]; exists {
			continue
		}

		return errors.Errorf(
			"table %s is referenced outside the join graph; use CrossJoin to include it explicitly",
			tableName,
		)
	}

	return nil
}
