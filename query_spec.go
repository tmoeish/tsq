package tsq

import (
	"slices"
	"sort"
	"strings"

	"github.com/juju/errors"
)

// QuerySpec is the single source of truth for a query definition before planning.
type QuerySpec struct {
	Selects       []Column
	Filters       []Condition
	KeywordSearch []Column
	Joins         []join
	GroupBy       []Column
	Having        []Condition
}

type queryPlan struct {
	cntSQL     string
	listSQL    string
	kwCntSQL   string
	kwListSQL  string
	cntArgs    []any
	listArgs   []any
	kwCntArgs  []any
	kwListArgs []any
}

func buildQueryPlan(spec QuerySpec, allowCartesianProduct bool) (*queryPlan, error) {
	if len(spec.Selects) == 0 {
		return nil, errors.Errorf("empty select fields: %+v", spec)
	}

	for _, item := range spec.Joins {
		if item.joinType == FullJoinType {
			return nil, errors.New("FULL JOIN is not supported by TSQ's built-in dialects")
		}
	}

	if err := spec.validateJoinGraph(allowCartesianProduct); err != nil {
		return nil, errors.Trace(err)
	}

	cntSQL, cntArgs := spec.buildCntSQL()
	listSQL, listArgs := spec.buildListSQL()
	kwCntSQL, kwCntArgs := spec.buildKwCntSQL()
	kwListSQL, kwListArgs := spec.buildKwListSQL()

	return &queryPlan{
		cntSQL:     cntSQL,
		listSQL:    listSQL,
		kwCntSQL:   kwCntSQL,
		kwListSQL:  kwListSQL,
		cntArgs:    slices.Clone(cntArgs),
		listArgs:   slices.Clone(listArgs),
		kwCntArgs:  slices.Clone(kwCntArgs),
		kwListArgs: slices.Clone(kwListArgs),
	}, nil
}

func (spec QuerySpec) selectTables() map[string]Table {
	return spec.tablesForColumns(spec.Selects)
}

func (spec QuerySpec) conditionTables() map[string]Table {
	return spec.tablesForConditions(spec.Filters)
}

func (spec QuerySpec) joinTables() map[string]Table {
	tables := make(map[string]Table, len(spec.Joins)*2)
	for _, item := range spec.Joins {
		if item.joinType == CrossJoinType {
			if !isNilValue(item.table) {
				tables[item.table.Table()] = item.table
			}

			continue
		}

		if table, err := validateColumnInput(item.left); err == nil {
			tables[table.Table()] = table
		}

		if table, err := validateColumnInput(item.right); err == nil {
			tables[table.Table()] = table
		}
	}

	return tables
}

func (spec QuerySpec) keywordTables() map[string]Table {
	return spec.tablesForColumns(spec.KeywordSearch)
}

func (spec QuerySpec) listQueryTables() map[string]Table {
	tables := spec.selectTables()
	for name, table := range spec.conditionTables() {
		tables[name] = table
	}
	for name, table := range spec.joinTables() {
		tables[name] = table
	}
	for name, table := range spec.tablesForColumns(spec.GroupBy) {
		tables[name] = table
	}
	for name, table := range spec.tablesForConditions(spec.Having) {
		tables[name] = table
	}

	return tables
}

func (spec QuerySpec) pageQueryTables() map[string]Table {
	tables := spec.listQueryTables()
	for name, table := range spec.keywordTables() {
		tables[name] = table
	}

	return tables
}

func (spec QuerySpec) tablesForColumns(cols []Column) map[string]Table {
	tables := make(map[string]Table, len(cols))
	for _, col := range cols {
		table, err := validateColumnInput(col)
		if err != nil {
			continue
		}

		tables[table.Table()] = table
	}

	return tables
}

func (spec QuerySpec) tablesForConditions(conds []Condition) map[string]Table {
	tables := make(map[string]Table)
	for _, cond := range conds {
		_, condTables, _, err := validateConditionInput(cond)
		if err != nil {
			continue
		}

		for name, table := range condTables {
			tables[name] = table
		}
	}

	return tables
}

func (spec QuerySpec) buildCntSQL() (string, []any) {
	if spec.requiresWrappedCount() {
		listSQL, listArgs := spec.buildListSQL()
		return spec.wrapCountSQL(listSQL), listArgs
	}

	whereSQL, whereArgs := spec.buildListWhere()

	return "SELECT COUNT(1) " + spec.buildListFrom() + whereSQL, append([]any(nil), whereArgs...)
}

func (spec QuerySpec) buildListSQL() (string, []any) {
	selectSQL, selectArgs := spec.buildSelect()
	whereSQL, whereArgs := spec.buildListWhere()
	groupBySQL, groupByArgs := spec.buildGroupBy()
	havingSQL, havingArgs := spec.buildHaving()

	args := slices.Clone(selectArgs)
	args = append(args, whereArgs...)
	args = append(args, groupByArgs...)
	args = append(args, havingArgs...)

	return selectSQL + spec.buildListFrom() + whereSQL + groupBySQL + havingSQL, args
}

func (spec QuerySpec) buildKwCntSQL() (string, []any) {
	if spec.requiresWrappedCount() {
		listSQL, listArgs := spec.buildKwListSQL()
		return spec.wrapCountSQL(listSQL), listArgs
	}

	whereSQL, whereArgs := spec.buildPageWhere()

	return "SELECT COUNT(1) " + spec.buildPageFrom() + whereSQL, whereArgs
}

func (spec QuerySpec) buildKwListSQL() (string, []any) {
	selectSQL, selectArgs := spec.buildSelect()
	whereSQL, whereArgs := spec.buildPageWhere()
	groupBySQL, groupByArgs := spec.buildGroupBy()
	havingSQL, havingArgs := spec.buildHaving()

	args := slices.Clone(selectArgs)
	args = append(args, whereArgs...)
	args = append(args, groupByArgs...)
	args = append(args, havingArgs...)

	return selectSQL + spec.buildPageFrom() + whereSQL + groupBySQL + havingSQL, args
}

func (spec QuerySpec) buildSelect() (string, []any) {
	args := make([]any, 0, len(spec.Selects))
	fullNames := make([]string, 0, len(spec.Selects))

	for _, col := range spec.Selects {
		fullNames = append(fullNames, rawColumnQualifiedName(col))
		args = append(args, expressionArgs(col)...)
	}

	return "SELECT " + strings.Join(fullNames, ", "), args
}

func (spec QuerySpec) buildGroupBy() (string, []any) {
	if len(spec.GroupBy) == 0 {
		return "", nil
	}

	groupByExprs := make([]string, 0, len(spec.GroupBy))

	var args []any
	for _, col := range spec.GroupBy {
		groupByExprs = append(groupByExprs, rawColumnQualifiedName(col))
		args = append(args, expressionArgs(col)...)
	}

	return " GROUP BY " + strings.Join(groupByExprs, ", "), args
}

func (spec QuerySpec) buildHaving() (string, []any) {
	if len(spec.Having) == 0 {
		return "", nil
	}

	clauses := make([]string, 0, len(spec.Having))

	var args []any
	for _, cond := range spec.Having {
		clauses = append(clauses, conditionClause(cond))
		args = append(args, cond.Args()...)
	}

	if len(clauses) == 1 {
		return " HAVING " + clauses[0], args
	}

	return " HAVING (" + strings.Join(clauses, " AND ") + ")", args
}

func (spec QuerySpec) buildListWhere() (string, []any) {
	if len(spec.Filters) == 0 {
		return "", nil
	}

	clauses := make([]string, 0, len(spec.Filters))
	for _, cond := range spec.Filters {
		clauses = append(clauses, conditionClause(cond))
	}

	args := collectConditionArgs(spec.Filters...)
	if len(clauses) == 1 {
		return " WHERE " + clauses[0], args
	}

	return " WHERE (" + strings.Join(clauses, " AND ") + ")", args
}

func (spec QuerySpec) buildPageWhere() (string, []any) {
	clauses := make([]string, 0, len(spec.Filters)+1)
	for _, cond := range spec.Filters {
		clauses = append(clauses, conditionClause(cond))
	}

	args := collectConditionArgs(spec.Filters...)

	if len(spec.KeywordSearch) > 0 {
		kwClauses := make([]string, 0, len(spec.KeywordSearch))
		for _, col := range spec.KeywordSearch {
			kwClauses = append(kwClauses, rawColumnQualifiedName(col)+" LIKE ?")
		}

		if len(kwClauses) > 0 {
			clauses = append(clauses, "("+strings.Join(kwClauses, " OR ")+")")
		}
	}

	if len(clauses) == 0 {
		return "", args
	}

	if len(clauses) == 1 {
		return " WHERE " + clauses[0], args
	}

	return " WHERE (" + strings.Join(clauses, " AND ") + ")", args
}

func (spec QuerySpec) buildJoinFrom(allTables map[string]Table) string {
	if len(spec.Joins) == 0 {
		return ""
	}

	var fromBuilder strings.Builder
	includedTables := make(map[string]bool)

	firstJoin := spec.Joins[0]

	var baseTable Table
	if firstJoin.joinType == CrossJoinType {
		baseTable = spec.crossJoinBaseTable(firstJoin.table.Table(), allTables)
	} else {
		baseTable = firstJoin.left.Table()
	}

	fromBuilder.WriteString(" FROM ")
	fromBuilder.WriteString(rawTableIdentifier(baseTable))
	includedTables[baseTable.Table()] = true

	for _, item := range spec.Joins {
		if item.joinType == CrossJoinType {
			if includedTables[item.table.Table()] {
				continue
			}

			fromBuilder.WriteString(" ")
			fromBuilder.WriteString(string(item.joinType))
			fromBuilder.WriteString(" ")
			fromBuilder.WriteString(rawTableIdentifier(item.table))
			includedTables[item.table.Table()] = true

			continue
		}

		rightTable := item.right.Table().Table()
		if includedTables[rightTable] {
			continue
		}

		fromBuilder.WriteString(" ")
		fromBuilder.WriteString(string(item.joinType))
		fromBuilder.WriteString(" ")
		fromBuilder.WriteString(rawTableIdentifier(item.right.Table()))
		fromBuilder.WriteString(" ON ")
		fromBuilder.WriteString(rawColumnQualifiedName(item.left))
		fromBuilder.WriteString(" = ")
		fromBuilder.WriteString(rawColumnQualifiedName(item.right))

		includedTables[item.left.Table().Table()] = true
		includedTables[rightTable] = true
	}

	return fromBuilder.String()
}

func (spec QuerySpec) buildListFrom() string {
	tables := spec.listQueryTables()
	if len(spec.Joins) > 0 {
		return spec.buildJoinFrom(tables)
	}

	if len(tables) == 0 {
		return ""
	}

	tableNames := make([]string, 0, len(tables))
	for _, table := range tables {
		tableNames = append(tableNames, rawTableIdentifier(table))
	}

	sort.Strings(tableNames)

	return " FROM " + strings.Join(tableNames, ", ")
}

func (spec QuerySpec) buildPageFrom() string {
	tables := spec.pageQueryTables()
	if len(spec.Joins) > 0 {
		return spec.buildJoinFrom(tables)
	}

	if len(tables) == 0 {
		return ""
	}

	tableNames := make([]string, 0, len(tables))
	for _, table := range tables {
		tableNames = append(tableNames, rawTableIdentifier(table))
	}

	sort.Strings(tableNames)

	return " FROM " + strings.Join(tableNames, ", ")
}

func (spec QuerySpec) requiresWrappedCount() bool {
	return len(spec.GroupBy) > 0 ||
		len(spec.Having) > 0 ||
		spec.hasDistinctSelect() ||
		spec.hasAggregateSelect()
}

func (spec QuerySpec) wrapCountSQL(inner string) string {
	return "SELECT COUNT(1) FROM (" + inner + ") AS _tsq_cnt"
}

func (spec QuerySpec) hasDistinctSelect() bool {
	type distinctExpr interface {
		isDistinctExpression() bool
	}

	for _, col := range spec.Selects {
		if expr, ok := col.(distinctExpr); ok && expr.isDistinctExpression() {
			return true
		}
	}

	return false
}

func (spec QuerySpec) hasAggregateSelect() bool {
	type aggregateExpr interface {
		isAggregateExpression() bool
	}

	for _, col := range spec.Selects {
		if expr, ok := col.(aggregateExpr); ok && expr.isAggregateExpression() {
			return true
		}
	}

	return false
}

func (spec QuerySpec) crossJoinBaseTable(joinTable string, allTables map[string]Table) Table {
	for _, col := range spec.Selects {
		if table := col.Table(); table.Table() != joinTable {
			return table
		}
	}

	tableNames := make([]string, 0, len(allTables))
	for name := range allTables {
		if name != joinTable {
			tableNames = append(tableNames, name)
		}
	}

	sort.Strings(tableNames)
	if len(tableNames) > 0 {
		return allTables[tableNames[0]]
	}

	return allTables[joinTable]
}

func (spec QuerySpec) validateJoinGraph(allowCartesianProduct bool) error {
	if len(spec.Joins) == 0 {
		if !allowCartesianProduct && len(spec.pageQueryTables()) > 1 {
			return errors.New("multiple tables require explicit Join/CrossJoin or AllowCartesianProduct")
		}

		return nil
	}

	allTables := spec.pageQueryTables()
	introduced := make(map[string]struct{}, len(spec.Joins)+1)

	firstJoin := spec.Joins[0]
	if firstJoin.joinType == CrossJoinType {
		baseTable := spec.crossJoinBaseTable(firstJoin.table.Table(), allTables)
		if baseTable != nil {
			introduced[baseTable.Table()] = struct{}{}
		}
	} else {
		introduced[firstJoin.left.Table().Table()] = struct{}{}
	}

	for _, item := range spec.Joins {
		switch item.joinType {
		case CrossJoinType:
			tableName := item.table.Table()
			if _, exists := introduced[tableName]; exists {
				return errors.Errorf("table %s is already present in join graph", tableName)
			}

			introduced[tableName] = struct{}{}
		default:
			leftTable := item.left.Table().Table()
			rightTable := item.right.Table().Table()

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
