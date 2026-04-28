package tsq

import (
	"maps"
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
	SetOps        []setOperation
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

	if err := spec.validateJoinGraph(allowCartesianProduct); err != nil {
		return nil, errors.Trace(err)
	}

	if err := spec.validateSetOperations(); err != nil {
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
	maps.Copy(tables, spec.conditionTables())

	maps.Copy(tables, spec.joinTables())

	maps.Copy(tables, spec.tablesForColumns(spec.GroupBy))

	maps.Copy(tables, spec.tablesForConditions(spec.Having))

	return tables
}

func (spec QuerySpec) pageQueryTables() map[string]Table {
	tables := spec.listQueryTables()
	maps.Copy(tables, spec.keywordTables())

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

		maps.Copy(tables, condTables)
	}

	return tables
}

func (spec QuerySpec) buildCntSQL() (string, []any) {
	if len(spec.SetOps) > 0 {
		listSQL, listArgs := spec.buildListSQL()
		return spec.wrapCountSQL(listSQL), listArgs
	}

	if spec.requiresWrappedCount() {
		listSQL, listArgs := spec.buildListSQL()
		return spec.wrapCountSQL(listSQL), listArgs
	}

	whereSQL, whereArgs := spec.buildListWhere()

	return "SELECT COUNT(1) " + spec.buildListFrom() + whereSQL, append([]any(nil), whereArgs...)
}

func (spec QuerySpec) buildListSQL() (string, []any) {
	if len(spec.SetOps) > 0 {
		return spec.buildCompoundListSQL(false)
	}

	return spec.buildSimpleListSQL()
}

func (spec QuerySpec) buildSimpleListSQL() (string, []any) {
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
	if len(spec.SetOps) > 0 {
		listSQL, listArgs := spec.buildKwListSQL()
		return spec.wrapCountSQL(listSQL), listArgs
	}

	if spec.requiresWrappedCount() {
		listSQL, listArgs := spec.buildKwListSQL()
		return spec.wrapCountSQL(listSQL), listArgs
	}

	whereSQL, whereArgs := spec.buildPageWhere()

	return "SELECT COUNT(1) " + spec.buildPageFrom() + whereSQL, whereArgs
}

func (spec QuerySpec) buildKwListSQL() (string, []any) {
	if len(spec.SetOps) > 0 {
		return spec.buildCompoundListSQL(true)
	}

	return spec.buildSimpleKwListSQL()
}

func (spec QuerySpec) buildSimpleKwListSQL() (string, []any) {
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

func (spec QuerySpec) buildCompoundListSQL(useKeyword bool) (string, []any) {
	baseSQL, baseArgs := spec.buildSimpleCompoundOperandSQL(useKeyword)
	args := slices.Clone(baseArgs)

	var builder strings.Builder
	builder.WriteString(baseSQL)

	for _, op := range spec.SetOps {
		rightSQL, rightArgs := op.spec.buildOperandSQL(useKeyword)

		builder.WriteByte(' ')
		builder.WriteString(string(op.op))
		builder.WriteByte(' ')
		builder.WriteString(rightSQL)

		args = append(args, rightArgs...)
	}

	return builder.String(), args
}

func (spec QuerySpec) buildOperandSQL(useKeyword bool) (string, []any) {
	if len(spec.SetOps) > 0 {
		sql, args := spec.buildCompoundListSQL(useKeyword)
		return "(" + sql + ")", args
	}

	return spec.buildSimpleCompoundOperandSQL(useKeyword)
}

func (spec QuerySpec) buildSimpleCompoundOperandSQL(useKeyword bool) (string, []any) {
	if useKeyword {
		return spec.buildSimpleKwListSQL()
	}

	return spec.buildSimpleListSQL()
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
			args = append(args, keywordArgMarker)
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
	return len(spec.SetOps) > 0 ||
		len(spec.GroupBy) > 0 ||
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

func (spec QuerySpec) validateSetOperations() error {
	if len(spec.SetOps) == 0 {
		return nil
	}

	if len(spec.KeywordSearch) > 0 {
		return errors.New("set operations do not support keyword search")
	}

	leftCount := len(spec.Selects)
	for _, op := range spec.SetOps {
		if len(op.spec.Selects) != leftCount {
			return errors.Errorf(
				"set operation %s requires matching select column counts: left=%d right=%d",
				op.op,
				leftCount,
				len(op.spec.Selects),
			)
		}

		if len(op.spec.KeywordSearch) > 0 {
			return errors.New("set operations do not support keyword search")
		}

		if err := op.spec.validateJoinGraph(op.allowCartesianProduct); err != nil {
			return errors.Trace(err)
		}

		if err := op.spec.validateSetOperations(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func cloneQuerySpec(spec QuerySpec) QuerySpec {
	cloned := QuerySpec{
		Selects:       slices.Clone(spec.Selects),
		Filters:       slices.Clone(spec.Filters),
		KeywordSearch: slices.Clone(spec.KeywordSearch),
		Joins:         slices.Clone(spec.Joins),
		GroupBy:       slices.Clone(spec.GroupBy),
		Having:        slices.Clone(spec.Having),
		SetOps:        make([]setOperation, 0, len(spec.SetOps)),
	}

	for _, op := range spec.SetOps {
		cloned.SetOps = append(cloned.SetOps, setOperation{
			op:                    op.op,
			spec:                  cloneQuerySpec(op.spec),
			allowCartesianProduct: op.allowCartesianProduct,
		})
	}

	return cloned
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

// validateJoinGraph validates that joins form a valid directed acyclic graph (DAG).
//
// This function checks:
// 1. No table appears twice in the join graph (except as aliases via AliasTable)
// 2. All left and right tables in each join have been previously introduced
//
// LIMITATION: Circular join dependencies are NOT supported.
// For example, the following circular dependency cannot be expressed:
//
//	A -> B -> C -> A
//
// This is a fundamental limitation of the current query builder design.
// Users who need circular relationships should:
//  1. Use self-joins with table aliases (via AliasTable) to simulate the pattern
//  2. Execute multiple queries instead of a single circular join
//  3. Use subqueries or CTEs (if supported by the target database)
//
// Example of circular dependency that WON'T work:
//
//	users.InnerJoin(orders, users.ID.EQ(orders.UserID)).
//	InnerJoin(invoices, orders.ID.EQ(invoices.OrderID)).
//	InnerJoin(users, invoices.UserID.EQ(users.ID))  // CIRCULAR: users already involved
//
// Example of self-join workaround (WILL work):
//
//	usersAlias := AliasTable(users, "u2")
//	users.InnerJoin(usersAlias, users.ID.EQ(usersAlias.ParentID))
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
