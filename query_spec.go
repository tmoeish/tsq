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
	From          Table
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

func buildQueryPlan(spec QuerySpec) (*queryPlan, error) {
	if len(spec.Selects) == 0 {
		return nil, errors.Errorf("empty select fields: %+v", spec)
	}

	if err := spec.validateJoinGraph(); err != nil {
		return nil, errors.Trace(err)
	}

	if err := spec.validateSetOperations(); err != nil {
		return nil, errors.Trace(err)
	}

	cntSQL, cntArgs, err := spec.buildCntSQL()
	if err != nil {
		return nil, errors.Trace(err)
	}

	listSQL, listArgs, err := spec.buildListSQL()
	if err != nil {
		return nil, errors.Trace(err)
	}

	kwCntSQL, kwCntArgs, err := spec.buildKwCntSQL()
	if err != nil {
		return nil, errors.Trace(err)
	}

	kwListSQL, kwListArgs, err := spec.buildKwListSQL()
	if err != nil {
		return nil, errors.Trace(err)
	}

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

func (spec QuerySpec) fromTables() map[string]Table {
	if isNilValue(spec.From) {
		return map[string]Table{}
	}

	return map[string]Table{spec.From.Table(): spec.From}
}

func (spec QuerySpec) conditionTables() map[string]Table {
	return spec.tablesForConditions(spec.Filters)
}

func (spec QuerySpec) joinTables() map[string]Table {
	tables := make(map[string]Table, len(spec.Joins)*2)

	for _, item := range spec.Joins {
		if !isNilValue(item.table) {
			tables[item.table.Table()] = item.table
		}

		maps.Copy(tables, spec.tablesForConditions(item.on))
	}

	return tables
}

func (spec QuerySpec) keywordTables() map[string]Table {
	return spec.tablesForColumns(spec.KeywordSearch)
}

func (spec QuerySpec) listQueryTables() map[string]Table {
	tables := spec.fromTables()
	maps.Copy(tables, spec.selectTables())

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
		if refs, ok := col.(interface{ referencedTables() map[string]Table }); ok {
			maps.Copy(tables, refs.referencedTables())
		}
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

func (spec QuerySpec) buildCntSQL() (string, []any, error) {
	cteSQL, cteArgs, err := spec.buildCTEPrefix(false)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	if len(spec.SetOps) > 0 || spec.requiresWrappedCount() {
		listSQL, listArgs := spec.buildListBodySQL(false)
		args := append(slices.Clone(cteArgs), listArgs...)

		return cteSQL + spec.wrapCountSQL(listSQL), args, nil
	}

	fromSQL, fromArgs := spec.buildListFrom()
	whereSQL, whereArgs := spec.buildListWhere()

	args := append(slices.Clone(cteArgs), fromArgs...)
	args = append(args, whereArgs...)

	return cteSQL + "SELECT COUNT(1)" + fromSQL + whereSQL, args, nil
}

func (spec QuerySpec) buildListSQL() (string, []any, error) {
	cteSQL, cteArgs, err := spec.buildCTEPrefix(false)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	bodySQL, bodyArgs := spec.buildListBodySQL(false)
	args := append(slices.Clone(cteArgs), bodyArgs...)

	return cteSQL + bodySQL, args, nil
}

func (spec QuerySpec) buildSimpleListSQL() (string, []any) {
	selectSQL, selectArgs := spec.buildSelect()
	fromSQL, fromArgs := spec.buildListFrom()
	whereSQL, whereArgs := spec.buildListWhere()
	groupBySQL, groupByArgs := spec.buildGroupBy()
	havingSQL, havingArgs := spec.buildHaving()

	args := slices.Clone(selectArgs)
	args = append(args, fromArgs...)
	args = append(args, whereArgs...)
	args = append(args, groupByArgs...)
	args = append(args, havingArgs...)

	return selectSQL + fromSQL + whereSQL + groupBySQL + havingSQL, args
}

func (spec QuerySpec) buildKwCntSQL() (string, []any, error) {
	cteSQL, cteArgs, err := spec.buildCTEPrefix(true)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	if len(spec.SetOps) > 0 || spec.requiresWrappedCount() {
		listSQL, listArgs := spec.buildListBodySQL(true)
		args := append(slices.Clone(cteArgs), listArgs...)

		return cteSQL + spec.wrapCountSQL(listSQL), args, nil
	}

	fromSQL, fromArgs := spec.buildPageFrom()
	whereSQL, whereArgs := spec.buildPageWhere()

	args := append(slices.Clone(cteArgs), fromArgs...)
	args = append(args, whereArgs...)

	return cteSQL + "SELECT COUNT(1)" + fromSQL + whereSQL, args, nil
}

func (spec QuerySpec) buildKwListSQL() (string, []any, error) {
	cteSQL, cteArgs, err := spec.buildCTEPrefix(true)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	bodySQL, bodyArgs := spec.buildListBodySQL(true)
	args := append(slices.Clone(cteArgs), bodyArgs...)

	return cteSQL + bodySQL, args, nil
}

func (spec QuerySpec) buildSimpleKwListSQL() (string, []any) {
	selectSQL, selectArgs := spec.buildSelect()
	fromSQL, fromArgs := spec.buildPageFrom()
	whereSQL, whereArgs := spec.buildPageWhere()
	groupBySQL, groupByArgs := spec.buildGroupBy()
	havingSQL, havingArgs := spec.buildHaving()

	args := slices.Clone(selectArgs)
	args = append(args, fromArgs...)
	args = append(args, whereArgs...)
	args = append(args, groupByArgs...)
	args = append(args, havingArgs...)

	return selectSQL + fromSQL + whereSQL + groupBySQL + havingSQL, args
}

func (spec QuerySpec) buildListBodySQL(useKeyword bool) (string, []any) {
	if len(spec.SetOps) > 0 {
		return spec.buildCompoundListSQL(useKeyword)
	}

	return spec.buildSimpleCompoundOperandSQL(useKeyword)
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
		sql, args := spec.buildListBodySQL(useKeyword)
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

func buildConditionSQL(prefix string, conds []Condition) (string, []any) {
	clauses := make([]string, 0, len(conds))
	for _, cond := range conds {
		clauses = append(clauses, conditionClause(cond))
	}

	args := collectConditionArgs(conds...)
	if len(clauses) == 1 {
		return prefix + clauses[0], args
	}

	return prefix + "(" + strings.Join(clauses, " AND ") + ")", args
}

func (spec QuerySpec) buildListWhere() (string, []any) {
	if len(spec.Filters) == 0 {
		return "", nil
	}

	return buildConditionSQL(" WHERE ", spec.Filters)
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

func (spec QuerySpec) buildFrom() (string, []any) {
	var fromBuilder strings.Builder
	args := make([]any, 0)

	includedTables := make(map[string]bool)

	fromBuilder.WriteString(" FROM ")
	fromBuilder.WriteString(rawTableIdentifier(spec.From))

	includedTables[spec.From.Table()] = true

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

		tableName := item.table.Table()
		if includedTables[tableName] {
			continue
		}

		fromBuilder.WriteString(" ")
		fromBuilder.WriteString(string(item.joinType))
		fromBuilder.WriteString(" ")
		fromBuilder.WriteString(rawTableIdentifier(item.table))

		if len(item.on) > 0 {
			onSQL, onArgs := buildConditionSQL(" ON ", item.on)
			fromBuilder.WriteString(onSQL)

			args = append(args, onArgs...)
		}

		includedTables[tableName] = true
	}

	return fromBuilder.String(), args
}

func (spec QuerySpec) buildListFrom() (string, []any) {
	return spec.buildFrom()
}

func (spec QuerySpec) buildPageFrom() (string, []any) {
	return spec.buildFrom()
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

func (spec QuerySpec) buildCTEPrefix(useKeyword bool) (string, []any, error) {
	defs, err := spec.collectCTEDefinitions(useKeyword)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	if len(defs) == 0 {
		return "", nil, nil
	}

	parts := make([]string, 0, len(defs))
	args := make([]any, 0)

	for _, def := range defs {
		bodySQL, bodyArgs := def.spec.buildListBodySQL(false)
		parts = append(parts, rawIdentifier(def.name)+" AS ("+bodySQL+")")
		args = append(args, bodyArgs...)
	}

	return "WITH " + strings.Join(parts, ", ") + " ", args, nil
}

func (spec QuerySpec) collectCTEDefinitions(useKeyword bool) ([]cteDefinition, error) {
	collector := &cteCollector{
		seen:     make(map[string]struct{}),
		visiting: make(map[string]struct{}),
	}

	if err := collector.collectFromSpec(spec, useKeyword); err != nil {
		return nil, errors.Trace(err)
	}

	return collector.ordered, nil
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

		if err := op.spec.validateJoinGraph(); err != nil {
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
		From:          spec.From,
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
			op:   op.op,
			spec: cloneQuerySpec(op.spec),
		})
	}

	return cloned
}

type cteCollector struct {
	ordered  []cteDefinition
	seen     map[string]struct{}
	visiting map[string]struct{}
}

func (c *cteCollector) collectFromSpec(spec QuerySpec, useKeyword bool) error {
	var tables map[string]Table
	if useKeyword {
		tables = spec.pageQueryTables()
	} else {
		tables = spec.listQueryTables()
	}

	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		tableNames = append(tableNames, name)
	}

	sort.Strings(tableNames)

	for _, name := range tableNames {
		provider, ok := tables[name].(cteProvider)
		if !ok {
			continue
		}

		if err := c.collectDefinition(provider.cteDefinition()); err != nil {
			return errors.Trace(err)
		}
	}

	for _, op := range spec.SetOps {
		if err := c.collectFromSpec(op.spec, useKeyword); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (c *cteCollector) collectDefinition(def cteDefinition) error {
	if strings.TrimSpace(def.name) == "" {
		return errors.New("cte name cannot be empty")
	}

	if _, exists := c.seen[def.name]; exists {
		return nil
	}

	if _, visiting := c.visiting[def.name]; visiting {
		return errors.Errorf("cyclic CTE dependency detected for %s", def.name)
	}

	if len(def.spec.Selects) == 0 {
		return errors.Errorf("cte %s requires at least one selected column", def.name)
	}

	if len(def.spec.KeywordSearch) > 0 {
		return errors.Errorf("cte %s does not support keyword search", def.name)
	}

	if err := def.spec.validateJoinGraph(); err != nil {
		return errors.Trace(err)
	}

	if err := def.spec.validateSetOperations(); err != nil {
		return errors.Trace(err)
	}

	c.visiting[def.name] = struct{}{}
	if err := c.collectFromSpec(def.spec, false); err != nil {
		delete(c.visiting, def.name)
		return errors.Trace(err)
	}

	delete(c.visiting, def.name)

	c.seen[def.name] = struct{}{}
	c.ordered = append(c.ordered, def)

	return nil
}

// validateJoinGraph validates that joins form a valid directed acyclic graph (DAG).
//
// This function checks:
// 1. No table appears twice in the join graph (except as aliases via AliasTable)
// 2. All non-target tables referenced by each ON condition have been previously introduced
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
//	users.InnerJoin(orders, users.ID.EQCol(orders.UserID)).
//	InnerJoin(invoices, orders.ID.EQCol(invoices.OrderID)).
//	InnerJoin(users, invoices.UserID.EQCol(users.ID))  // CIRCULAR: users already involved
//
// Example of self-join workaround (WILL work):
//
//	usersAlias := AliasTable(users, "u2")
//	users.InnerJoin(usersAlias, users.ID.EQCol(usersAlias.ParentID))
func (spec QuerySpec) validateJoinGraph() error {
	if err := validateTableInput(spec.From, "from table"); err != nil {
		return errors.Trace(err)
	}

	allTables := spec.pageQueryTables()
	introduced := make(map[string]struct{}, len(spec.Joins)+1)

	introduced[spec.From.Table()] = struct{}{}

	for _, item := range spec.Joins {
		if isNilValue(item.table) {
			return errors.New("join table cannot be nil")
		}

		switch item.joinType {
		case CrossJoinType:
			tableName := item.table.Table()
			if _, exists := introduced[tableName]; exists {
				return errors.Errorf("table %s is already present in join graph", tableName)
			}

			introduced[tableName] = struct{}{}
		default:
			tableName := item.table.Table()
			if _, exists := introduced[tableName]; exists {
				return errors.Errorf("join table %s is already present; aliases are required for repeated joins", tableName)
			}

			if len(item.on) == 0 {
				return errors.Errorf("%s %s requires at least one ON condition", item.joinType, tableName)
			}

			condTables := spec.tablesForConditions(item.on)
			if _, exists := condTables[tableName]; !exists {
				return errors.Errorf("%s %s ON conditions must reference joined table %s", item.joinType, tableName, tableName)
			}

			connectedToIntroduced := false

			for condTable := range condTables {
				if condTable == tableName {
					continue
				}

				if _, exists := introduced[condTable]; !exists {
					return errors.Errorf("join condition table %s is not connected to the current FROM/JOIN graph", condTable)
				}

				connectedToIntroduced = true
			}

			if !connectedToIntroduced {
				return errors.Errorf("%s %s ON conditions must reference at least one table already in the FROM/JOIN graph", item.joinType, tableName)
			}

			introduced[tableName] = struct{}{}
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
