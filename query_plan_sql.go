package tsq

import (
	"slices"
	"strconv"
	"strings"
)

func (spec querySpec[O]) buildCntSQL() (string, []any, error) {
	return spec.buildCountSQL(false)
}

func (spec querySpec[O]) buildListSQL() (string, []any, error) {
	cteSQL, cteArgs, err := spec.buildCTEPrefix(false)
	if err != nil {
		return "", nil, err
	}

	bodySQL, bodyArgs := spec.buildListBodySQL(false)
	args := append(slices.Clone(cteArgs), bodyArgs...)

	tailSQL, tailArgs := spec.buildQueryTail()
	args = append(args, tailArgs...)

	return appendQueryLockClause(cteSQL+bodySQL+tailSQL, spec.Lock), args, nil
}

func (spec querySpec[O]) buildSimpleListSQL(useKeyword bool) (string, []any) {
	selectSQL, selectArgs := spec.buildSelect()
	fromSQL, fromArgs := spec.buildFrom()
	whereSQL, whereArgs := spec.buildWhere(useKeyword)
	groupBySQL, groupByArgs := spec.buildGroupBy()
	havingSQL, havingArgs := spec.buildHaving()

	args := slices.Clone(selectArgs)
	args = append(args, fromArgs...)
	args = append(args, whereArgs...)
	args = append(args, groupByArgs...)
	args = append(args, havingArgs...)

	return selectSQL + fromSQL + whereSQL + groupBySQL + havingSQL, args
}

func (spec querySpec[O]) buildCountSQL(useKeyword bool) (string, []any, error) {
	cteSQL, cteArgs, err := spec.buildCTEPrefix(useKeyword)
	if err != nil {
		return "", nil, err
	}

	if len(spec.SetOps) > 0 || spec.requiresWrappedCount() {
		listSQL, listArgs := spec.buildListBodySQL(useKeyword)
		args := append(slices.Clone(cteArgs), listArgs...)

		return cteSQL + spec.wrapCountSQL(listSQL), args, nil
	}

	fromSQL, fromArgs := spec.buildFrom()
	whereSQL, whereArgs := spec.buildWhere(useKeyword)

	args := append(slices.Clone(cteArgs), fromArgs...)
	args = append(args, whereArgs...)

	return cteSQL + "SELECT COUNT(1)" + fromSQL + whereSQL, args, nil
}

func (spec querySpec[O]) buildKwCntSQL() (string, []any, error) {
	return spec.buildCountSQL(true)
}

func (spec querySpec[O]) buildKwListSQL() (string, []any, error) {
	cteSQL, cteArgs, err := spec.buildCTEPrefix(true)
	if err != nil {
		return "", nil, err
	}

	bodySQL, bodyArgs := spec.buildListBodySQL(true)
	args := append(slices.Clone(cteArgs), bodyArgs...)

	tailSQL, tailArgs := spec.buildQueryTail()
	args = append(args, tailArgs...)

	return appendQueryLockClause(cteSQL+bodySQL+tailSQL, spec.Lock), args, nil
}

func (spec querySpec[O]) buildListBodySQL(useKeyword bool) (string, []any) {
	if len(spec.SetOps) > 0 {
		return spec.buildCompoundListSQL(useKeyword)
	}

	return spec.buildSimpleCompoundOperandSQL(useKeyword)
}

func (spec querySpec[O]) buildCompoundListSQL(useKeyword bool) (string, []any) {
	return spec.buildCompoundChainSQL(useKeyword, len(spec.SetOps))
}

// buildCompoundChainSQL writes the first operand and the first n set operations.
// A chain is evaluated left to right, as it reads; SQL instead binds INTERSECT
// tighter than UNION and EXCEPT on MySQL and PostgreSQL but not on SQLite. So the
// operations before an INTERSECT that follows a UNION or EXCEPT are grouped as a
// derived table, which every dialect evaluates first.
func (spec querySpec[O]) buildCompoundChainSQL(useKeyword bool, n int) (string, []any) {
	ops := spec.SetOps[:n]

	var (
		builder strings.Builder
		args    []any
	)

	if split := regroupSetOperationsAt(ops); split > 0 {
		prefixSQL, prefixArgs := spec.buildCompoundChainSQL(useKeyword, split)
		builder.WriteString(derivedSetOperand(prefixSQL))

		args = append(args, prefixArgs...)
		ops = ops[split:]
	} else {
		baseSQL, baseArgs := spec.buildSimpleCompoundOperandSQL(useKeyword)
		builder.WriteString(baseSQL)

		args = append(args, baseArgs...)
	}

	for _, op := range ops {
		rightSQL, rightArgs := op.spec.buildOperandSQL(useKeyword)

		builder.WriteByte(' ')
		builder.WriteString(string(op.op))
		builder.WriteByte(' ')
		builder.WriteString(rightSQL)

		args = append(args, rightArgs...)
	}

	return builder.String(), args
}

// regroupSetOperationsAt returns the index of the last INTERSECT preceded by a
// UNION or EXCEPT, or 0 when the chain means the same flat on every dialect.
func regroupSetOperationsAt[O Owner](ops []setOperation[O]) int {
	loose := false
	split := 0

	for i, op := range ops {
		if op.op == intersectType || op.op == intersectAllType {
			if loose {
				split = i
			}

			continue
		}

		loose = true
	}

	return split
}

// derivedSetOperand groups a combined query as a derived table: SQLite has no
// parenthesized compound SELECT, and this spelling runs everywhere.
func derivedSetOperand(sql string) string {
	return "SELECT * FROM (" + sql + ") AS tsq_set"
}

func (spec querySpec[O]) buildOperandSQL(useKeyword bool) (string, []any) {
	if len(spec.SetOps) > 0 {
		sql, args := spec.buildListBodySQL(useKeyword)
		return derivedSetOperand(sql), args
	}

	return spec.buildSimpleCompoundOperandSQL(useKeyword)
}

func (spec querySpec[O]) buildSimpleCompoundOperandSQL(useKeyword bool) (string, []any) {
	return spec.buildSimpleListSQL(useKeyword)
}

func (spec querySpec[O]) buildSelect() (string, []any) {
	args := make([]any, 0, len(spec.Selects))
	fullNames := make([]string, 0, len(spec.Selects))

	// Rows are read by position, so a column's name only matters where the query
	// becomes a derived table (counting a grouped query, grouping a set operation),
	// and there MySQL refuses two columns of one name (error 1060): users.id and
	// orders.id. A repeated name is replaced after its first use.
	named := make(map[string]bool, len(spec.Selects))

	for i, col := range spec.Selects {
		name := rawColumnQualifiedName(col)

		if t, ok := col.(interface{ isTransformedExpression() bool }); !ok || !t.isTransformedExpression() {
			if named[col.OutputName()] {
				name += " AS " + rawIdentifier("tsq_c"+strconv.Itoa(i+1))
			}

			named[col.OutputName()] = true
		}

		fullNames = append(fullNames, name)
		args = append(args, expressionArgs(col)...)
	}

	return "SELECT " + strings.Join(fullNames, ", "), args
}

func (spec querySpec[O]) buildGroupBy() (string, []any) {
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

func (spec querySpec[O]) buildHaving() (string, []any) {
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

func (spec querySpec[O]) buildWhere(useKeyword bool) (string, []any) {
	if !useKeyword {
		if len(spec.Filters) == 0 {
			return "", nil
		}

		return buildConditionSQL(" WHERE ", spec.Filters)
	}

	clauses := make([]string, 0, len(spec.Filters)+1)
	for _, cond := range spec.Filters {
		clauses = append(clauses, conditionClause(cond))
	}

	args := collectConditionArgs(spec.Filters...)

	if len(spec.KeywordSearch) > 0 {
		kwClauses := make([]string, 0, len(spec.KeywordSearch))
		for _, col := range spec.KeywordSearch {
			kwClauses = append(kwClauses, rawColumnQualifiedName(col)+" LIKE ?"+keywordLikeEscapeClause)
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

func (spec querySpec[O]) buildFrom() (string, []any) {
	var fromBuilder strings.Builder
	args := make([]any, 0)

	includedTables := make(map[string]bool)

	fromBuilder.WriteString(" FROM ")
	fromBuilder.WriteString(rawTableIdentifier(spec.From))

	includedTables[spec.From.Table()] = true

	for _, item := range spec.Joins {
		if item.joinType == crossJoinType {
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

func (spec querySpec[O]) requiresWrappedCount() bool {
	return len(spec.SetOps) > 0 ||
		len(spec.GroupBy) > 0 ||
		len(spec.Having) > 0 ||
		spec.hasDistinctSelect() ||
		spec.hasAggregateSelect()
}

func (spec querySpec[O]) wrapCountSQL(inner string) string {
	return "SELECT COUNT(1) FROM (" + inner + ") AS _tsq_cnt"
}

func (spec querySpec[O]) hasDistinctSelect() bool {
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

func (spec querySpec[O]) hasAggregateSelect() bool {
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

func cloneQuerySpec[O Owner](spec querySpec[O]) querySpec[O] {
	cloned := querySpec[O]{
		From:          spec.From,
		Selects:       slices.Clone(spec.Selects),
		Filters:       slices.Clone(spec.Filters),
		KeywordSearch: slices.Clone(spec.KeywordSearch),
		Joins:         slices.Clone(spec.Joins),
		GroupBy:       slices.Clone(spec.GroupBy),
		Having:        slices.Clone(spec.Having),
		OrderBys:      slices.Clone(spec.OrderBys),
		Limit:         cloneIntPointer(spec.Limit),
		Offset:        cloneIntPointer(spec.Offset),
		Lock:          spec.Lock,
		SetOps:        make([]setOperation[O], 0, len(spec.SetOps)),
		Correlated:    slices.Clone(spec.Correlated),
	}

	for _, op := range spec.SetOps {
		cloned.SetOps = append(cloned.SetOps, setOperation[O]{
			op:   op.op,
			spec: cloneQuerySpec(op.spec),
		})
	}

	return cloned
}

// buildQueryTail renders ORDER BY, LIMIT and OFFSET.
//
// It is deliberately not part of buildListBodySQL: the body is also used as a set
// operation operand and as a CTE body, and ORDER BY / LIMIT there would bind to the
// operand rather than to the whole query. The tail is appended once, around the
// finished body, and still before the row-lock clause, which SQL puts last.
//
// The count query never carries the tail. Count reports how many rows match, which a
// LIMIT does not change, and ORDER BY in a counted subquery is pointless work.
func (spec querySpec[O]) buildQueryTail() (string, []any) {
	var (
		builder strings.Builder
		args    []any
	)

	if len(spec.OrderBys) > 0 {
		terms := make([]string, 0, len(spec.OrderBys))

		for _, order := range spec.OrderBys {
			// A set operation is ordered by its output columns, by name: a
			// table-qualified column is refused by PostgreSQL and MySQL there.
			if len(spec.SetOps) > 0 {
				terms = append(terms, rawIdentifier(order.field.OutputName())+" "+string(order.order))
				continue
			}

			terms = append(terms, rawColumnQualifiedName(order.field)+" "+string(order.order))
			args = append(args, expressionArgs(order.field)...)
		}

		builder.WriteString(" ORDER BY ")
		builder.WriteString(strings.Join(terms, ", "))
	}

	// LIMIT is bound rather than inlined so the value travels the same path as every
	// other argument and the dialect rewrites its placeholder like any other.
	if spec.Limit != nil {
		builder.WriteString(" LIMIT ?")

		args = append(args, *spec.Limit)
	}

	if spec.Offset != nil {
		// Every supported dialect requires a LIMIT before OFFSET; a bare OFFSET is a
		// syntax error on MySQL and SQLite. Build() rejects that combination, so
		// reaching here without a limit is not possible.
		builder.WriteString(" OFFSET ?")

		args = append(args, *spec.Offset)
	}

	return builder.String(), args
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}

	cloned := *value

	return &cloned
}

func appendQueryLockClause(sql string, lock queryLock) string {
	clause := lock.clause()
	if clause == "" {
		return sql
	}

	return sql + " " + clause
}
