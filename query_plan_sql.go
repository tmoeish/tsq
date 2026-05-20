package tsq

import (
	"slices"
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

	return appendQueryLockClause(cteSQL+bodySQL, spec.Lock), args, nil
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

	return appendQueryLockClause(cteSQL+bodySQL, spec.Lock), args, nil
}

func (spec querySpec[O]) buildListBodySQL(useKeyword bool) (string, []any) {
	if len(spec.SetOps) > 0 {
		return spec.buildCompoundListSQL(useKeyword)
	}

	return spec.buildSimpleCompoundOperandSQL(useKeyword)
}

func (spec querySpec[O]) buildCompoundListSQL(useKeyword bool) (string, []any) {
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

func (spec querySpec[O]) buildOperandSQL(useKeyword bool) (string, []any) {
	if len(spec.SetOps) > 0 {
		sql, args := spec.buildListBodySQL(useKeyword)
		return "(" + sql + ")", args
	}

	return spec.buildSimpleCompoundOperandSQL(useKeyword)
}

func (spec querySpec[O]) buildSimpleCompoundOperandSQL(useKeyword bool) (string, []any) {
	return spec.buildSimpleListSQL(useKeyword)
}

func (spec querySpec[O]) buildSelect() (string, []any) {
	args := make([]any, 0, len(spec.Selects))
	fullNames := make([]string, 0, len(spec.Selects))

	for _, col := range spec.Selects {
		fullNames = append(fullNames, rawColumnQualifiedName(col))
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
		Lock:          spec.Lock,
		SetOps:        make([]setOperation[O], 0, len(spec.SetOps)),
	}

	for _, op := range spec.SetOps {
		cloned.SetOps = append(cloned.SetOps, setOperation[O]{
			op:   op.op,
			spec: cloneQuerySpec(op.spec),
		})
	}

	return cloned
}

func appendQueryLockClause(sql string, lock queryLock) string {
	clause := lock.clause()
	if clause == "" {
		return sql
	}

	return sql + " " + clause
}
