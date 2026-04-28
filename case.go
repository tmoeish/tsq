package tsq

import (
	"maps"
	"sort"
	"strings"

	"github.com/juju/errors"
)

type caseBranch struct {
	cond   Condition
	result Expression
}

// CaseBuilder builds a searched CASE expression.
type CaseBuilder[T any] struct {
	whens     []caseBranch
	elseExpr  Expression
	hasElse   bool
	tables    map[string]Table
	aggregate bool
	distinct  bool
	buildErr  error
}

// Case creates a searched CASE expression builder.
func Case[T any]() *CaseBuilder[T] {
	return &CaseBuilder[T]{
		whens:  make([]caseBranch, 0),
		tables: make(map[string]Table),
	}
}

// When appends a WHEN ... THEN ... branch to the CASE expression.
func (b *CaseBuilder[T]) When(cond Condition, result any) *CaseBuilder[T] {
	if b == nil {
		return &CaseBuilder[T]{buildErr: errors.New("case builder cannot be nil")}
	}

	if b.buildErr != nil {
		return b
	}

	clause, condTables, _, err := validateConditionInput(cond)
	if err != nil {
		b.buildErr = errors.Trace(err)
		return b
	}

	if strings.TrimSpace(clause) == "" {
		b.buildErr = errors.New("case condition cannot be empty")
		return b
	}

	expr := argumentToExpression(result)
	if err := expressionBuildError(expr); err != nil {
		b.buildErr = errors.Trace(err)
		return b
	}

	b.whens = append(b.whens, caseBranch{cond: cond, result: expr})
	maps.Copy(b.tables, condTables)
	maps.Copy(b.tables, expressionTables(result))

	agg, distinct := expressionFlags(result)
	b.aggregate = b.aggregate || agg
	b.distinct = b.distinct || distinct

	return b
}

// Else sets the ELSE branch for the CASE expression.
func (b *CaseBuilder[T]) Else(result any) *CaseBuilder[T] {
	if b == nil {
		return &CaseBuilder[T]{buildErr: errors.New("case builder cannot be nil")}
	}

	if b.buildErr != nil {
		return b
	}

	expr := argumentToExpression(result)
	if err := expressionBuildError(expr); err != nil {
		b.buildErr = errors.Trace(err)
		return b
	}

	b.elseExpr = expr
	b.hasElse = true
	maps.Copy(b.tables, expressionTables(result))

	agg, distinct := expressionFlags(result)
	b.aggregate = b.aggregate || agg
	b.distinct = b.distinct || distinct

	return b
}

// End finalizes the CASE expression into a selectable column.
func (b *CaseBuilder[T]) End() Col[T] {
	if b == nil {
		return Col[T]{buildErr: errors.New("case builder cannot be nil")}
	}

	if b.buildErr != nil {
		return Col[T]{buildErr: errors.Trace(b.buildErr)}
	}

	if len(b.whens) == 0 {
		return Col[T]{buildErr: errors.New("case expression requires at least one WHEN branch")}
	}

	if len(b.tables) == 0 {
		return Col[T]{buildErr: errors.New("case expression must reference at least one table")}
	}

	tableNames := make([]string, 0, len(b.tables))
	for name := range b.tables {
		tableNames = append(tableNames, name)
	}

	sort.Strings(tableNames)

	baseName := tableNames[0]
	baseTable := b.tables[baseName]

	otherTables := cloneTableMap(b.tables)
	delete(otherTables, baseName)

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("CASE")

	args := make([]any, 0)

	for _, branch := range b.whens {
		sqlBuilder.WriteString(" WHEN ")
		sqlBuilder.WriteString(conditionClause(branch.cond))
		sqlBuilder.WriteString(" THEN ")
		sqlBuilder.WriteString(branch.result.Expr())
		args = append(args, branch.cond.Args()...)
		args = append(args, branch.result.Args()...)
	}

	if b.hasElse {
		sqlBuilder.WriteString(" ELSE ")
		sqlBuilder.WriteString(b.elseExpr.Expr())
		args = append(args, b.elseExpr.Args()...)
	}

	sqlBuilder.WriteString(" END")

	return Col[T]{
		table:         baseTable,
		name:          "case",
		qualifiedName: sqlBuilder.String(),
		jsonFieldName: "case",
		args:          args,
		aggregate:     b.aggregate,
		distinct:      b.distinct,
		transformed:   true,
		tables:        otherTables,
	}
}

func cloneTableMap(src map[string]Table) map[string]Table {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]Table, len(src))
	maps.Copy(dst, src)

	return dst
}

func expressionTables(arg any) map[string]Table {
	switch v := arg.(type) {
	case Column:
		return columnTables(v)
	default:
		return nil
	}
}

func expressionFlags(arg any) (aggregate, distinct bool) {
	switch v := arg.(type) {
	case interface{ isAggregateExpression() bool }:
		aggregate = v.isAggregateExpression()
	case Column:
		if agg, ok := v.(interface{ isAggregateExpression() bool }); ok {
			aggregate = agg.isAggregateExpression()
		}
	}

	switch v := arg.(type) {
	case interface{ isDistinctExpression() bool }:
		distinct = v.isDistinctExpression()
	case Column:
		if d, ok := v.(interface{ isDistinctExpression() bool }); ok {
			distinct = d.isDistinctExpression()
		}
	}

	return aggregate, distinct
}

func columnTables(col Column) map[string]Table {
	table, err := validateColumnInput(col)
	if err != nil {
		return nil
	}

	tables := map[string]Table{table.Table(): table}
	if refs, ok := col.(interface{ referencedTables() map[string]Table }); ok {
		maps.Copy(tables, refs.referencedTables())
	}

	return tables
}
