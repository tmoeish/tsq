package tsq

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"time"
)

type queryArgMarker string

const (
	externalArgMarker queryArgMarker = "external"
	keywordArgMarker  queryArgMarker = "keyword"
)

// And combines conditions with SQL AND.
func And(conds ...Condition) Cond {
	if len(conds) == 0 {
		return rawCondition("1 = 1")
	}

	tables := make(map[string]Table)
	clauses := make([]string, 0, len(conds))

	for _, c := range conds {
		clause, condTables, _, err := validateConditionInput(c)
		if err != nil {
			return Cond{buildErr: err}
		}

		maps.Copy(tables, condTables)

		clauses = append(clauses, clause)
	}

	return Cond{
		tables: tables,
		expr:   "(" + strings.Join(clauses, " AND ") + ")",
		args:   collectConditionArgs(conds...),
	}
}

// Or combines conditions with SQL OR.
func Or(conds ...Condition) Cond {
	if len(conds) == 0 {
		return rawCondition("1 = 0")
	}

	tables := make(map[string]Table)
	clauses := make([]string, 0, len(conds))

	for _, c := range conds {
		clause, condTables, _, err := validateConditionInput(c)
		if err != nil {
			return Cond{buildErr: err}
		}

		maps.Copy(tables, condTables)

		clauses = append(clauses, clause)
	}

	return Cond{
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

// Cond stores a rendered predicate plus the metadata needed to execute it.
type Cond struct {
	tables   map[string]Table
	expr     string
	args     []any
	buildErr error
}

func pred[O Owner](cond Cond) Predicate[O] {
	return Predicate[O]{Cond: cond}
}

// Tables returns the tables referenced by the condition, keyed by logical table name.
func (c Cond) Tables() map[string]Table {
	return cloneTableMap(c.tables)
}

// Clause returns the canonical SQL fragment for the condition.
func (c Cond) Clause() string {
	return renderCanonicalSQL(c.expr)
}

func (c Cond) rawClause() string {
	return c.expr
}

// Args returns the bind arguments captured by the condition.
func (c Cond) Args() []any {
	return append([]any(nil), c.args...)
}

func (c Cond) buildError() error {
	return c.buildErr
}

// EQVar compares the column to a runtime-bound value with =.
func (c ColumnImpl[Owner, T]) EQVar() Predicate[Owner] { return c.Predicate(`%s = %s`, varMarker) }

// NEVar compares the column to a runtime-bound value with <>.
func (c ColumnImpl[Owner, T]) NEVar() Predicate[Owner] { return c.Predicate(`%s <> %s`, varMarker) }

// GTVar compares the column to a runtime-bound value with >.
func (c ColumnImpl[Owner, T]) GTVar() Predicate[Owner] { return c.Predicate(`%s > %s`, varMarker) }

// GTEVar compares the column to a runtime-bound value with >=.
func (c ColumnImpl[Owner, T]) GTEVar() Predicate[Owner] { return c.Predicate(`%s >= %s`, varMarker) }

// LTVar compares the column to a runtime-bound value with <.
func (c ColumnImpl[Owner, T]) LTVar() Predicate[Owner] { return c.Predicate(`%s < %s`, varMarker) }

// LTEVar compares the column to a runtime-bound value with <=.
func (c ColumnImpl[Owner, T]) LTEVar() Predicate[Owner] { return c.Predicate(`%s <= %s`, varMarker) }

// InVar binds a slice at execution time for IN predicates.
//
// TSQ intentionally treats nil and empty slices as an explicit "match nothing"
// filter. During execution the placeholder list is expanded from the runtime
// argument slice; when that slice is empty, TSQ renders IN (NULL), which keeps
// the query valid while producing zero matches across the supported built-in
// dialects. This is by design and lets callers express "no selected IDs" without
// adding custom branching around the query.
func (c ColumnImpl[Owner, T]) InVar() Predicate[Owner] {
	return c.Predicate(`%s IN (%s)`, varSliceMarker)
}

// StartsWithVar defers a prefix match to execution time, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) StartsWithVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("StartsWithVar"))
}

// NStartsWithVar defers a negated prefix match to execution time, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) NStartsWithVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NStartsWithVar"))
}

// EndsWithVar defers a suffix match to execution time, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) EndsWithVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("EndsWithVar"))
}

// NEndsWithVar defers a negated suffix match to execution time, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) NEndsWithVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NEndsWithVar"))
}

// ContainsVar defers a contains match to execution time, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) ContainsVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("ContainsVar"))
}

// NContainsVar defers a negated contains match to execution time, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) NContainsVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NContainsVar"))
}

// BetweenVar compares the column to two runtime-bound values with BETWEEN.
func (c ColumnImpl[Owner, T]) BetweenVar() Predicate[Owner] {
	return c.Predicate(`%s BETWEEN %s AND %s`, varMarker, varMarker)
}

// NBetweenVar compares the column to two runtime-bound values with NOT BETWEEN.
func (c ColumnImpl[Owner, T]) NBetweenVar() Predicate[Owner] {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, varMarker, varMarker)
}

// EQ compares the column to arg with =.
func (c ColumnImpl[Owner, T]) EQ(arg T) Predicate[Owner] {
	return c.Predicate(`%s = %s`, Bind(arg))
}

// NE compares the column to arg with <>.
func (c ColumnImpl[Owner, T]) NE(arg T) Predicate[Owner] {
	return c.Predicate(`%s <> %s`, Bind(arg))
}

// GT compares the column to arg with >.
func (c ColumnImpl[Owner, T]) GT(arg T) Predicate[Owner] {
	return c.Predicate(`%s > %s`, Bind(arg))
}

// GTE compares the column to arg with >=.
func (c ColumnImpl[Owner, T]) GTE(arg T) Predicate[Owner] {
	return c.Predicate(`%s >= %s`, Bind(arg))
}

// LT compares the column to arg with <.
func (c ColumnImpl[Owner, T]) LT(arg T) Predicate[Owner] {
	return c.Predicate(`%s < %s`, Bind(arg))
}

// LTE compares the column to arg with <=.
func (c ColumnImpl[Owner, T]) LTE(arg T) Predicate[Owner] {
	return c.Predicate(`%s <= %s`, Bind(arg))
}

// StartsWith compares the column to a bound prefix pattern.
func (c ColumnImpl[Owner, T]) StartsWith(str string) Predicate[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind(str+"%"))
}

// NStartsWith compares the column to a negated bound prefix pattern.
func (c ColumnImpl[Owner, T]) NStartsWith(str string) Predicate[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind(str+"%"))
}

// EndsWith compares the column to a bound suffix pattern.
func (c ColumnImpl[Owner, T]) EndsWith(str string) Predicate[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind("%"+str))
}

// NEndsWith compares the column to a negated bound suffix pattern.
func (c ColumnImpl[Owner, T]) NEndsWith(str string) Predicate[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind("%"+str))
}

// Contains compares the column to a bound contains pattern.
func (c ColumnImpl[Owner, T]) Contains(str string) Predicate[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind("%"+str+"%"))
}

// NContains compares the column to a negated bound contains pattern.
func (c ColumnImpl[Owner, T]) NContains(str string) Predicate[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind("%"+str+"%"))
}

// Between compares the column to an inclusive range.
func (c ColumnImpl[Owner, T]) Between(start, end T) Predicate[Owner] {
	return c.Predicate(`%s BETWEEN %s AND %s`, Bind(start), Bind(end))
}

// NBetween compares the column to values outside an inclusive range.
func (c ColumnImpl[Owner, T]) NBetween(start, end T) Predicate[Owner] {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, Bind(start), Bind(end))
}

// In compares the column to an explicit list of bound values.
func (c ColumnImpl[Owner, T]) In(args ...T) Predicate[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 0"))
	}

	return c.Predicate(`%s IN (%s)`, BindSlice(args))
}

// NIn compares the column to a negated list of bound values.
func (c ColumnImpl[Owner, T]) NIn(args ...T) Predicate[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 1"))
	}

	return c.Predicate(`%s NOT IN (%s)`, BindSlice(args))
}

// IsNull checks whether the column value is NULL.
func (c ColumnImpl[Owner, T]) IsNull() Predicate[Owner] {
	return c.Predicate(`%s IS NULL`)
}

// IsNotNull checks whether the column value is not NULL.
func (c ColumnImpl[Owner, T]) IsNotNull() Predicate[Owner] {
	return c.Predicate(`%s IS NOT NULL`)
}

// EQCol compares the column to another column with =.
func (c ColumnImpl[Owner, T]) EQCol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Predicate(`%s = %s`, other)
}

// NECol compares the column to another column with <>.
func (c ColumnImpl[Owner, T]) NECol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Predicate(`%s <> %s`, other)
}

// GTCol compares the column to another column with >.
func (c ColumnImpl[Owner, T]) GTCol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Predicate(`%s > %s`, other)
}

// GTECol compares the column to another column with >=.
func (c ColumnImpl[Owner, T]) GTECol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Predicate(`%s >= %s`, other)
}

// LTCol compares the column to another column with <.
func (c ColumnImpl[Owner, T]) LTCol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Predicate(`%s < %s`, other)
}

// LTECol compares the column to another column with <=.
func (c ColumnImpl[Owner, T]) LTECol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Predicate(`%s <= %s`, other)
}

// StartsWithCol compares the column to another column with a prefix match, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) StartsWithCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("StartsWithCol"))
}

// NStartsWithCol compares the column to another column with a negated prefix match, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) NStartsWithCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NStartsWithCol"))
}

// EndsWithCol compares the column to another column with a suffix match, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) EndsWithCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("EndsWithCol"))
}

// NEndsWithCol compares the column to another column with a negated suffix match, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) NEndsWithCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NEndsWithCol"))
}

// ContainsCol compares the column to another column with a contains match, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) ContainsCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("ContainsCol"))
}

// NContainsCol compares the column to another column with a negated contains match, which tsq rejects for portability.
func (c ColumnImpl[Owner, T]) NContainsCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NContainsCol"))
}

// ================================================
// 子查询条件
// ================================================

type subquery interface {
	subquerySQL() string
	subqueryArgs() []any
	subquerySelectCount() int
}

type subqueryUsage string

const (
	scalarSubqueryUsage     subqueryUsage = "scalar"
	membershipSubqueryUsage subqueryUsage = "membership"
	existsSubqueryUsage     subqueryUsage = "exists"
)

// EQSub compares the column to a scalar subquery with =.
func (c ColumnImpl[Owner, T]) EQSub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s = %s`, scalarSubquery(sq))
}

// NESub compares the column to a scalar subquery with <>.
func (c ColumnImpl[Owner, T]) NESub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s <> %s`, scalarSubquery(sq))
}

// GTSub compares the column to a scalar subquery with >.
func (c ColumnImpl[Owner, T]) GTSub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s > %s`, scalarSubquery(sq))
}

// GTESub compares the column to a scalar subquery with >=.
func (c ColumnImpl[Owner, T]) GTESub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s >= %s`, scalarSubquery(sq))
}

// LTSub compares the column to a scalar subquery with <.
func (c ColumnImpl[Owner, T]) LTSub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s < %s`, scalarSubquery(sq))
}

// LTESub compares the column to a scalar subquery with <=.
func (c ColumnImpl[Owner, T]) LTESub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s <= %s`, scalarSubquery(sq))
}

// LikeSub compares the column to a scalar subquery with LIKE.
func (c ColumnImpl[Owner, T]) LikeSub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s LIKE %s`, scalarSubquery(sq))
}

// NLikeSub compares the column to a scalar subquery with NOT LIKE.
func (c ColumnImpl[Owner, T]) NLikeSub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, scalarSubquery(sq))
}

// InSub compares the column to a membership subquery with IN.
func (c ColumnImpl[Owner, T]) InSub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s IN %s`, membershipSubquery(sq))
}

// NInSub compares the column to a membership subquery with NOT IN.
func (c ColumnImpl[Owner, T]) NInSub(sq subquery) Predicate[Owner] {
	return c.Predicate(`%s NOT IN %s`, membershipSubquery(sq))
}

// ExistsSub returns an EXISTS predicate for the supplied subquery.
func (c ColumnImpl[Owner, T]) ExistsSub(sq subquery) Predicate[Owner] {
	subquery, args, err := buildSubqueryExpression(sq, existsSubqueryUsage)
	if err != nil {
		return pred[Owner](Cond{buildErr: err})
	}

	return pred[Owner](Cond{
		tables: map[string]Table{},
		expr:   "EXISTS " + subquery,
		args:   args,
	})
}

// NExistsSub returns a NOT EXISTS predicate for the supplied subquery.
func (c ColumnImpl[Owner, T]) NExistsSub(sq subquery) Predicate[Owner] {
	subquery, args, err := buildSubqueryExpression(sq, existsSubqueryUsage)
	if err != nil {
		return pred[Owner](Cond{buildErr: err})
	}

	return pred[Owner](Cond{
		tables: map[string]Table{},
		expr:   "NOT EXISTS " + subquery,
		args:   args,
	})
}

// Unique returns a deferred portability error because UNIQUE subquery predicates are not supported.
func (c ColumnImpl[Owner, T]) Unique(_ subquery) Predicate[Owner] {
	return pred[Owner](unsupportedSubqueryPredicate("UNIQUE"))
}

// NUnique returns a deferred portability error because NOT UNIQUE subquery predicates are not supported.
func (c ColumnImpl[Owner, T]) NUnique(_ subquery) Predicate[Owner] {
	return pred[Owner](unsupportedSubqueryPredicate("NOT UNIQUE"))
}

// Predicate renders a custom predicate format around the receiver column.
func (c ColumnImpl[Owner, T]) Predicate(op string, args ...any) Predicate[Owner] {
	if err := validatePredicateFormat(op, len(args)+1); err != nil {
		return pred[Owner](Cond{buildErr: err})
	}

	baseTable, err := validateColumnInput(c)
	if err != nil {
		return pred[Owner](Cond{buildErr: err})
	}

	tables := map[string]Table{baseTable.Table(): baseTable}

	for _, arg := range args {
		if col, ok := arg.(SQLColumn); ok {
			table, err := validateColumnInput(col)
			if err != nil {
				return pred[Owner](Cond{buildErr: err})
			}

			tables[table.Table()] = table
		}
	}

	formatArgs := make([]any, 0, len(args)+1)
	formatArgs = append(formatArgs, c.rawQualifiedName())

	for _, arg := range args {
		expr := argumentToExpression(arg)
		if err := expressionBuildError(expr); err != nil {
			return pred[Owner](Cond{buildErr: err})
		}

		formatArgs = append(formatArgs, expr.Expr())
	}

	return pred[Owner](Cond{
		tables: tables,
		expr:   fmt.Sprintf(op, formatArgs...),
		args:   collectExpressionArgs(args...),
	})
}

func (c ColumnImpl[Owner, T]) rawCondition(expr string) Predicate[Owner] {
	table, err := validateColumnInput(c)
	if err != nil {
		return pred[Owner](Cond{buildErr: err})
	}

	return pred[Owner](Cond{
		tables: map[string]Table{table.Table(): table},
		expr:   expr,
	})
}

func rawCondition(expr string) Cond {
	return Cond{
		tables: map[string]Table{},
		expr:   expr,
	}
}

// unsupportedSubqueryPredicate returns a condition with a deferred error indicating
// that this predicate uses subqueries, which are not supported by TSQ's built-in dialects.
// The error will be returned when Build() is called, not immediately.
func unsupportedSubqueryPredicate(name string) Cond {
	return Cond{buildErr: fmt.Errorf("%s subquery predicate is not supported by TSQ's built-in dialects", name)}
}

// unsupportedPatternPredicate returns a condition with a deferred error indicating
// that this pattern predicate is not portable across TSQ's built-in dialects.
// The error will be returned when Build() is called, not immediately.
// Users should use LIKE with an explicit pattern instead.
func unsupportedPatternPredicate(name string) Cond {
	return Cond{buildErr: fmt.Errorf(
		"%s is not portable across TSQ's built-in dialects; use LIKE with an explicit pattern instead",
		name,
	)}
}

func validatePredicateFormat(op string, placeholderCount int) error {
	if strings.TrimSpace(op) == "" {
		return fmt.Errorf("predicate format cannot be empty")
	}

	actual, err := countStringFormatPlaceholders(op)
	if err != nil {
		return err
	}

	if actual != placeholderCount {
		return fmt.Errorf(
			"predicate format placeholder count mismatch: expected %d, got %d",
			placeholderCount,
			actual,
		)
	}

	return nil
}

// Expression represents a SQL fragment plus the args needed to render it safely.
type Expression interface {
	Expr() string // Expr returns the SQL fragment text.
	Args() []any  // Args returns the bind arguments referenced by Expr.
}

type expressionError struct {
	err error
}

// Expr returns an empty fragment so expressionError can flow through builders until build time.
func (e expressionError) Expr() string { return "" }

// Args returns nil because expressionError carries only a deferred build error.
func (e expressionError) Args() []any { return nil }

func (e expressionError) buildError() error {
	return e.err
}

// variableExpression marks a single runtime-supplied bind placeholder.
type variableExpression struct{}

// Expr returns the placeholder emitted into the SQL fragment.
func (v variableExpression) Expr() string { return "?" }

// Args returns the marker consumed later by runtime argument resolution.
func (v variableExpression) Args() []any { return []any{externalArgMarker} }

var varMarker variableExpression

// variableSliceExpression marks a runtime-supplied slice placeholder used by IN predicates.
type variableSliceExpression struct{}

// Expr returns the placeholder emitted into the SQL fragment before slice expansion.
func (v variableSliceExpression) Expr() string { return "?" }

// Args returns the marker consumed later by runtime slice expansion.
func (v variableSliceExpression) Args() []any { return []any{externalSliceArgMarker{}} }

var varSliceMarker variableSliceExpression

// valuesExpression stores a fully expanded placeholder list such as "?, ?, ?".
type valuesExpression struct {
	expr string
	args []any
}

// Expr returns the expanded placeholder list.
func (a valuesExpression) Expr() string {
	return a.expr
}

// Args returns the bind arguments that correspond to the placeholder list.
func (a valuesExpression) Args() []any {
	return append([]any(nil), a.args...)
}

func newValuesExpression(args any) Expression {
	v := reflect.ValueOf(args)
	if v.Kind() != reflect.Slice {
		return expressionError{err: fmt.Errorf("expected slice, got %T", args)}
	}

	values := make([]string, v.Len())
	bindArgs := make([]any, v.Len())

	for i := range v.Len() {
		value := v.Index(i).Interface()
		if err := validatePredicateValue(value); err != nil {
			return expressionError{err: err}
		}

		values[i] = "?"
		bindArgs[i] = value
	}

	return valuesExpression{
		expr: strings.Join(values, ", "),
		args: bindArgs,
	}
}

type rawExpression struct {
	expr string
	args []any
}

// Expr returns the raw SQL fragment.
func (r rawExpression) Expr() string { return r.expr }

// Args returns the bind arguments referenced by the raw fragment.
func (r rawExpression) Args() []any { return append([]any(nil), r.args...) }

// Bind returns a parameterized expression for value.
func Bind(value any) Expression {
	if err := validatePredicateValue(value); err != nil {
		return expressionError{err: err}
	}

	return rawExpression{expr: "?", args: []any{value}}
}

// BindSlice returns a comma-separated placeholder list for a slice value.
func BindSlice(values any) Expression {
	return newValuesExpression(values)
}

// argumentToExpression converts various argument types to their SQL expression representation.
func argumentToExpression(arg any) Expression {
	switch v := arg.(type) {
	case Expression:
		return v
	case SQLColumn:
		return rawExpression{expr: rawColumnQualifiedName(v), args: expressionArgs(v)}
	case validatedSubquery:
		expr, args, err := buildSubqueryExpression(v.query, v.usage)
		if err != nil {
			return expressionError{err: err}
		}

		return rawExpression{expr: expr, args: args}
	case subquery:
		return expressionError{err: errors.New(
			"raw subqueries are not allowed in Predicate; use EQSub/NESub/GTSub/InSub/ExistsSub helpers",
		)}
	default:
		return Bind(v)
	}
}

type validatedSubquery struct {
	query subquery
	usage subqueryUsage
}

func scalarSubquery(q subquery) validatedSubquery {
	return validatedSubquery{query: q, usage: scalarSubqueryUsage}
}

func membershipSubquery(q subquery) validatedSubquery {
	return validatedSubquery{query: q, usage: membershipSubqueryUsage}
}

func buildSubqueryExpression(q subquery, usage subqueryUsage) (string, []any, error) {
	if q == nil {
		return "", nil, errors.New("subquery cannot be nil")
	}

	sqlText := strings.TrimSpace(q.subquerySQL())
	if sqlText == "" {
		return "", nil, errors.New("subquery is not built")
	}

	selectCount := q.subquerySelectCount()
	if selectCount == 0 {
		return "", nil, errors.New("subquery metadata is unavailable; build the subquery with tsq.Select(...).Build()")
	}

	switch usage {
	case scalarSubqueryUsage:
		if selectCount != 1 {
			return "", nil, fmt.Errorf("scalar subquery must select exactly one column, got %d", selectCount)
		}
	case membershipSubqueryUsage:
		if selectCount != 1 {
			return "", nil, fmt.Errorf("in subquery must select exactly one column, got %d", selectCount)
		}
	case existsSubqueryUsage:
	default:
		return "", nil, fmt.Errorf("unknown subquery usage %q", usage)
	}

	return fmt.Sprintf("(%s)", sqlText), q.subqueryArgs(), nil
}

func collectExpressionArgs(args ...any) []any {
	var result []any

	for _, arg := range args {
		result = append(result, argumentToExpression(arg).Args()...)
	}

	return result
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

func expressionArgs(col SQLColumn) []any {
	type expressionArgser interface {
		expressionArgs() []any
	}

	if withArgs, ok := col.(expressionArgser); ok {
		return withArgs.expressionArgs()
	}

	return nil
}

func expressionBuildError(expr Expression) error {
	if expr == nil {
		return errors.New("expression cannot be nil")
	}

	if carrier, ok := expr.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return carrier.buildError()
	}

	return nil
}

func validatePredicateValue(arg any) error {
	if isNilValue(arg) {
		return errors.New("null predicate values are not supported; use IsNull/IsNotNull explicitly")
	}

	if valuer, ok := arg.(driver.Valuer); ok {
		value, err := valuer.Value()
		if err != nil {
			return fmt.Errorf("failed to evaluate predicate value %T: %w", arg, err)
		}

		if value == nil {
			return errors.New("null predicate values are not supported; use IsNull/IsNotNull explicitly")
		}

		return validatePredicateScalar(value)
	}

	return validatePredicateScalar(arg)
}

func validatePredicateScalar(arg any) error {
	v := reflect.ValueOf(arg)
	for v.IsValid() && v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return errors.New("null predicate values are not supported; use IsNull/IsNotNull explicitly")
		}

		v = v.Elem()
		arg = v.Interface()
	}

	if !v.IsValid() {
		return errors.New("null predicate values are not supported; use IsNull/IsNotNull explicitly")
	}

	if _, ok := arg.(time.Time); ok {
		return nil
	}

	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}

		return fmt.Errorf("unsupported predicate slice type %v; use In/InVar for collections", v.Type())
	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}

		return fmt.Errorf("unsupported predicate array type %v", v.Type())
	case reflect.Map, reflect.Struct:
		return fmt.Errorf("unsupported predicate value type %v", v.Type())
	default:
		return nil
	}
}
