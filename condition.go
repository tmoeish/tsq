package tsq

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"maps"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
)

const sqlNullLiteral = "NULL"

type queryArgMarker string

const (
	externalArgMarker queryArgMarker = "external"
	keywordArgMarker  queryArgMarker = "keyword"
)

// ================================================
// 逻辑组合条件
// ================================================

// And combines multiple conditions with AND logic
func And(conds ...Condition) Cond {
	if len(conds) == 0 {
		return rawCondition("1 = 1")
	}

	tables := make(map[string]Table)
	clauses := make([]string, 0, len(conds))

	for _, c := range conds {
		clause, condTables, _, err := validateConditionInput(c)
		if err != nil {
			return Cond{buildErr: errors.Trace(err)}
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

// Or combines multiple conditions with OR logic
func Or(conds ...Condition) Cond {
	if len(conds) == 0 {
		return rawCondition("1 = 0")
	}

	tables := make(map[string]Table)
	clauses := make([]string, 0, len(conds))

	for _, c := range conds {
		clause, condTables, _, err := validateConditionInput(c)
		if err != nil {
			return Cond{buildErr: errors.Trace(err)}
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

// ================================================
// 基础条件接口和结构体
// ================================================

// Condition interface for SQL conditions
type Condition interface {
	Tables() map[string]Table
	Clause() string
	Args() []any
}

type rawConditionClauser interface {
	rawClause() string
}

// Cond represents a SQL condition
type Cond struct {
	tables   map[string]Table
	expr     string
	args     []any
	buildErr error
}

func pred[Owner Table](cond Cond) Pred[Owner] {
	return Pred[Owner]{Cond: cond}
}

func (c Cond) Tables() map[string]Table {
	return c.tables
}

func (c Cond) Clause() string {
	return renderCanonicalSQL(c.expr)
}

func (c Cond) rawClause() string {
	return c.expr
}

func (c Cond) Args() []any {
	return append([]any(nil), c.args...)
}

func (c Cond) buildError() error {
	return c.buildErr
}

// ================================================
// 条件方法参数顺序约定
// ================================================
//
// 所有 Condition 方法遵循一致的参数顺序模式：
//
// Pattern: column.OPERATOR(values...)
//   - AnyColumn: 接收者 (implicit)
//   - Operator: 方法名（EQ, GT, StartsWith 等）
//   - Values: 参数（value1, value2, ...)
//
// 约定示例：
//   col.EQ(value)              // column = value
//   col.Between(start, end)    // column BETWEEN start AND end
//   col.In(v1, v2, v3)        // column IN (v1, v2, v3)
//   col.StartsWith(prefix)      // column LIKE 'prefix%'
//
// 方法分类：
//   - 基础比较: EQ, NE, GT, GTE, LT, LTE
//   - 模式匹配: StartsWith, EndsWith, Contains
//   - 集合: In, InVar, NIn
//   - 范围: Between, NBetween
//   - 空值检查: IsNull, IsNotNull
//
// 绑定方式后缀（无后缀为参数绑定）：
//   - Var: 使用 ? 占位符，值由执行时提供
//   - Literal: 直接嵌入字面量
//   - Col: 与另一列比较
//   - Sub: 与子查询结果比较

// ================================================
// 变量比较条件 (使用 ? 占位符)
// ================================================

func (c Col[Owner, T]) EQVar() Pred[Owner]  { return c.Predicate(`%s = %s`, Var) }
func (c Col[Owner, T]) NEVar() Pred[Owner]  { return c.Predicate(`%s <> %s`, Var) }
func (c Col[Owner, T]) GTVar() Pred[Owner]  { return c.Predicate(`%s > %s`, Var) }
func (c Col[Owner, T]) GTEVar() Pred[Owner] { return c.Predicate(`%s >= %s`, Var) }
func (c Col[Owner, T]) LTVar() Pred[Owner]  { return c.Predicate(`%s < %s`, Var) }
func (c Col[Owner, T]) LTEVar() Pred[Owner] { return c.Predicate(`%s <= %s`, Var) }
func (c Col[Owner, T]) InVar() Pred[Owner]  { return c.Predicate(`%s IN (%s)`, VarSlice) }
func (c Col[Owner, T]) StartsWithVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("StartsWithVar"))
}

func (c Col[Owner, T]) NStartsWithVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NStartsWithVar"))
}

func (c Col[Owner, T]) EndsWithVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("EndsWithVar"))
}

func (c Col[Owner, T]) NEndsWithVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NEndsWithVar"))
}

func (c Col[Owner, T]) ContainsVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("ContainsVar"))
}

func (c Col[Owner, T]) NContainsVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NContainsVar"))
}
func (c Col[Owner, T]) BetweenVar() Pred[Owner] { return c.Predicate(`%s BETWEEN %s AND %s`, Var, Var) }
func (c Col[Owner, T]) NBetweenVar() Pred[Owner] {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, Var, Var)
}

// ================================================
// 常量比较条件
// ================================================

func (c Col[Owner, T]) EQ(arg T) Pred[Owner]  { return c.Predicate(`%s = %s`, Bind(arg)) }
func (c Col[Owner, T]) NE(arg T) Pred[Owner]  { return c.Predicate(`%s <> %s`, Bind(arg)) }
func (c Col[Owner, T]) GT(arg T) Pred[Owner]  { return c.Predicate(`%s > %s`, Bind(arg)) }
func (c Col[Owner, T]) GTE(arg T) Pred[Owner] { return c.Predicate(`%s >= %s`, Bind(arg)) }
func (c Col[Owner, T]) LT(arg T) Pred[Owner]  { return c.Predicate(`%s < %s`, Bind(arg)) }
func (c Col[Owner, T]) LTE(arg T) Pred[Owner] { return c.Predicate(`%s <= %s`, Bind(arg)) }
func (c Col[Owner, T]) StartsWith(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind(str+"%"))
}

func (c Col[Owner, T]) NStartsWith(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind(str+"%"))
}

func (c Col[Owner, T]) EndsWith(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind("%"+str))
}

func (c Col[Owner, T]) NEndsWith(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind("%"+str))
}

func (c Col[Owner, T]) Contains(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind("%"+str+"%"))
}

func (c Col[Owner, T]) NContains(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind("%"+str+"%"))
}

func (c Col[Owner, T]) Between(start, end T) Pred[Owner] {
	return c.Predicate(`%s BETWEEN %s AND %s`, Bind(start), Bind(end))
}

func (c Col[Owner, T]) NBetween(start, end T) Pred[Owner] {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, Bind(start), Bind(end))
}

func (c Col[Owner, T]) EQLiteral(arg T) Pred[Owner]  { return c.Predicate(`%s = %s`, Literal(arg)) }
func (c Col[Owner, T]) NELiteral(arg T) Pred[Owner]  { return c.Predicate(`%s <> %s`, Literal(arg)) }
func (c Col[Owner, T]) GTLiteral(arg T) Pred[Owner]  { return c.Predicate(`%s > %s`, Literal(arg)) }
func (c Col[Owner, T]) GTELiteral(arg T) Pred[Owner] { return c.Predicate(`%s >= %s`, Literal(arg)) }
func (c Col[Owner, T]) LTLiteral(arg T) Pred[Owner]  { return c.Predicate(`%s < %s`, Literal(arg)) }
func (c Col[Owner, T]) LTELiteral(arg T) Pred[Owner] { return c.Predicate(`%s <= %s`, Literal(arg)) }
func (c Col[Owner, T]) StartsWithLiteral(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Literal(str+"%"))
}

func (c Col[Owner, T]) NStartsWithLiteral(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Literal(str+"%"))
}

func (c Col[Owner, T]) EndsWithLiteral(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Literal("%"+str))
}

func (c Col[Owner, T]) NEndsWithLiteral(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Literal("%"+str))
}

func (c Col[Owner, T]) ContainsLiteral(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Literal("%"+str+"%"))
}

func (c Col[Owner, T]) NContainsLiteral(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Literal("%"+str+"%"))
}

func (c Col[Owner, T]) BetweenLiteral(start, end T) Pred[Owner] {
	return c.Predicate(`%s BETWEEN %s AND %s`, Literal(start), Literal(end))
}

func (c Col[Owner, T]) NBetweenLiteral(start, end T) Pred[Owner] {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, Literal(start), Literal(end))
}

func (c Col[Owner, T]) In(args ...T) Pred[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 0"))
	}

	return c.Predicate(`%s IN (%s)`, BindSlice(args))
}

func (c Col[Owner, T]) NIn(args ...T) Pred[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 1"))
	}

	return c.Predicate(`%s NOT IN (%s)`, BindSlice(args))
}

func (c Col[Owner, T]) InLiteral(args ...T) Pred[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 0"))
	}

	return c.Predicate(`%s IN (%s)`, literalValues(args))
}

func (c Col[Owner, T]) NInLiteral(args ...T) Pred[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 1"))
	}

	return c.Predicate(`%s NOT IN (%s)`, literalValues(args))
}
func (c Col[Owner, T]) IsNull() Pred[Owner]    { return c.Predicate(`%s IS NULL`) }
func (c Col[Owner, T]) IsNotNull() Pred[Owner] { return c.Predicate(`%s IS NOT NULL`) }

// ================================================
// 字段比较条件
// ================================================

func (c Col[Owner, T]) EQCol(other typedColumn[T]) Pred[Owner] { return c.Predicate(`%s = %s`, other) }
func (c Col[Owner, T]) NECol(other typedColumn[T]) Pred[Owner] { return c.Predicate(`%s <> %s`, other) }
func (c Col[Owner, T]) GTCol(other typedColumn[T]) Pred[Owner] { return c.Predicate(`%s > %s`, other) }
func (c Col[Owner, T]) GTECol(other typedColumn[T]) Pred[Owner] {
	return c.Predicate(`%s >= %s`, other)
}
func (c Col[Owner, T]) LTCol(other typedColumn[T]) Pred[Owner] { return c.Predicate(`%s < %s`, other) }
func (c Col[Owner, T]) LTECol(other typedColumn[T]) Pred[Owner] {
	return c.Predicate(`%s <= %s`, other)
}

func (c Col[Owner, T]) StartsWithCol(_ typedColumn[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("StartsWithCol"))
}

func (c Col[Owner, T]) NStartsWithCol(_ typedColumn[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NStartsWithCol"))
}

func (c Col[Owner, T]) EndsWithCol(_ typedColumn[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("EndsWithCol"))
}

func (c Col[Owner, T]) NEndsWithCol(_ typedColumn[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NEndsWithCol"))
}

func (c Col[Owner, T]) ContainsCol(_ typedColumn[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("ContainsCol"))
}

func (c Col[Owner, T]) NContainsCol(_ typedColumn[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NContainsCol"))
}

// ================================================
// 子查询条件
// ================================================

func (c Col[Owner, T]) EQSub(sqb *Query) Pred[Owner]    { return c.Predicate(`%s = %s`, sqb) }
func (c Col[Owner, T]) NESub(sqb *Query) Pred[Owner]    { return c.Predicate(`%s <> %s`, sqb) }
func (c Col[Owner, T]) GTSub(sqb *Query) Pred[Owner]    { return c.Predicate(`%s > %s`, sqb) }
func (c Col[Owner, T]) GTESub(sqb *Query) Pred[Owner]   { return c.Predicate(`%s >= %s`, sqb) }
func (c Col[Owner, T]) LTSub(sqb *Query) Pred[Owner]    { return c.Predicate(`%s < %s`, sqb) }
func (c Col[Owner, T]) LTESub(sqb *Query) Pred[Owner]   { return c.Predicate(`%s <= %s`, sqb) }
func (c Col[Owner, T]) LikeSub(sqb *Query) Pred[Owner]  { return c.Predicate(`%s LIKE %s`, sqb) }
func (c Col[Owner, T]) NLikeSub(sqb *Query) Pred[Owner] { return c.Predicate(`%s NOT LIKE %s`, sqb) }
func (c Col[Owner, T]) InSub(sqb *Query) Pred[Owner]    { return c.Predicate(`%s IN %s`, sqb) }
func (c Col[Owner, T]) NInSub(sqb *Query) Pred[Owner]   { return c.Predicate(`%s NOT IN %s`, sqb) }
func (c Col[Owner, T]) ExistsSub(sqb *Query) Pred[Owner] {
	subquery, err := formatSubquery(sqb)
	if err != nil {
		return pred[Owner](Cond{buildErr: errors.Trace(err)})
	}

	return pred[Owner](rawCondition("EXISTS " + subquery))
}

func (c Col[Owner, T]) NExistsSub(sqb *Query) Pred[Owner] {
	subquery, err := formatSubquery(sqb)
	if err != nil {
		return pred[Owner](Cond{buildErr: errors.Trace(err)})
	}

	return pred[Owner](rawCondition("NOT EXISTS " + subquery))
}

func (c Col[Owner, T]) Unique(sqb *Query) Pred[Owner] {
	return pred[Owner](unsupportedSubqueryPredicate("UNIQUE"))
}

func (c Col[Owner, T]) NUnique(sqb *Query) Pred[Owner] {
	return pred[Owner](unsupportedSubqueryPredicate("NOT UNIQUE"))
}

// ================================================
// 条件构建核心方法
// ================================================

// Predicate builds a condition with the given operator and arguments
func (c Col[Owner, T]) Predicate(op string, args ...any) Pred[Owner] {
	if err := validatePredicateFormat(op, len(args)+1); err != nil {
		return pred[Owner](Cond{buildErr: errors.Trace(err)})
	}

	baseTable, err := validateColumnInput(c)
	if err != nil {
		return pred[Owner](Cond{buildErr: errors.Trace(err)})
	}

	tables := map[string]Table{baseTable.Table(): baseTable}

	// Collect tables from arguments that are also columns
	for _, arg := range args {
		if col, ok := arg.(AnyColumn); ok {
			table, err := validateColumnInput(col)
			if err != nil {
				return pred[Owner](Cond{buildErr: errors.Trace(err)})
			}

			tables[table.Table()] = table
		}
	}

	// Build arguments for string formatting
	formatArgs := make([]any, 0, len(args)+1)
	formatArgs = append(formatArgs, c.rawQualifiedName())

	for _, arg := range args {
		expr := argumentToExpression(arg)
		if err := expressionBuildError(expr); err != nil {
			return pred[Owner](Cond{buildErr: errors.Trace(err)})
		}

		formatArgs = append(formatArgs, expr.Expr())
	}

	return pred[Owner](Cond{
		tables: tables,
		expr:   fmt.Sprintf(op, formatArgs...),
		args:   collectExpressionArgs(args...),
	})
}

func (c Col[Owner, T]) rawCondition(expr string) Pred[Owner] {
	table, err := validateColumnInput(c)
	if err != nil {
		return pred[Owner](Cond{buildErr: errors.Trace(err)})
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
	return Cond{buildErr: errors.Errorf("%s subquery predicate is not supported by TSQ's built-in dialects", name)}
}

// unsupportedPatternPredicate returns a condition with a deferred error indicating
// that this pattern predicate is not portable across TSQ's built-in dialects.
// The error will be returned when Build() is called, not immediately.
// Users should use LIKE with an explicit pattern instead.
func unsupportedPatternPredicate(name string) Cond {
	return Cond{buildErr: errors.Errorf(
		"%s is not portable across TSQ's built-in dialects; use LIKE with an explicit pattern instead",
		name,
	)}
}

func validatePredicateFormat(op string, placeholderCount int) error {
	if strings.TrimSpace(op) == "" {
		return errors.Errorf("predicate format cannot be empty")
	}

	actual, err := countStringFormatPlaceholders(op)
	if err != nil {
		return errors.Trace(err)
	}

	if actual != placeholderCount {
		return errors.Errorf(
			"predicate format placeholder count mismatch: expected %d, got %d",
			placeholderCount,
			actual,
		)
	}

	return nil
}

// ================================================
// 表达式类型和辅助函数
// ================================================

// Expression interface for SQL expressions
type Expression interface {
	Expr() string
	Args() []any
}

type expressionError struct {
	err error
}

func (e expressionError) Expr() string { return "" }
func (e expressionError) Args() []any  { return nil }
func (e expressionError) buildError() error {
	return errors.Trace(e.err)
}

// variableExpression represents a variable placeholder (?)
type variableExpression struct{}

func (v variableExpression) Expr() string { return "?" }
func (v variableExpression) Args() []any  { return []any{externalArgMarker} }

var Var variableExpression

type variableSliceExpression struct{}

func (v variableSliceExpression) Expr() string { return "?" }
func (v variableSliceExpression) Args() []any  { return []any{externalSliceArgMarker{}} }

var VarSlice variableSliceExpression

// valuesExpression represents a list of values in SQL
type valuesExpression struct {
	expr string
	args []any
}

func (a valuesExpression) Expr() string {
	return a.expr
}

func (a valuesExpression) Args() []any {
	return append([]any(nil), a.args...)
}

func newValuesExpression(args any) Expression {
	v := reflect.ValueOf(args)
	if v.Kind() != reflect.Slice {
		return expressionError{err: errors.Errorf("expected slice, got %T", args)}
	}

	values := make([]string, v.Len())
	bindArgs := make([]any, v.Len())

	for i := range v.Len() {
		value := v.Index(i).Interface()
		if err := validatePredicateValue(value); err != nil {
			return expressionError{err: errors.Trace(err)}
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

func (r rawExpression) Expr() string { return r.expr }
func (r rawExpression) Args() []any  { return append([]any(nil), r.args...) }

func Bind(value any) Expression {
	if err := validatePredicateValue(value); err != nil {
		return expressionError{err: errors.Trace(err)}
	}

	return rawExpression{expr: "?", args: []any{value}}
}

func BindSlice(values any) Expression {
	return newValuesExpression(values)
}

func Literal(value any) Expression {
	expr, err := literalValue(value)
	if err != nil {
		return expressionError{err: errors.Trace(err)}
	}

	return rawExpression{expr: expr}
}

func literalValues(values any) Expression {
	v := reflect.ValueOf(values)
	if v.Kind() != reflect.Slice {
		return expressionError{err: errors.Errorf("expected slice, got %T", values)}
	}

	parts := make([]string, v.Len())
	for i := range v.Len() {
		part, err := literalValue(v.Index(i).Interface())
		if err != nil {
			return expressionError{err: errors.Trace(err)}
		}

		parts[i] = part
	}

	return rawExpression{expr: strings.Join(parts, ", ")}
}

// argumentToExpression converts various argument types to their SQL expression representation.
func argumentToExpression(arg any) Expression {
	switch v := arg.(type) {
	case Expression:
		return v
	case AnyColumn:
		return rawExpression{expr: rawColumnQualifiedName(v), args: expressionArgs(v)}
	case *Query:
		expr, err := formatSubquery(v)
		if err != nil {
			return expressionError{err: errors.Trace(err)}
		}

		return rawExpression{expr: expr, args: queryArgs(v)}
	default:
		return Literal(v)
	}
}

// formatSubquery formats a subquery for use in SQL
func formatSubquery(q *Query) (string, error) {
	if err := validateQuery(q); err != nil {
		return "", errors.Trace(err)
	}

	return fmt.Sprintf("(%s)", q.listSQL), nil
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

func expressionArgs(col AnyColumn) []any {
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
		return errors.Trace(carrier.buildError())
	}

	return nil
}

func validatePredicateValue(arg any) error {
	val, err := sqlValue(arg)
	if err != nil {
		return errors.Errorf("failed to convert value %v (%T): %v", arg, arg, err)
	}

	if val == sqlNullLiteral {
		return errors.New("null literal values are not supported in predicates; use IsNull/IsNotNull explicitly")
	}

	return nil
}

func literalValue(arg any) (string, error) {
	val, err := sqlValue(arg)
	if err != nil {
		return "", errors.Errorf("failed to convert value %v (%T): %v", arg, arg, err)
	}

	if val == sqlNullLiteral {
		return "", errors.New("null literal values are not supported in predicates; use IsNull/IsNotNull explicitly")
	}

	return val, nil
}

// sqlValue converts a Go value to its SQL string representation
// This function supports all standard SQL types and their Go equivalents
func sqlValue(arg any) (string, error) {
	if isNilValue(arg) {
		return sqlNullLiteral, nil
	}

	// Handle driver.Valuer interface (e.g., time.Time, sql.Null* types, custom types)
	if valuer, ok := arg.(driver.Valuer); ok {
		val, err := valuer.Value()
		if err != nil {
			return "", errors.Trace(err)
		}

		if val == nil {
			return sqlNullLiteral, nil
		}
		// Recursively handle the converted value
		res, err := sqlValue(val)

		return res, errors.Trace(err)
	}

	// Use reflection to handle pointers and get the underlying type
	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return sqlNullLiteral, nil
		}

		v = v.Elem()
		arg = v.Interface()
	}

	switch val := arg.(type) {
	case string:
		return sqlEscapeString(val)
	case []byte:
		return sqlEscapeString(string(val))
	case sql.RawBytes:
		return sqlEscapeString(string(val))

	// Integer types
	case int:
		return strconv.FormatInt(int64(val), 10), nil
	case int8:
		return strconv.FormatInt(int64(val), 10), nil
	case int16:
		return strconv.FormatInt(int64(val), 10), nil
	case int32:
		return strconv.FormatInt(int64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil

	// Unsigned integer types
	case uint:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint64:
		return strconv.FormatUint(val, 10), nil

	// Floating point types
	case float32:
		if math.IsNaN(float64(val)) {
			return "NULL", nil // NaN is treated as NULL in SQL
		}

		if math.IsInf(float64(val), 0) {
			return "NULL", nil // Infinity is treated as NULL in SQL
		}

		return strconv.FormatFloat(float64(val), 'g', -1, 32), nil
	case float64:
		if math.IsNaN(val) {
			return "NULL", nil // NaN is treated as NULL in SQL
		}

		if math.IsInf(val, 0) {
			return "NULL", nil // Infinity is treated as NULL in SQL
		}

		return strconv.FormatFloat(val, 'g', -1, 64), nil

	// Boolean type
	case bool:
		if val {
			return "TRUE", nil
		}

		return "FALSE", nil

	// Time type
	case time.Time:
		if val.IsZero() {
			return "NULL", nil
		}
		// Format as SQL standard datetime: 'YYYY-MM-DD HH:MM:SS'
		return fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05")), nil

	default:
		// Use reflection for other types
		return sqlValueReflect(v)
	}
}

// sqlEscapeString escapes a string for safe use in SQL
func sqlEscapeString(s string) (string, error) {
	if strings.Contains(s, `\`) {
		return "", errors.Errorf("string literals containing backslashes are not portable; use bind variables instead")
	}

	// Replace single quotes with double single quotes (SQL standard)
	escaped := strings.ReplaceAll(s, "'", "''")

	return fmt.Sprintf("'%s'", escaped), nil
}

// sqlValueReflect handles types using reflection
func sqlValueReflect(v reflect.Value) (string, error) {
	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// Handle []uint8 (same as []byte)
			bytes := v.Bytes()
			return sqlEscapeString(string(bytes))
		}

		return "", errors.Errorf("unsupported slice type: %v", v.Type())

	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// Handle [N]uint8 (byte arrays)
			bytes := make([]byte, v.Len())
			for i := range v.Len() {
				bytes[i] = byte(v.Index(i).Uint())
			}

			return sqlEscapeString(string(bytes))
		}

		return "", errors.Errorf("unsupported array type: %v", v.Type())

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil

	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "NULL", nil
		}

		return strconv.FormatFloat(f, 'g', -1, 64), nil

	case reflect.Bool:
		if v.Bool() {
			return "TRUE", nil
		}

		return "FALSE", nil

	case reflect.String:
		return sqlEscapeString(v.String())

	default:
		return "", errors.Errorf("unsupported value type: %v", v.Type())
	}
}
