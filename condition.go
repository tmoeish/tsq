package tsq

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
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

		for tn, t := range condTables {
			tables[tn] = t
		}

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

		for tn, t := range condTables {
			tables[tn] = t
		}

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
//   - Column: 接收者 (implicit)
//   - Operator: 方法名（EQ, GT, StartWith 等）
//   - Values: 参数（value1, value2, ...)
//
// 约定示例：
//   col.EQ(value)              // column = value
//   col.Between(start, end)    // column BETWEEN start AND end
//   col.In(v1, v2, v3)        // column IN (v1, v2, v3)
//   col.StartWith(prefix)      // column LIKE 'prefix%'
//
// 方法分类：
//   - 基础比较: EQ, NE, GT, GTE, LT, LTE
//   - 模式匹配: StartWith, EndWith, Contains
//   - 集合: In, NIn
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

func (c Col[T]) EQVar() Cond         { return c.Predicate(`%s = %s`, Var) }
func (c Col[T]) NEVar() Cond         { return c.Predicate(`%s <> %s`, Var) }
func (c Col[T]) GTVar() Cond         { return c.Predicate(`%s > %s`, Var) }
func (c Col[T]) GETVar() Cond        { return c.Predicate(`%s >= %s`, Var) }
func (c Col[T]) LTVar() Cond         { return c.Predicate(`%s < %s`, Var) }
func (c Col[T]) LETVar() Cond        { return c.Predicate(`%s <= %s`, Var) }
func (c Col[T]) StartWithVar() Cond  { return unsupportedPatternPredicate("StartWithVar") }
func (c Col[T]) NStartWithVar() Cond { return unsupportedPatternPredicate("NStartWithVar") }
func (c Col[T]) EndWithVar() Cond    { return unsupportedPatternPredicate("EndWithVar") }
func (c Col[T]) NEndWithVar() Cond   { return unsupportedPatternPredicate("NEndWithVar") }
func (c Col[T]) ContainsVar() Cond   { return unsupportedPatternPredicate("ContainsVar") }
func (c Col[T]) NContainsVar() Cond  { return unsupportedPatternPredicate("NContainsVar") }
func (c Col[T]) BetweenVar() Cond    { return c.Predicate(`%s BETWEEN %s AND %s`, Var, Var) }
func (c Col[T]) NBetweenVar() Cond   { return c.Predicate(`%s NOT BETWEEN %s AND %s`, Var, Var) }

// ================================================
// 常量比较条件
// ================================================

func (c Col[T]) EQ(arg T) Cond              { return c.Predicate(`%s = %s`, Bind(arg)) }
func (c Col[T]) NE(arg T) Cond              { return c.Predicate(`%s <> %s`, Bind(arg)) }
func (c Col[T]) GT(arg T) Cond              { return c.Predicate(`%s > %s`, Bind(arg)) }
func (c Col[T]) GTE(arg T) Cond             { return c.Predicate(`%s >= %s`, Bind(arg)) }
func (c Col[T]) LT(arg T) Cond              { return c.Predicate(`%s < %s`, Bind(arg)) }
func (c Col[T]) LTE(arg T) Cond             { return c.Predicate(`%s <= %s`, Bind(arg)) }
func (c Col[T]) StartWith(str string) Cond  { return c.Predicate(`%s LIKE %s`, Bind(str+"%")) }
func (c Col[T]) NStartWith(str string) Cond { return c.Predicate(`%s NOT LIKE %s`, Bind(str+"%")) }
func (c Col[T]) EndWith(str string) Cond    { return c.Predicate(`%s LIKE %s`, Bind("%"+str)) }
func (c Col[T]) NEndWith(str string) Cond   { return c.Predicate(`%s NOT LIKE %s`, Bind("%"+str)) }
func (c Col[T]) Contains(str string) Cond   { return c.Predicate(`%s LIKE %s`, Bind("%"+str+"%")) }
func (c Col[T]) NContains(str string) Cond  { return c.Predicate(`%s NOT LIKE %s`, Bind("%"+str+"%")) }
func (c Col[T]) Between(start, end T) Cond {
	return c.Predicate(`%s BETWEEN %s AND %s`, Bind(start), Bind(end))
}

func (c Col[T]) NBetween(start, end T) Cond {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, Bind(start), Bind(end))
}

func (c Col[T]) EQLiteral(arg T) Cond             { return c.Predicate(`%s = %s`, Literal(arg)) }
func (c Col[T]) NELiteral(arg T) Cond             { return c.Predicate(`%s <> %s`, Literal(arg)) }
func (c Col[T]) GTLiteral(arg T) Cond             { return c.Predicate(`%s > %s`, Literal(arg)) }
func (c Col[T]) GTELiteral(arg T) Cond            { return c.Predicate(`%s >= %s`, Literal(arg)) }
func (c Col[T]) LTLiteral(arg T) Cond             { return c.Predicate(`%s < %s`, Literal(arg)) }
func (c Col[T]) LTELiteral(arg T) Cond            { return c.Predicate(`%s <= %s`, Literal(arg)) }
func (c Col[T]) StartWithLiteral(str string) Cond { return c.Predicate(`%s LIKE %s`, Literal(str+"%")) }
func (c Col[T]) NStartWithLiteral(str string) Cond {
	return c.Predicate(`%s NOT LIKE %s`, Literal(str+"%"))
}
func (c Col[T]) EndWithLiteral(str string) Cond { return c.Predicate(`%s LIKE %s`, Literal("%"+str)) }
func (c Col[T]) NEndWithLiteral(str string) Cond {
	return c.Predicate(`%s NOT LIKE %s`, Literal("%"+str))
}

func (c Col[T]) ContainsLiteral(str string) Cond {
	return c.Predicate(`%s LIKE %s`, Literal("%"+str+"%"))
}

func (c Col[T]) NContainsLiteral(str string) Cond {
	return c.Predicate(`%s NOT LIKE %s`, Literal("%"+str+"%"))
}

func (c Col[T]) BetweenLiteral(start, end T) Cond {
	return c.Predicate(`%s BETWEEN %s AND %s`, Literal(start), Literal(end))
}

func (c Col[T]) NBetweenLiteral(start, end T) Cond {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, Literal(start), Literal(end))
}

func (c Col[T]) In(args ...T) Cond {
	if len(args) == 0 {
		return rawCondition("1 = 0")
	}

	return c.Predicate(`%s IN (%s)`, BindSlice(args))
}

func (c Col[T]) NIn(args ...T) Cond {
	if len(args) == 0 {
		return rawCondition("1 = 1")
	}

	return c.Predicate(`%s NOT IN (%s)`, BindSlice(args))
}

func (c Col[T]) InLiteral(args ...T) Cond {
	if len(args) == 0 {
		return rawCondition("1 = 0")
	}

	return c.Predicate(`%s IN (%s)`, literalValues(args))
}

func (c Col[T]) NInLiteral(args ...T) Cond {
	if len(args) == 0 {
		return rawCondition("1 = 1")
	}

	return c.Predicate(`%s NOT IN (%s)`, literalValues(args))
}
func (c Col[T]) IsNull() Cond    { return c.Predicate(`%s IS NULL`) }
func (c Col[T]) IsNotNull() Cond { return c.Predicate(`%s IS NOT NULL`) }

// ================================================
// 字段比较条件
// ================================================

func (c Col[T]) EQCol(other Col[T]) Cond    { return c.Predicate(`%s = %s`, other) }
func (c Col[T]) NECol(other Col[T]) Cond    { return c.Predicate(`%s <> %s`, other) }
func (c Col[T]) GTCol(other Col[T]) Cond    { return c.Predicate(`%s > %s`, other) }
func (c Col[T]) GTECol(other Col[T]) Cond   { return c.Predicate(`%s >= %s`, other) }
func (c Col[T]) LTCol(other Col[T]) Cond    { return c.Predicate(`%s < %s`, other) }
func (c Col[T]) LTECol(other Col[T]) Cond   { return c.Predicate(`%s <= %s`, other) }
func (c Col[T]) StartWithCol(_ Col[T]) Cond { return unsupportedPatternPredicate("StartWithCol") }
func (c Col[T]) NStartWithCol(_ Col[T]) Cond {
	return unsupportedPatternPredicate("NStartWithCol")
}
func (c Col[T]) EndWithCol(_ Col[T]) Cond   { return unsupportedPatternPredicate("EndWithCol") }
func (c Col[T]) NEndWithCol(_ Col[T]) Cond  { return unsupportedPatternPredicate("NEndWithCol") }
func (c Col[T]) ContainsCol(_ Col[T]) Cond  { return unsupportedPatternPredicate("ContainsCol") }
func (c Col[T]) NContainsCol(_ Col[T]) Cond { return unsupportedPatternPredicate("NContainsCol") }

// ================================================
// 子查询条件
// ================================================

func (c Col[T]) EQSub(sqb *Query) Cond    { return c.Predicate(`%s = %s`, sqb) }
func (c Col[T]) NESub(sqb *Query) Cond    { return c.Predicate(`%s <> %s`, sqb) }
func (c Col[T]) GTSub(sqb *Query) Cond    { return c.Predicate(`%s > %s`, sqb) }
func (c Col[T]) GESub(sqb *Query) Cond    { return c.Predicate(`%s >= %s`, sqb) }
func (c Col[T]) LTSub(sqb *Query) Cond    { return c.Predicate(`%s < %s`, sqb) }
func (c Col[T]) LESub(sqb *Query) Cond    { return c.Predicate(`%s <= %s`, sqb) }
func (c Col[T]) LikeSub(sqb *Query) Cond  { return c.Predicate(`%s LIKE %s`, sqb) }
func (c Col[T]) NLikeSub(sqb *Query) Cond { return c.Predicate(`%s NOT LIKE %s`, sqb) }
func (c Col[T]) InSub(sqb *Query) Cond    { return c.Predicate(`%s IN %s`, sqb) }
func (c Col[T]) NInSub(sqb *Query) Cond   { return c.Predicate(`%s NOT IN %s`, sqb) }
func (c Col[T]) ExistsSub(sqb *Query) Cond {
	subquery, err := formatSubquery(sqb)
	if err != nil {
		return Cond{buildErr: errors.Trace(err)}
	}

	return rawCondition("EXISTS " + subquery)
}

func (c Col[T]) NExistsSub(sqb *Query) Cond {
	subquery, err := formatSubquery(sqb)
	if err != nil {
		return Cond{buildErr: errors.Trace(err)}
	}

	return rawCondition("NOT EXISTS " + subquery)
}
func (c Col[T]) Unique(sqb *Query) Cond  { return unsupportedSubqueryPredicate("UNIQUE") }
func (c Col[T]) NUnique(sqb *Query) Cond { return unsupportedSubqueryPredicate("NOT UNIQUE") }

// ================================================
// 条件构建核心方法
// ================================================

// Predicate builds a condition with the given operator and arguments
func (c Col[T]) Predicate(op string, args ...any) Cond {
	if err := validatePredicateFormat(op, len(args)+1); err != nil {
		return Cond{buildErr: errors.Trace(err)}
	}

	baseTable, err := validateColumnInput(c)
	if err != nil {
		return Cond{buildErr: errors.Trace(err)}
	}

	tables := map[string]Table{baseTable.Table(): baseTable}

	// Collect tables from arguments that are also columns
	for _, arg := range args {
		if col, ok := arg.(Column); ok {
			table, err := validateColumnInput(col)
			if err != nil {
				return Cond{buildErr: errors.Trace(err)}
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
			return Cond{buildErr: errors.Trace(err)}
		}

		formatArgs = append(formatArgs, expr.Expr())
	}

	return Cond{
		tables: tables,
		expr:   fmt.Sprintf(op, formatArgs...),
		args:   collectExpressionArgs(args...),
	}
}

func (c Col[T]) rawCondition(expr string) Cond {
	table, err := validateColumnInput(c)
	if err != nil {
		return Cond{buildErr: errors.Trace(err)}
	}

	return Cond{
		tables: map[string]Table{table.Table(): table},
		expr:   expr,
	}
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
	return e.err
}

// variableExpression represents a variable placeholder (?)
type variableExpression struct{}

func (v variableExpression) Expr() string { return "?" }
func (v variableExpression) Args() []any  { return []any{externalArgMarker} }

var Var variableExpression

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
	case Column:
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

func expressionArgs(col Column) []any {
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
			return "", err
		}

		if val == nil {
			return sqlNullLiteral, nil
		}
		// Recursively handle the converted value
		return sqlValue(val)
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
		return "", fmt.Errorf("string literals containing backslashes are not portable; use bind variables instead")
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

		return "", fmt.Errorf("unsupported slice type: %v", v.Type())

	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// Handle [N]uint8 (byte arrays)
			bytes := make([]byte, v.Len())
			for i := range v.Len() {
				bytes[i] = byte(v.Index(i).Uint())
			}

			return sqlEscapeString(string(bytes))
		}

		return "", fmt.Errorf("unsupported array type: %v", v.Type())

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
		return "", fmt.Errorf("unsupported value type: %v", v.Type())
	}
}
