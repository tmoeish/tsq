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

// ================================================
// 逻辑组合条件
// ================================================

// And 将多个条件以 AND 逻辑组合。
// 架构意图：And 实现了条件的链式或组合式构建。它通过遍历子条件，
// 合并所有引用的表，并将表达式片段用 " AND " 连接。
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

// Or 将多个条件以 OR 逻辑组合。
// 架构意图：逻辑与 And 类似，但使用 " OR " 连接。
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

// ================================================
// 基础条件接口和结构体
// ================================================

// Condition 是 SQL 条件的接口定义。
// 它暴露了条件引用的表、SQL 字句片段以及绑定的参数。
type Condition interface {
	Tables() map[string]Table
	Clause() string
	Args() []any
}

type rawConditionClauser interface {
	rawClause() string
}

// Cond 是 SQL 条件的具体实现结构。
// 架构意图：它是构建过程中携带元数据（表引用、表达式字符串、参数、构建错误）的容器。
// expr 字段存储了带有占位符的原始 SQL 表达式，args 存储了对应的参数。
type Cond struct {
	tables   map[string]Table
	expr     string
	args     []any
	buildErr error
}

func pred[O Owner](cond Cond) Pred[O] {
	return Pred[O]{Cond: cond}
}

// Tables returns the tables referenced by the condition, keyed by logical table name.
func (c Cond) Tables() map[string]Table {
	return cloneTableMap(c.tables)
}

// Clause 返回标准化的 SQL 片段。
// 注意：此时可能包含标记占位符（如 identifierMarkerPrefix），最终的渲染由 sql_render.go 处理。
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
//   - EQVar / InVar 等方法：使用执行时占位符，值由执行时提供
//   - Col: 与另一列比较
//   - Sub: 与子查询结果比较

// ================================================
// 变量比较条件 (使用 ? 占位符)
// ================================================

func (c ColumnImpl[Owner, T]) EQVar() Pred[Owner]  { return c.Predicate(`%s = %s`, varMarker) }
func (c ColumnImpl[Owner, T]) NEVar() Pred[Owner]  { return c.Predicate(`%s <> %s`, varMarker) }
func (c ColumnImpl[Owner, T]) GTVar() Pred[Owner]  { return c.Predicate(`%s > %s`, varMarker) }
func (c ColumnImpl[Owner, T]) GTEVar() Pred[Owner] { return c.Predicate(`%s >= %s`, varMarker) }
func (c ColumnImpl[Owner, T]) LTVar() Pred[Owner]  { return c.Predicate(`%s < %s`, varMarker) }
func (c ColumnImpl[Owner, T]) LTEVar() Pred[Owner] { return c.Predicate(`%s <= %s`, varMarker) }

// InVar binds a slice at execution time for IN predicates.
//
// TSQ intentionally treats nil and empty slices as an explicit "match nothing"
// filter. During execution the placeholder list is expanded from the runtime
// argument slice; when that slice is empty, TSQ renders IN (NULL), which keeps
// the query valid while producing zero matches across the supported built-in
// dialects. This is by design and lets callers express "no selected IDs" without
// adding custom branching around the query.
func (c ColumnImpl[Owner, T]) InVar() Pred[Owner] { return c.Predicate(`%s IN (%s)`, varSliceMarker) }

func (c ColumnImpl[Owner, T]) StartsWithVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("StartsWithVar"))
}

func (c ColumnImpl[Owner, T]) NStartsWithVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NStartsWithVar"))
}

func (c ColumnImpl[Owner, T]) EndsWithVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("EndsWithVar"))
}

func (c ColumnImpl[Owner, T]) NEndsWithVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NEndsWithVar"))
}

func (c ColumnImpl[Owner, T]) ContainsVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("ContainsVar"))
}

func (c ColumnImpl[Owner, T]) NContainsVar() Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NContainsVar"))
}

func (c ColumnImpl[Owner, T]) BetweenVar() Pred[Owner] {
	return c.Predicate(`%s BETWEEN %s AND %s`, varMarker, varMarker)
}

func (c ColumnImpl[Owner, T]) NBetweenVar() Pred[Owner] {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, varMarker, varMarker)
}

// ================================================
// 常量比较条件
// ================================================

func (c ColumnImpl[Owner, T]) EQ(arg T) Pred[Owner] {
	return c.Predicate(`%s = %s`, Bind(arg))
}

func (c ColumnImpl[Owner, T]) NE(arg T) Pred[Owner] {
	return c.Predicate(`%s <> %s`, Bind(arg))
}

func (c ColumnImpl[Owner, T]) GT(arg T) Pred[Owner] {
	return c.Predicate(`%s > %s`, Bind(arg))
}

func (c ColumnImpl[Owner, T]) GTE(arg T) Pred[Owner] {
	return c.Predicate(`%s >= %s`, Bind(arg))
}

func (c ColumnImpl[Owner, T]) LT(arg T) Pred[Owner] {
	return c.Predicate(`%s < %s`, Bind(arg))
}

func (c ColumnImpl[Owner, T]) LTE(arg T) Pred[Owner] {
	return c.Predicate(`%s <= %s`, Bind(arg))
}

func (c ColumnImpl[Owner, T]) StartsWith(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind(str+"%"))
}

func (c ColumnImpl[Owner, T]) NStartsWith(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind(str+"%"))
}

func (c ColumnImpl[Owner, T]) EndsWith(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind("%"+str))
}

func (c ColumnImpl[Owner, T]) NEndsWith(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind("%"+str))
}

func (c ColumnImpl[Owner, T]) Contains(str string) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, Bind("%"+str+"%"))
}

func (c ColumnImpl[Owner, T]) NContains(str string) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, Bind("%"+str+"%"))
}

func (c ColumnImpl[Owner, T]) Between(start, end T) Pred[Owner] {
	return c.Predicate(`%s BETWEEN %s AND %s`, Bind(start), Bind(end))
}

func (c ColumnImpl[Owner, T]) NBetween(start, end T) Pred[Owner] {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, Bind(start), Bind(end))
}

func (c ColumnImpl[Owner, T]) In(args ...T) Pred[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 0"))
	}

	return c.Predicate(`%s IN (%s)`, BindSlice(args))
}

func (c ColumnImpl[Owner, T]) NIn(args ...T) Pred[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 1"))
	}

	return c.Predicate(`%s NOT IN (%s)`, BindSlice(args))
}

func (c ColumnImpl[Owner, T]) IsNull() Pred[Owner] {
	return c.Predicate(`%s IS NULL`)
}

func (c ColumnImpl[Owner, T]) IsNotNull() Pred[Owner] {
	return c.Predicate(`%s IS NOT NULL`)
}

// ================================================
// 字段比较条件
// ================================================

func (c ColumnImpl[Owner, T]) EQCol(other typedColumnInternal[T]) Pred[Owner] {
	return c.Predicate(`%s = %s`, other)
}

func (c ColumnImpl[Owner, T]) NECol(other typedColumnInternal[T]) Pred[Owner] {
	return c.Predicate(`%s <> %s`, other)
}

func (c ColumnImpl[Owner, T]) GTCol(other typedColumnInternal[T]) Pred[Owner] {
	return c.Predicate(`%s > %s`, other)
}

func (c ColumnImpl[Owner, T]) GTECol(other typedColumnInternal[T]) Pred[Owner] {
	return c.Predicate(`%s >= %s`, other)
}

func (c ColumnImpl[Owner, T]) LTCol(other typedColumnInternal[T]) Pred[Owner] {
	return c.Predicate(`%s < %s`, other)
}

func (c ColumnImpl[Owner, T]) LTECol(other typedColumnInternal[T]) Pred[Owner] {
	return c.Predicate(`%s <= %s`, other)
}

func (c ColumnImpl[Owner, T]) StartsWithCol(_ typedColumnInternal[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("StartsWithCol"))
}

func (c ColumnImpl[Owner, T]) NStartsWithCol(_ typedColumnInternal[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NStartsWithCol"))
}

func (c ColumnImpl[Owner, T]) EndsWithCol(_ typedColumnInternal[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("EndsWithCol"))
}

func (c ColumnImpl[Owner, T]) NEndsWithCol(_ typedColumnInternal[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NEndsWithCol"))
}

func (c ColumnImpl[Owner, T]) ContainsCol(_ typedColumnInternal[T]) Pred[Owner] {
	return pred[Owner](unsupportedPatternPredicate("ContainsCol"))
}

func (c ColumnImpl[Owner, T]) NContainsCol(_ typedColumnInternal[T]) Pred[Owner] {
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

func (c ColumnImpl[Owner, T]) EQSub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s = %s`, scalarSubquery(sq))
}

func (c ColumnImpl[Owner, T]) NESub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s <> %s`, scalarSubquery(sq))
}

func (c ColumnImpl[Owner, T]) GTSub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s > %s`, scalarSubquery(sq))
}

func (c ColumnImpl[Owner, T]) GTESub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s >= %s`, scalarSubquery(sq))
}

func (c ColumnImpl[Owner, T]) LTSub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s < %s`, scalarSubquery(sq))
}

func (c ColumnImpl[Owner, T]) LTESub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s <= %s`, scalarSubquery(sq))
}

func (c ColumnImpl[Owner, T]) LikeSub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s LIKE %s`, scalarSubquery(sq))
}

func (c ColumnImpl[Owner, T]) NLikeSub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s NOT LIKE %s`, scalarSubquery(sq))
}

func (c ColumnImpl[Owner, T]) InSub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s IN %s`, membershipSubquery(sq))
}

func (c ColumnImpl[Owner, T]) NInSub(sq subquery) Pred[Owner] {
	return c.Predicate(`%s NOT IN %s`, membershipSubquery(sq))
}

func (c ColumnImpl[Owner, T]) ExistsSub(sq subquery) Pred[Owner] {
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

func (c ColumnImpl[Owner, T]) NExistsSub(sq subquery) Pred[Owner] {
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

func (c ColumnImpl[Owner, T]) Unique(_ subquery) Pred[Owner] {
	return pred[Owner](unsupportedSubqueryPredicate("UNIQUE"))
}

func (c ColumnImpl[Owner, T]) NUnique(_ subquery) Pred[Owner] {
	return pred[Owner](unsupportedSubqueryPredicate("NOT UNIQUE"))
}

// ================================================
// 条件构建核心方法
// ================================================

// Predicate 是构建条件的核心方法。
// 架构意图：它是所有比较操作（EQ, GT, In 等）的基础。它接收一个格式化字符串和变长参数。
// 1. 验证占位符数量。
// 2. 自动收集所有涉及到的表（包括接收者列和参数中的列）。
// 3. 将参数转换为 SQL 表达式。
// 4. 使用 fmt.Sprintf 组合成最终的表达式字符串。
func (c ColumnImpl[Owner, T]) Predicate(op string, args ...any) Pred[Owner] {
	if err := validatePredicateFormat(op, len(args)+1); err != nil {
		return pred[Owner](Cond{buildErr: err})
	}

	baseTable, err := validateColumnInput(c)
	if err != nil {
		return pred[Owner](Cond{buildErr: err})
	}

	tables := map[string]Table{baseTable.Table(): baseTable}

	// 从也是列的参数中收集表引用。
	for _, arg := range args {
		if col, ok := arg.(SQLColumn); ok {
			table, err := validateColumnInput(col)
			if err != nil {
				return pred[Owner](Cond{buildErr: err})
			}

			tables[table.Table()] = table
		}
	}

	// 构建用于字符串格式化的参数。
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

func (c ColumnImpl[Owner, T]) rawCondition(expr string) Pred[Owner] {
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

// ================================================
// 表达式类型和辅助函数
// ================================================

// Expression represents a SQL fragment plus the args needed to render it safely.
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

// variableExpression 代表一个变量占位符 (?)。
// 它在 Args() 中返回 externalArgMarker，这是一个特殊的“延迟解析”标记。
// 架构意图：当用户使用 EQVar() 等方法时，TSQ 并不立即绑定值，而是留下这个标记。
// 真正的参数值将在 Query.List(ctx, db, value) 调用时，由 resolveQuery 函数根据此标记进行对齐。
type variableExpression struct{}

func (v variableExpression) Expr() string { return "?" }
func (v variableExpression) Args() []any  { return []any{externalArgMarker} }

var varMarker variableExpression

// variableSliceExpression 代表一个切片变量占位符。
// 架构意图：专门用于 IN 比较。在构建期，我们不知道切片的长度，因此无法确定生成多少个 "?"。
// 它返回 externalSliceArgMarker{}，resolveQuery 在执行时会检测此标记，
// 并根据传入切片的实际长度动态扩展 SQL 中的 "?" 占位符数量。
type variableSliceExpression struct{}

func (v variableSliceExpression) Expr() string { return "?" }
func (v variableSliceExpression) Args() []any  { return []any{externalSliceArgMarker{}} }

var varSliceMarker variableSliceExpression

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

func (r rawExpression) Expr() string { return r.expr }
func (r rawExpression) Args() []any  { return append([]any(nil), r.args...) }

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
