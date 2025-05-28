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
)

// ================================================
// 逻辑组合条件
// ================================================

// And combines multiple conditions with AND logic
func And(conds ...Condition) Cond {
	tables := make(map[string]Table)
	clauses := make([]string, 0, len(conds))

	for _, c := range conds {
		for tn, t := range c.Tables() {
			tables[tn] = t
		}

		clauses = append(clauses, c.Clause())
	}

	return Cond{
		tables: tables,
		expr:   "(" + strings.Join(clauses, " AND ") + ")",
	}
}

// Or combines multiple conditions with OR logic
func Or(conds ...Condition) Cond {
	tables := make(map[string]Table)
	clauses := make([]string, 0, len(conds))

	for _, c := range conds {
		for tn, t := range c.Tables() {
			tables[tn] = t
		}

		clauses = append(clauses, c.Clause())
	}

	return Cond{
		tables: tables,
		expr:   "(" + strings.Join(clauses, " OR ") + ")",
	}
}

// ================================================
// 基础条件接口和结构体
// ================================================

// Condition interface for SQL conditions
type Condition interface {
	Tables() map[string]Table
	Clause() string
}

// Cond represents a SQL condition
type Cond struct {
	tables map[string]Table
	expr   string
}

func (c Cond) Tables() map[string]Table {
	return c.tables
}

func (c Cond) Clause() string {
	return c.expr
}

// ================================================
// 变量比较条件 (使用 ? 占位符)
// ================================================

func (c Col[T]) EQVar() Cond         { return c.Predicate(`%s = %s`, Var) }
func (c Col[T]) NEVar() Cond         { return c.Predicate(`%s <> %s`, Var) }
func (c Col[T]) GTVar() Cond         { return c.Predicate(`%s > %s`, Var) }
func (c Col[T]) GETVar() Cond        { return c.Predicate(`%s >= %s`, Var) }
func (c Col[T]) LTVar() Cond         { return c.Predicate(`%s < %s`, Var) }
func (c Col[T]) LETVar() Cond        { return c.Predicate(`%s <= %s`, Var) }
func (c Col[T]) StartWithVar() Cond  { return c.Predicate(`%s LIKE %%%s`, Var) }
func (c Col[T]) NStartWithVar() Cond { return c.Predicate(`%s NOT LIKE %%%s`, Var) }
func (c Col[T]) EndWithVar() Cond    { return c.Predicate(`%s LIKE %s%%`, Var) }
func (c Col[T]) NEndWithVar() Cond   { return c.Predicate(`%s NOT LIKE %s%%`, Var) }
func (c Col[T]) ContainsVar() Cond   { return c.Predicate(`%s LIKE %%%s%%`, Var) }
func (c Col[T]) NContainsVar() Cond  { return c.Predicate(`%s NOT LIKE %%%s%%`, Var) }
func (c Col[T]) BetweenVar() Cond    { return c.Predicate(`%s BETWEEN %s AND %s`, Var, Var) }
func (c Col[T]) NBetweenVar() Cond   { return c.Predicate(`%s NOT BETWEEN %s AND %s`, Var, Var) }

// ================================================
// 常量比较条件
// ================================================

func (c Col[T]) EQ(arg T) Cond              { return c.Predicate(`%s = %s`, arg) }
func (c Col[T]) NE(arg T) Cond              { return c.Predicate(`%s <> %s`, arg) }
func (c Col[T]) GT(arg T) Cond              { return c.Predicate(`%s > %s`, arg) }
func (c Col[T]) GTE(arg T) Cond             { return c.Predicate(`%s >= %s`, arg) }
func (c Col[T]) LT(arg T) Cond              { return c.Predicate(`%s < %s`, arg) }
func (c Col[T]) LTE(arg T) Cond             { return c.Predicate(`%s <= %s`, arg) }
func (c Col[T]) StartWith(str string) Cond  { return c.Predicate(`%s LIKE %%%s`, str) }
func (c Col[T]) NStartWith(str string) Cond { return c.Predicate(`%s NOT LIKE %%%s`, str) }
func (c Col[T]) EndWith(str string) Cond    { return c.Predicate(`%s LIKE %s%%`, str) }
func (c Col[T]) NEndWith(str string) Cond   { return c.Predicate(`%s NOT LIKE %s%%`, str) }
func (c Col[T]) Contains(str string) Cond   { return c.Predicate(`%s LIKE %%%s%%`, str) }
func (c Col[T]) NContains(str string) Cond  { return c.Predicate(`%s NOT LIKE %%%s%%`, str) }
func (c Col[T]) Between(start, end T) Cond  { return c.Predicate(`%s BETWEEN %s AND %s`, start, end) }
func (c Col[T]) NBetween(start, end T) Cond {
	return c.Predicate(`%s NOT BETWEEN %s AND %s`, start, end)
}
func (c Col[T]) In(args ...T) Cond  { return c.Predicate(`%s IN (%s)`, newValuesExpression(args)) }
func (c Col[T]) NIn(args ...T) Cond { return c.Predicate(`%s NOT IN (%s)`, newValuesExpression(args)) }
func (c Col[T]) IsNull() Cond       { return c.Predicate(`%s IS NULL`) }
func (c Col[T]) IsNotNull() Cond    { return c.Predicate(`%s IS NOT NULL`) }

// ================================================
// 字段比较条件
// ================================================

func (c Col[T]) EQCol(other Col[T]) Cond  { return c.Predicate(`%s = %s`, other) }
func (c Col[T]) NECol(other Col[T]) Cond  { return c.Predicate(`%s <> %s`, other) }
func (c Col[T]) GTCol(other Col[T]) Cond  { return c.Predicate(`%s > %s`, other) }
func (c Col[T]) GTECol(other Col[T]) Cond { return c.Predicate(`%s >= %s`, other) }
func (c Col[T]) LTCol(other Col[T]) Cond  { return c.Predicate(`%s < %s`, other) }
func (c Col[T]) LTECol(other Col[T]) Cond { return c.Predicate(`%s <= %s`, other) }
func (c Col[T]) StartWithCol(other Col[T]) Cond {
	return c.Predicate(`%s LIKE CONCAT('%%', %s)`, other)
}

func (c Col[T]) NStartWithCol(other Col[T]) Cond {
	return c.Predicate(`%s NOT LIKE CONCAT(%s, '%%')`, other)
}
func (c Col[T]) EndWithCol(other Col[T]) Cond { return c.Predicate(`%s LIKE CONCAT('%%', %s)`, other) }
func (c Col[T]) NEndWithCol(other Col[T]) Cond {
	return c.Predicate(`%s NOT LIKE CONCAT(%s, '%%')`, other)
}

func (c Col[T]) ContainsCol(other Col[T]) Cond {
	return c.Predicate(`%s LIKE CONCAT('%%', %s, '%%')`, other)
}

func (c Col[T]) NContainsCol(other Col[T]) Cond {
	return c.Predicate(`%s NOT LIKE CONCAT('%%', %s, '%%')`, other)
}

// ================================================
// 子查询条件
// ================================================

func (c Col[T]) EQSub(sqb *Query) Cond      { return c.Predicate(`%s = %s`, sqb) }
func (c Col[T]) NESub(sqb *Query) Cond      { return c.Predicate(`%s <> %s`, sqb) }
func (c Col[T]) GTSub(sqb *Query) Cond      { return c.Predicate(`%s > %s`, sqb) }
func (c Col[T]) GESub(sqb *Query) Cond      { return c.Predicate(`%s >= %s`, sqb) }
func (c Col[T]) LTSub(sqb *Query) Cond      { return c.Predicate(`%s < %s`, sqb) }
func (c Col[T]) LESub(sqb *Query) Cond      { return c.Predicate(`%s <= %s`, sqb) }
func (c Col[T]) LikeSub(sqb *Query) Cond    { return c.Predicate(`%s LIKE %s`, sqb) }
func (c Col[T]) NLikeSub(sqb *Query) Cond   { return c.Predicate(`%s NOT LIKE %s`, sqb) }
func (c Col[T]) InSub(sqb *Query) Cond      { return c.Predicate(`%s IN %s`, sqb) }
func (c Col[T]) NInSub(sqb *Query) Cond     { return c.Predicate(`%s NOT IN %s`, sqb) }
func (c Col[T]) ExistsSub(sqb *Query) Cond  { return c.Predicate(`%s EXISTS %s`, sqb) }
func (c Col[T]) NExistsSub(sqb *Query) Cond { return c.Predicate(`%s NOT EXISTS %s`, sqb) }
func (c Col[T]) Unique(sqb *Query) Cond     { return c.Predicate(`%s UNIQUE %s`, sqb) }
func (c Col[T]) NUnique(sqb *Query) Cond    { return c.Predicate(`%s NOT UNIQUE %s`, sqb) }

// ================================================
// 条件构建核心方法
// ================================================

// Predicate builds a condition with the given operator and arguments
func (c Col[T]) Predicate(op string, args ...any) Cond {
	tables := map[string]Table{c.table.Table(): c.table}

	// Collect tables from arguments that are also columns
	for _, arg := range args {
		if col, ok := arg.(Column); ok {
			tables[col.Table().Table()] = col.Table()
		}
	}

	// Build arguments for string formatting
	formatArgs := make([]any, 0, len(args)+1)
	formatArgs = append(formatArgs, c.QualifiedName())

	for _, arg := range args {
		formatArgs = append(formatArgs, argumentToString(arg))
	}

	return Cond{
		tables: tables,
		expr:   fmt.Sprintf(op, formatArgs...),
	}
}

// ================================================
// 表达式类型和辅助函数
// ================================================

// Expression interface for SQL expressions
type Expression interface {
	Expr() string
}

// variableExpression represents a variable placeholder (?)
type variableExpression struct{}

func (v variableExpression) Expr() string { return "?" }

var Var variableExpression

// valuesExpression represents a list of values in SQL
type valuesExpression struct {
	expr string
}

func (a valuesExpression) Expr() string {
	return a.expr
}

func newValuesExpression(args any) valuesExpression {
	v := reflect.ValueOf(args)
	if v.Kind() != reflect.Slice {
		panic(fmt.Sprintf("expected slice, got %T", args))
	}

	values := make([]string, v.Len())
	for i := range v.Len() {
		values[i] = valueOrPanic(v.Index(i).Interface())
	}

	return valuesExpression{
		expr: strings.Join(values, ", "),
	}
}

// argumentToString converts various argument types to their SQL string representation
func argumentToString(arg any) string {
	switch v := arg.(type) {
	case Expression:
		return v.Expr()
	case Column:
		return v.QualifiedName()
	case *Query:
		return formatSubquery(v)
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	default:
		return valueOrPanic(arg)
	}
}

// formatSubquery formats a subquery for use in SQL
func formatSubquery(q *Query) string {
	if q == nil {
		panic("subquery cannot be nil")
	}

	return fmt.Sprintf("(%s)", q.ListSQL())
}

// valueOrPanic converts a value to its SQL representation or panics
func valueOrPanic(arg any) string {
	val, err := sqlValue(arg)
	if err != nil {
		panic(fmt.Sprintf("failed to convert value %v (%T): %v", arg, arg, err))
	}

	return val
}

// sqlValue converts a Go value to its SQL string representation
// This function supports all standard SQL types and their Go equivalents
func sqlValue(arg any) (string, error) {
	if arg == nil {
		return "NULL", nil
	}

	// Handle driver.Valuer interface (e.g., time.Time, sql.Null* types, custom types)
	if valuer, ok := arg.(driver.Valuer); ok {
		val, err := valuer.Value()
		if err != nil {
			return "", err
		}

		if val == nil {
			return "NULL", nil
		}
		// Recursively handle the converted value
		return sqlValue(val)
	}

	// Use reflection to handle pointers and get the underlying type
	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return "NULL", nil
		}

		v = v.Elem()
		arg = v.Interface()
	}

	switch val := arg.(type) {
	case string:
		return sqlEscapeString(val), nil
	case []byte:
		return sqlEscapeString(string(val)), nil
	case sql.RawBytes:
		return sqlEscapeString(string(val)), nil

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
func sqlEscapeString(s string) string {
	// Replace single quotes with double single quotes (SQL standard)
	escaped := strings.ReplaceAll(s, "'", "''")
	// Escape backslashes for databases that require it
	escaped = strings.ReplaceAll(escaped, "\\", "\\\\")

	return fmt.Sprintf("'%s'", escaped)
}

// sqlValueReflect handles types using reflection
func sqlValueReflect(v reflect.Value) (string, error) {
	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// Handle []uint8 (same as []byte)
			bytes := v.Bytes()
			return sqlEscapeString(string(bytes)), nil
		}

		return "", fmt.Errorf("unsupported slice type: %v", v.Type())

	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// Handle [N]uint8 (byte arrays)
			bytes := make([]byte, v.Len())
			for i := range v.Len() {
				bytes[i] = byte(v.Index(i).Uint())
			}

			return sqlEscapeString(string(bytes)), nil
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
		return sqlEscapeString(v.String()), nil

	default:
		// For other types, try to convert to string as fallback
		return sqlEscapeString(fmt.Sprintf("%v", v.Interface())), nil
	}
}
