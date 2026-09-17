package tsq

import (
	"errors"
	"fmt"
	"strings"
)

// SQLColumn is any selectable expression: a table column, an expression derived
// from one, a CASE, or a result projection. Its methods beyond these are internal,
// so only TSQ constructs it.
type SQLColumn interface {
	// Name returns the physical column name, or for a derived expression the name of
	// the column it was derived from.
	Name() string
	// Table returns the table the expression primarily belongs to.
	Table() Table
	// JSONFieldName returns the field name PageRequest.OrderBy may use to sort by it.
	JSONFieldName() string

	core() *columnCore
}

// BoundColumn is a selectable expression that scans into O.
type BoundColumn[O any] interface {
	SQLColumn
	boundTo(O)
}

// TypedColumn is a selectable expression that scans into O and holds a T.
type TypedColumn[O, T any] interface {
	BoundColumn[O]
	valueOf(T)
}

// ValueColumn is an expression that holds a T, whatever it scans into.
type ValueColumn[T any] interface {
	SQLColumn
	valueOf(T)
}

// ResultColumn is a result projection: it scans into O and holds a T, but it is not
// a table column, so it offers no predicates of its own.
type ResultColumn[O, T any] interface {
	TypedColumn[O, T]
}

// SearchColumn is a text column keyword search may match against; make one with
// Searchable.
type SearchColumn interface {
	SQLColumn
	searchable()
}

// RHS is the right-hand side of a comparison against a T: a column or expression
// holding a T, a Param[T], or a typed scalar Subquery[T]. Values use the *Val methods.
type RHS[T any] interface {
	rhsValue(T)
	operand() exprInfo
}

// SetRHS is the right-hand side of IN and NOT IN over T: a ListParam[T] or a typed
// Subquery[T]. Literal lists use InVal and NotInVal.
type SetRHS[T any] interface {
	setValue(T)
	setOperand(negated bool) exprInfo
}

// Column is the typed column API of generated code. Its methods are the ones that
// make sense for every T; functions that need a particular kind of value (Upper
// needs text, Sum needs numbers) are package-level functions with a constraint on
// T, so applying them to the wrong column does not compile.
type Column[O, T any] interface {
	TypedColumn[O, T]
	RHS[T]

	// WithTable returns the column rebound to another source with the same column,
	// such as a CTE or an alias of its table.
	WithTable(table Table) Column[O, T]
	// As returns the column rebound to an alias of its table.
	As(alias string) Column[O, T]

	// Param returns the column's own parameter, for queries that compare the column
	// to a value supplied at execution. Every call returns the same parameter.
	Param() Param[T]
	// ListParam returns the column's own list parameter, for In and NotIn.
	ListParam() ListParam[T]
	// Bind supplies a value for Param.
	Bind(value T) Arg
	// BindList supplies values for ListParam.
	BindList(values ...T) Arg

	IsNull() Condition
	IsNotNull() Condition

	// EQ and the other comparisons take a column, a Param or a typed subquery.
	EQ(rhs RHS[T]) Condition
	NE(rhs RHS[T]) Condition
	GT(rhs RHS[T]) Condition
	GTE(rhs RHS[T]) Condition
	LT(rhs RHS[T]) Condition
	LTE(rhs RHS[T]) Condition
	// Like matches a pattern as written, wildcards included. StartsWith, EndsWith
	// and Contains match literally.
	Like(rhs RHS[T]) Condition
	NotLike(rhs RHS[T]) Condition
	Between(start, end RHS[T]) Condition
	NotBetween(start, end RHS[T]) Condition
	// In takes a ListParam or a typed subquery. An empty list matches nothing, and
	// NotIn over an empty list matches everything.
	In(set SetRHS[T]) Condition
	NotIn(set SetRHS[T]) Condition

	// The *Val forms take a Go value, bound as a parameter. They exist because the
	// value's type comes from the column, so an untyped constant fits any numeric
	// column: Price.GTVal(10) works on an int64 column.
	EQVal(value T) Condition
	NEVal(value T) Condition
	GTVal(value T) Condition
	GTEVal(value T) Condition
	LTVal(value T) Condition
	LTEVal(value T) Condition
	LikeVal(pattern T) Condition
	NotLikeVal(pattern T) Condition
	BetweenVal(start, end T) Condition
	NotBetweenVal(start, end T) Condition
	// InVal matches any of values; no values matches nothing.
	InVal(values ...T) Condition
	// NotInVal matches none of values; no values matches everything.
	NotInVal(values ...T) Condition

	// Pred builds a custom condition. The first %s is the column and each further %s
	// takes the next argument, which may be a column, a Param, a typed subquery or a
	// plain value (bound). %% is a literal percent sign.
	Pred(format string, args ...any) Condition
	// Expr wraps the column in custom SQL; format has exactly one %s.
	Expr(format string) Column[O, T]
	// Exprf is Expr with further arguments, as in Pred.
	Exprf(format string, args ...any) Column[O, T]

	Asc() OrderBy
	Desc() OrderBy
}

// scanPointer returns the address a selected value scans into.
type scanPointer func(holder any) any

// columnCore is the untyped state behind every column value. It is never mutated
// after construction; derived columns get a new core.
type columnCore struct {
	table Table
	name  string
	json  string
	info  exprInfo
	// plain reports a direct reference to table.name, which can be rebound to
	// another source and sorted by name.
	plain bool
	scan  scanPointer
	// param and list are the column's own parameters, shared by every copy and
	// rebinding of the column so that Bind finds them.
	param *paramSpec
	list  *paramSpec
}

func (c *columnCore) err() error {
	if c == nil {
		return errors.New("column cannot be nil")
	}

	return c.info.err
}

// columnRef renders table.name for the table's reference name.
func columnRef(table Table, name string) sqlExpr {
	return sqlJoin(tableRef(table), sqlText("."), sqlIdent(name))
}

type columnImpl[O, T any] struct {
	c *columnCore
}

// NewColumn declares a column of table. Generated code calls it; see TableOf for
// the declaration order it expects.
func NewColumn[O, T any](table *TableOf[O], name, jsonName string, field func(*O) *T) Column[O, T] {
	core := &columnCore{
		name:  name,
		json:  jsonName,
		plain: true,
		param: newParamSpec(name, paramScalar),
		list:  newParamSpec(name, paramList),
	}

	switch {
	case table == nil:
		core.info.err = fmt.Errorf("column %s has a nil table", name)
	case field == nil:
		core.info.err = fmt.Errorf("column %s has a nil field accessor", name)
	case !builtInIdentifierPattern.MatchString(name):
		core.info.err = fmt.Errorf("column name %q is not a plain SQL identifier", name)
	default:
		core.table = table
		core.info = exprInfo{sql: columnRef(table, name), tables: map[string]Table{table.Name(): table}}
		core.scan = func(holder any) any { return field(holder.(*O)) }
	}

	if table != nil {
		core.param.name = table.Name() + "." + name
		core.list.name = table.Name() + "." + name
	}

	return columnImpl[O, T]{c: core}
}

func (c columnImpl[O, T]) core() *columnCore { return c.c }

// Name returns the physical column name.
func (c columnImpl[O, T]) Name() string { return c.c.name }

// Table returns the table the column belongs to.
func (c columnImpl[O, T]) Table() Table { return c.c.table }

// JSONFieldName returns the JSON field name of the column.
func (c columnImpl[O, T]) JSONFieldName() string { return c.c.json }

// String renders the column for debugging, in SQLite syntax.
func (c columnImpl[O, T]) String() string { return debugSQL(c.c.info.sql) }

func (columnImpl[O, T]) boundTo(O)  {}
func (columnImpl[O, T]) valueOf(T)  {}
func (columnImpl[O, T]) rhsValue(T) {}

func (c columnImpl[O, T]) operand() exprInfo { return c.c.info }

// derive returns a column whose SQL is sql, keeping the scan target, name and
// parameters of c.
func (c columnImpl[O, T]) derive(info exprInfo) *columnCore {
	next := *c.c
	next.info = info
	next.plain = false

	return &next
}

// WithTable rebinds the column to table.
func (c columnImpl[O, T]) WithTable(table Table) Column[O, T] {
	return columnImpl[O, T]{c: rebind(c.c, table)}
}

// As rebinds the column to alias.
func (c columnImpl[O, T]) As(alias string) Column[O, T] {
	return c.WithTable(AliasTable(c.c.table, alias))
}

func rebind(c *columnCore, table Table) *columnCore {
	next := *c

	switch {
	case c.info.err != nil:
		return &next
	case isNilValue(table):
		next.info = exprInfo{err: fmt.Errorf("cannot rebind column %s to a nil table", c.name)}
	case !c.plain:
		next.info = exprInfo{err: fmt.Errorf("cannot rebind the expression on %s; rebind the column before applying functions", c.name)}
	case !tableHasColumn(table, c.name):
		next.info = exprInfo{err: fmt.Errorf("column %s does not exist on %s", c.name, table.Name())}
	default:
		next.table = table
		next.info = exprInfo{sql: columnRef(table, c.name), tables: map[string]Table{table.Name(): table}}
	}

	return &next
}

// Param returns the column's parameter.
func (c columnImpl[O, T]) Param() Param[T] { return Param[T]{spec: c.c.param} }

// ListParam returns the column's list parameter.
func (c columnImpl[O, T]) ListParam() ListParam[T] { return ListParam[T]{spec: c.c.list} }

// Bind supplies a value for the column's parameter.
func (c columnImpl[O, T]) Bind(value T) Arg { return c.Param().Bind(value) }

// BindList supplies values for the column's list parameter.
func (c columnImpl[O, T]) BindList(values ...T) Arg { return c.ListParam().Bind(values...) }

func (c columnImpl[O, T]) compare(op string, rhs exprInfo) Condition {
	info := c.c.info.merge(rhs)

	return newCondition(info.withSQL(sqlJoin(c.c.info.sql, sqlText(" "+op+" "), rhs.sql)))
}

func rhsInfo[T any](rhs RHS[T]) exprInfo {
	if isNilValue(rhs) {
		return exprInfo{err: errors.New("comparison operand cannot be nil")}
	}

	return rhs.operand()
}

// IsNull matches NULL.
func (c columnImpl[O, T]) IsNull() Condition {
	return newCondition(c.c.info.withSQL(sqlJoin(c.c.info.sql, sqlText(" IS NULL"))))
}

// IsNotNull matches anything but NULL.
func (c columnImpl[O, T]) IsNotNull() Condition {
	return newCondition(c.c.info.withSQL(sqlJoin(c.c.info.sql, sqlText(" IS NOT NULL"))))
}

// EQ compares with =.
func (c columnImpl[O, T]) EQ(rhs RHS[T]) Condition { return c.compare("=", rhsInfo(rhs)) }

// NE compares with <>.
func (c columnImpl[O, T]) NE(rhs RHS[T]) Condition { return c.compare("<>", rhsInfo(rhs)) }

// GT compares with >.
func (c columnImpl[O, T]) GT(rhs RHS[T]) Condition { return c.compare(">", rhsInfo(rhs)) }

// GTE compares with >=.
func (c columnImpl[O, T]) GTE(rhs RHS[T]) Condition { return c.compare(">=", rhsInfo(rhs)) }

// LT compares with <.
func (c columnImpl[O, T]) LT(rhs RHS[T]) Condition { return c.compare("<", rhsInfo(rhs)) }

// LTE compares with <=.
func (c columnImpl[O, T]) LTE(rhs RHS[T]) Condition { return c.compare("<=", rhsInfo(rhs)) }

// Like matches with LIKE.
func (c columnImpl[O, T]) Like(rhs RHS[T]) Condition { return c.compare("LIKE", rhsInfo(rhs)) }

// NotLike matches with NOT LIKE.
func (c columnImpl[O, T]) NotLike(rhs RHS[T]) Condition {
	return c.compare("NOT LIKE", rhsInfo(rhs))
}

// Between matches the inclusive range.
func (c columnImpl[O, T]) Between(start, end RHS[T]) Condition {
	return c.between("BETWEEN", rhsInfo(start), rhsInfo(end))
}

// NotBetween matches outside the inclusive range.
func (c columnImpl[O, T]) NotBetween(start, end RHS[T]) Condition {
	return c.between("NOT BETWEEN", rhsInfo(start), rhsInfo(end))
}

func (c columnImpl[O, T]) between(op string, start, end exprInfo) Condition {
	info := c.c.info.merge(start).merge(end)

	return newCondition(info.withSQL(sqlJoin(
		c.c.info.sql, sqlText(" "+op+" "), start.sql, sqlText(" AND "), end.sql,
	)))
}

// In matches values in set.
func (c columnImpl[O, T]) In(set SetRHS[T]) Condition { return c.membership("IN", set, false) }

// NotIn matches values not in set.
func (c columnImpl[O, T]) NotIn(set SetRHS[T]) Condition {
	return c.membership("NOT IN", set, true)
}

func (c columnImpl[O, T]) membership(op string, set SetRHS[T], negated bool) Condition {
	if isNilValue(set) {
		return conditionError(errors.New("IN operand cannot be nil"))
	}

	return c.compare(op, set.setOperand(negated))
}

// EQVal compares with = to a bound value.
func (c columnImpl[O, T]) EQVal(value T) Condition { return c.compare("=", operandOf(value)) }

// NEVal compares with <> to a bound value.
func (c columnImpl[O, T]) NEVal(value T) Condition { return c.compare("<>", operandOf(value)) }

// GTVal compares with > to a bound value.
func (c columnImpl[O, T]) GTVal(value T) Condition { return c.compare(">", operandOf(value)) }

// GTEVal compares with >= to a bound value.
func (c columnImpl[O, T]) GTEVal(value T) Condition { return c.compare(">=", operandOf(value)) }

// LTVal compares with < to a bound value.
func (c columnImpl[O, T]) LTVal(value T) Condition { return c.compare("<", operandOf(value)) }

// LTEVal compares with <= to a bound value.
func (c columnImpl[O, T]) LTEVal(value T) Condition { return c.compare("<=", operandOf(value)) }

// LikeVal matches with LIKE against a bound pattern.
func (c columnImpl[O, T]) LikeVal(pattern T) Condition {
	return c.compare("LIKE", operandOf(pattern))
}

// NotLikeVal matches with NOT LIKE against a bound pattern.
func (c columnImpl[O, T]) NotLikeVal(pattern T) Condition {
	return c.compare("NOT LIKE", operandOf(pattern))
}

// BetweenVal matches the inclusive range of bound values.
func (c columnImpl[O, T]) BetweenVal(start, end T) Condition {
	return c.between("BETWEEN", operandOf(start), operandOf(end))
}

// NotBetweenVal matches outside the inclusive range of bound values.
func (c columnImpl[O, T]) NotBetweenVal(start, end T) Condition {
	return c.between("NOT BETWEEN", operandOf(start), operandOf(end))
}

// InVal matches any of values.
func (c columnImpl[O, T]) InVal(values ...T) Condition { return c.valueList("IN", "1 = 0", values) }

// NotInVal matches none of values.
func (c columnImpl[O, T]) NotInVal(values ...T) Condition {
	return c.valueList("NOT IN", "1 = 1", values)
}

func (c columnImpl[O, T]) valueList(op, empty string, values []T) Condition {
	if len(values) == 0 {
		return newCondition(c.c.info.withSQL(sqlText(empty)))
	}

	info := exprInfo{}
	items := make([]sqlExpr, 0, len(values))

	for _, v := range values {
		item := operandOf(v)
		info = info.merge(item)
		items = append(items, item.sql)
	}

	return c.compare(op, info.withSQL(sqlJoin(sqlText("("), sqlList(", ", items), sqlText(")"))))
}

// Pred builds a custom condition around the column.
func (c columnImpl[O, T]) Pred(format string, args ...any) Condition {
	info, err := c.format(format, args)
	if err != nil {
		return conditionError(err)
	}

	return newCondition(info)
}

func (c columnImpl[O, T]) format(format string, args []any) (exprInfo, error) {
	if strings.TrimSpace(format) == "" {
		return exprInfo{}, errors.New("format cannot be empty")
	}

	info := c.c.info
	operands := make([]sqlExpr, 0, len(args)+1)
	operands = append(operands, c.c.info.sql)

	for _, arg := range args {
		op := operandOf(arg)
		info = info.merge(op)
		operands = append(operands, op.sql)
	}

	sql, err := sqlFormat(format, operands...)
	if err != nil {
		return exprInfo{}, err
	}

	return info.withSQL(sql), info.err
}

// Expr wraps the column in custom SQL.
func (c columnImpl[O, T]) Expr(format string) Column[O, T] {
	return c.Exprf(format)
}

// Exprf wraps the column and further arguments in custom SQL.
func (c columnImpl[O, T]) Exprf(format string, args ...any) Column[O, T] {
	info, err := c.format(format, args)
	if err != nil {
		info = exprInfo{err: err}
	}

	return columnImpl[O, T]{c: c.derive(info)}
}

// Asc orders by the column ascending.
func (c columnImpl[O, T]) Asc() OrderBy { return OrderBy{column: c, direction: ASC} }

// Desc orders by the column descending.
func (c columnImpl[O, T]) Desc() OrderBy { return OrderBy{column: c, direction: DESC} }

// SQLColumns converts typed columns into a slice of SQLColumn.
func SQLColumns[O any](cols ...BoundColumn[O]) []SQLColumn {
	result := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		result = append(result, col)
	}

	return result
}

// MapInto projects source into a field of a result type. Generated result code
// calls it; the result's json name is what PageRequest.OrderBy sorts by.
func MapInto[Target, T any](source ValueColumn[T], field func(*Target) *T, jsonName string) ResultColumn[Target, T] {
	if isNilValue(source) {
		return columnImpl[Target, T]{c: &columnCore{json: jsonName, info: exprInfo{err: errors.New("projection source cannot be nil")}}}
	}

	next := *source.core()
	next.json = jsonName
	next.plain = false

	if field == nil {
		next.info = exprInfo{err: fmt.Errorf("projection of %s has a nil field accessor", next.name)}
	} else {
		next.scan = func(holder any) any { return field(holder.(*Target)) }
	}

	return columnImpl[Target, T]{c: &next}
}

// columnInfo reads a column that may be nil.
func columnInfo(col SQLColumn) exprInfo {
	if isNilValue(col) || col.core() == nil {
		return exprInfo{err: errors.New("column cannot be nil")}
	}

	return col.core().info
}
