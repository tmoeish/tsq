package tsq

import (
	"errors"
	"fmt"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
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

// SearchColumn is a column keyword search may match against.
type SearchColumn interface {
	SQLColumn
	searchable()
}

// RHS is the right-hand side of a comparison against a T: a column or expression
// holding a T, a Param[T], or a typed scalar Subquery[T]. Plain Go values use the
// *Val methods instead.
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

// Column is the typed column API of generated code.
type Column[O, T any] interface {
	TypedColumn[O, T]
	RHS[T]
	SearchColumn

	// WithTable returns the column rebound to another source with the same column,
	// such as a CTE or an alias of its table.
	WithTable(table Table) Column[O, T]
	// As returns the column rebound to an alias of its table.
	As(alias string) Column[O, T]

	// Param returns the column's own parameter, for queries that compare the column
	// to a value supplied at execution. Every call returns the same parameter.
	Param() Param[T]
	// ListParam returns the column's own list parameter, for IN and NOT IN.
	ListParam() ListParam[T]
	// Bind supplies a value for Param.
	Bind(value T) Arg
	// BindList supplies values for ListParam.
	BindList(values ...T) Arg

	IsNull() Condition
	IsNotNull() Condition

	EQ(rhs RHS[T]) Condition
	NE(rhs RHS[T]) Condition
	GT(rhs RHS[T]) Condition
	GTE(rhs RHS[T]) Condition
	LT(rhs RHS[T]) Condition
	LTE(rhs RHS[T]) Condition
	// Like matches the column against a pattern; the pattern's wildcards are the
	// caller's.
	Like(rhs RHS[T]) Condition
	NotLike(rhs RHS[T]) Condition
	Between(start, end RHS[T]) Condition
	NotBetween(start, end RHS[T]) Condition
	In(set SetRHS[T]) Condition
	NotIn(set SetRHS[T]) Condition
	// StartsWith matches values beginning with the parameter's value; wildcards in
	// the value match literally.
	StartsWith(prefix Param[string]) Condition
	NotStartsWith(prefix Param[string]) Condition
	EndsWith(suffix Param[string]) Condition
	NotEndsWith(suffix Param[string]) Condition
	Contains(part Param[string]) Condition
	NotContains(part Param[string]) Condition

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
	StartsWithVal(prefix string) Condition
	NotStartsWithVal(prefix string) Condition
	EndsWithVal(suffix string) Condition
	NotEndsWithVal(suffix string) Condition
	ContainsVal(part string) Condition
	NotContainsVal(part string) Condition

	// Pred builds a custom condition. The first %s is the column and each further %s
	// takes the next argument, which may be a column, a Param, a typed subquery or a
	// plain value (bound). %% is a literal percent sign.
	Pred(format string, args ...any) Condition
	// Expr wraps the column in custom SQL; format has exactly one %s.
	Expr(format string) Column[O, T]
	// Exprf is Expr with further arguments, as in Pred.
	Exprf(format string, args ...any) Column[O, T]

	Count() Column[O, int64]
	Sum() Column[O, T]
	Avg() Column[O, float64]
	Max() Column[O, T]
	Min() Column[O, T]
	Distinct() Column[O, T]

	Upper() Column[O, T]
	Lower() Column[O, T]
	Substring(start, length int) Column[O, T]
	Length() Column[O, int64]
	Trim() Column[O, T]

	Date() Column[O, T]
	// Year, Month and Day extract a date part as an integer, spelled per dialect.
	Year() Column[O, int64]
	Month() Column[O, int64]
	Day() Column[O, int64]

	Round(precision int) Column[O, T]
	Ceil() Column[O, T]
	Floor() Column[O, T]
	Abs() Column[O, T]

	Coalesce(value any) Column[O, T]
	NullIf(value any) Column[O, T]

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

func (columnImpl[O, T]) boundTo(O)   {}
func (columnImpl[O, T]) valueOf(T)   {}
func (columnImpl[O, T]) rhsValue(T)  {}
func (columnImpl[O, T]) searchable() {}

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

// StartsWith matches values beginning with prefix.
func (c columnImpl[O, T]) StartsWith(prefix Param[string]) Condition {
	return c.pattern("LIKE", prefix, paramPrefix)
}

// NotStartsWith matches values not beginning with prefix.
func (c columnImpl[O, T]) NotStartsWith(prefix Param[string]) Condition {
	return c.pattern("NOT LIKE", prefix, paramPrefix)
}

// EndsWith matches values ending with suffix.
func (c columnImpl[O, T]) EndsWith(suffix Param[string]) Condition {
	return c.pattern("LIKE", suffix, paramSuffix)
}

// NotEndsWith matches values not ending with suffix.
func (c columnImpl[O, T]) NotEndsWith(suffix Param[string]) Condition {
	return c.pattern("NOT LIKE", suffix, paramSuffix)
}

// Contains matches values containing part.
func (c columnImpl[O, T]) Contains(part Param[string]) Condition {
	return c.pattern("LIKE", part, paramContains)
}

// NotContains matches values not containing part.
func (c columnImpl[O, T]) NotContains(part Param[string]) Condition {
	return c.pattern("NOT LIKE", part, paramContains)
}

func (c columnImpl[O, T]) pattern(op string, p Param[string], mode paramMode) Condition {
	if p.spec == nil {
		return conditionError(errors.New("pattern parameter is not initialized; use tsq.NewParam"))
	}

	return c.compare(op, exprInfo{sql: sqlJoin(sqlParam(p.spec.derive(mode)), sqlText(likeEscapeClause))})
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

// StartsWithVal matches values beginning with prefix, which matches literally.
func (c columnImpl[O, T]) StartsWithVal(prefix string) Condition {
	return c.patternVal("LIKE", escapeLikePattern(prefix)+"%")
}

// NotStartsWithVal matches values not beginning with prefix.
func (c columnImpl[O, T]) NotStartsWithVal(prefix string) Condition {
	return c.patternVal("NOT LIKE", escapeLikePattern(prefix)+"%")
}

// EndsWithVal matches values ending with suffix.
func (c columnImpl[O, T]) EndsWithVal(suffix string) Condition {
	return c.patternVal("LIKE", "%"+escapeLikePattern(suffix))
}

// NotEndsWithVal matches values not ending with suffix.
func (c columnImpl[O, T]) NotEndsWithVal(suffix string) Condition {
	return c.patternVal("NOT LIKE", "%"+escapeLikePattern(suffix))
}

// ContainsVal matches values containing part.
func (c columnImpl[O, T]) ContainsVal(part string) Condition {
	return c.patternVal("LIKE", "%"+escapeLikePattern(part)+"%")
}

// NotContainsVal matches values not containing part.
func (c columnImpl[O, T]) NotContainsVal(part string) Condition {
	return c.patternVal("NOT LIKE", "%"+escapeLikePattern(part)+"%")
}

func (c columnImpl[O, T]) patternVal(op, pattern string) Condition {
	return c.compare(op, exprInfo{sql: sqlJoin(sqlValue(pattern), sqlText(likeEscapeClause))})
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

func (c columnImpl[O, T]) wrap(open string) *columnCore {
	return c.derive(c.c.info.withSQL(sqlJoin(sqlText(open), c.c.info.sql, sqlText(")"))))
}

func aggregate[O, T any](core *columnCore) Column[O, T] {
	core.info.aggregate = true

	return columnImpl[O, T]{c: core}
}

// Count wraps the column in COUNT.
func (c columnImpl[O, T]) Count() Column[O, int64] { return aggregate[O, int64](c.wrap("COUNT(")) }

// Sum wraps the column in SUM.
func (c columnImpl[O, T]) Sum() Column[O, T] { return aggregate[O, T](c.wrap("SUM(")) }

// Avg wraps the column in AVG.
func (c columnImpl[O, T]) Avg() Column[O, float64] { return aggregate[O, float64](c.wrap("AVG(")) }

// Max wraps the column in MAX.
func (c columnImpl[O, T]) Max() Column[O, T] { return aggregate[O, T](c.wrap("MAX(")) }

// Min wraps the column in MIN.
func (c columnImpl[O, T]) Min() Column[O, T] { return aggregate[O, T](c.wrap("MIN(")) }

// Distinct selects distinct values of the column.
func (c columnImpl[O, T]) Distinct() Column[O, T] {
	core := c.derive(c.c.info.withSQL(sqlJoin(sqlText("DISTINCT "), c.c.info.sql)))
	core.info.distinct = true

	return columnImpl[O, T]{c: core}
}

// Upper applies UPPER.
func (c columnImpl[O, T]) Upper() Column[O, T] { return columnImpl[O, T]{c: c.wrap("UPPER(")} }

// Lower applies LOWER.
func (c columnImpl[O, T]) Lower() Column[O, T] { return columnImpl[O, T]{c: c.wrap("LOWER(")} }

// Substring applies SUBSTRING(column, start, length) with a 1-based start.
func (c columnImpl[O, T]) Substring(start, length int) Column[O, T] {
	if start < 1 || length < 0 {
		return columnImpl[O, T]{c: c.derive(exprInfo{err: fmt.Errorf("invalid substring range start=%d length=%d", start, length)})}
	}

	return c.Exprf("SUBSTRING(%s, %s, %s)", start, length)
}

// Length applies LENGTH.
func (c columnImpl[O, T]) Length() Column[O, int64] {
	return columnImpl[O, int64]{c: c.wrap("LENGTH(")}
}

// Trim applies TRIM.
func (c columnImpl[O, T]) Trim() Column[O, T] { return columnImpl[O, T]{c: c.wrap("TRIM(")} }

// Date applies DATE.
func (c columnImpl[O, T]) Date() Column[O, T] { return columnImpl[O, T]{c: c.wrap("DATE(")} }

// Year extracts the year.
func (c columnImpl[O, T]) Year() Column[O, int64] { return c.datePart("year", "YEAR", "%Y") }

// Month extracts the month.
func (c columnImpl[O, T]) Month() Column[O, int64] { return c.datePart("month", "MONTH", "%m") }

// Day extracts the day of the month.
func (c columnImpl[O, T]) Day() Column[O, int64] { return c.datePart("day", "DAY", "%d") }

func (c columnImpl[O, T]) datePart(part, sqlPart, strftime string) Column[O, int64] {
	x := c.c.info.sql
	sql := sqlByDialect(part+" extraction", map[tsqdialect.Name]sqlExpr{
		tsqdialect.MySQL:    sqlJoin(sqlText(sqlPart+"("), x, sqlText(")")),
		tsqdialect.Postgres: sqlJoin(sqlText("CAST(EXTRACT("+sqlPart+" FROM "), x, sqlText(") AS BIGINT)")),
		tsqdialect.SQLite:   sqlJoin(sqlText("CAST(strftime('"+strftime+"', "), x, sqlText(") AS INTEGER)")),
	})

	return columnImpl[O, int64]{c: c.derive(c.c.info.withSQL(sql))}
}

// Round applies ROUND(column, precision).
func (c columnImpl[O, T]) Round(precision int) Column[O, T] {
	if precision < 0 {
		return columnImpl[O, T]{c: c.derive(exprInfo{err: errors.New("round precision cannot be negative")})}
	}

	return c.Exprf("ROUND(%s, %s)", precision)
}

// Ceil applies CEIL.
func (c columnImpl[O, T]) Ceil() Column[O, T] { return columnImpl[O, T]{c: c.wrap("CEIL(")} }

// Floor applies FLOOR.
func (c columnImpl[O, T]) Floor() Column[O, T] { return columnImpl[O, T]{c: c.wrap("FLOOR(")} }

// Abs applies ABS.
func (c columnImpl[O, T]) Abs() Column[O, T] { return columnImpl[O, T]{c: c.wrap("ABS(")} }

// Coalesce applies COALESCE(column, value); value may be a column or a plain value.
func (c columnImpl[O, T]) Coalesce(value any) Column[O, T] {
	return c.Exprf("COALESCE(%s, %s)", value)
}

// NullIf applies NULLIF(column, value); value may be a column or a plain value.
func (c columnImpl[O, T]) NullIf(value any) Column[O, T] {
	return c.Exprf("NULLIF(%s, %s)", value)
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
