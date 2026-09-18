package tsq

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
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

// SearchColumn is a text column keyword search may match against; make one with
// Searchable.
type SearchColumn interface {
	SQLColumn
	searchable()
}

// RHS is the right-hand side of a comparison against a T: a column or expression
// holding a T, a Param[T], a Value[T] from Val, or a typed scalar Subquery[T].
type RHS[T any] interface {
	rhsValue(T)
	operand() exprInfo
}

// SetRHS is the right-hand side of IN and NOT IN over T: a ListParam[T], a
// ValueList[T] from Vals, or a typed Subquery[T].
type SetRHS[T any] interface {
	setValue(T)
	setOperand(negated bool) exprInfo
}

// Expression is a typed SQL expression: a column, a function of one, a CASE, or
// custom SQL. Its value type is T, and it can be compared, ordered and grouped by.
//
// An expression is not tied to a row type, so it cannot be passed to Select: what
// it holds may have nothing to do with the field its source column scans into
// (tsq.Date of a time column holds text). Project it into a field with MapInto, or
// select it on its own with SelectValue.
type Expression[T any] interface {
	ValueColumn[T]
	RHS[T]

	IsNull() Condition
	IsNotNull() Condition

	// EQ and the other comparisons take a column, a Param, a Val or a typed subquery.
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
	// In takes a ListParam, Vals or a typed subquery. An empty list matches nothing, and
	// NotIn over an empty list matches everything.
	In(set SetRHS[T]) Condition
	NotIn(set SetRHS[T]) Condition

	// Pred builds a custom condition. The first %s is the expression and each further
	// %s takes the next argument, which may be a column, a Param, a typed subquery or
	// a plain value (bound). %% is a literal percent sign.
	Pred(format string, args ...any) Condition
	// Expr wraps the expression in custom SQL; format has exactly one %s.
	Expr(format string) Expression[T]
	// Exprf is Expr with further arguments, as in Pred.
	Exprf(format string, args ...any) Expression[T]

	Asc() OrderBy
	Desc() OrderBy
}

// Column is the typed column API of generated code: an Expression that also scans
// into a field of O, which is what Select needs. Functions that need a particular
// kind of value (Upper needs text, Sum needs numbers) are package-level functions
// with a constraint on T, so applying them to the wrong column does not compile.
type Column[O, T any] interface {
	Expression[T]
	TypedColumn[O, T]

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
	// get reads the value the row holds, without reflection: the write path binds
	// one value per column per row, which is where reflection cost shows up.
	get func(holder any) any
	// fill says who provides the value: the caller, or the database through a
	// DEFAULT or a generated expression. It comes from TableSpec.Schema.
	fill tsqdialect.Fill
	// nullable reports that the scan target holds NULL: a NullColumn, or a
	// MapIntoNull projection. A value that can be NULL may only be read into one.
	nullable bool
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

// exprImpl implements Expression[T]; columnImpl adds what only a column has.
type exprImpl[T any] struct {
	c *columnCore
}

type columnImpl[O, T any] struct {
	exprImpl[T]
}

// NullColumn is a column that can hold NULL. Its value type T is what it holds
// when it is not NULL, so it compares with a T like any column: Nickname.EQ(
// tsq.Val("x")). NULL is matched with IsNull and written with SetNull.
type NullColumn[O, T any] interface {
	Column[O, T]
	nullColumn()
}

type nullColumnImpl[O, T any] struct {
	columnImpl[O, T]
}

func (nullColumnImpl[O, T]) nullColumn() {}

// NewColumn declares a NOT NULL column of table. Generated code calls it; see
// TableOf for the declaration order it expects. A field that can hold NULL (a
// pointer, sql.NullString, sql.Null[T], null.String, ...) is a NewNullColumn.
func NewColumn[O, T any](table *TableOf[O], name, jsonName string, field func(*O) *T) Column[O, T] {
	c := newColumn(table, name, jsonName, field)
	if _, ok := nullableValueType(reflect.TypeFor[T]()); ok && c.c.info.err == nil {
		c.c.info.err = fmt.Errorf("column %s is held in %v, which can be NULL; declare it with NewNullColumn[%v]",
			name, reflect.TypeFor[T](), valueTypeName[T]())
	}

	return c
}

// NewNullColumn declares a column of table that can hold NULL. T is the value
// type and F the field type, a nullable form of T: *T, sql.Null[T], or a struct
// with a Valid bool and one field of type T, such as sql.NullString or
// null.String. Only T is written: tsq.NewNullColumn[string](h, "nick", ...).
func NewNullColumn[T, O, F any](table *TableOf[O], name, jsonName string, field func(*O) *F) NullColumn[O, T] {
	c := newColumn(table, name, jsonName, field)
	c.c.nullable = true
	c.c.info.null.always = true

	if value, ok := nullableValueType(reflect.TypeFor[F]()); (!ok || value != reflect.TypeFor[T]()) && c.c.info.err == nil {
		c.c.info.err = fmt.Errorf("column %s: %v is not a nullable form of %v", name, reflect.TypeFor[F](), reflect.TypeFor[T]())
	}

	return nullColumnImpl[O, T]{c: c.c}
}

var scannerType = reflect.TypeFor[sql.Scanner]()

// nullableValueType returns the value type of a nullable form: the element of a
// pointer, or the single data field of a scannable struct with a Valid bool.
func nullableValueType(t reflect.Type) (reflect.Type, bool) {
	switch t.Kind() {
	case reflect.Pointer:
		return t.Elem(), true
	case reflect.Struct:
	default:
		return nil, false
	}

	if !reflect.PointerTo(t).Implements(scannerType) {
		return nil, false
	}

	var (
		valid bool
		value reflect.Type
		n     int
	)

	for _, f := range reflect.VisibleFields(t) {
		switch {
		case f.Anonymous || !f.IsExported():
		case f.Name == "Valid" && f.Type.Kind() == reflect.Bool:
			valid = true
		default:
			value = f.Type
			n++
		}
	}

	return value, valid && n == 1
}

func valueTypeName[T any]() string {
	if value, ok := nullableValueType(reflect.TypeFor[T]()); ok {
		return value.String()
	}

	return reflect.TypeFor[T]().String()
}

func newColumn[O, T any](table *TableOf[O], name, jsonName string, field func(*O) *T) columnImpl[O, T] {
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
		core.info = exprInfo{sql: columnRef(table, name), tables: map[string]Table{table.Name(): table}, null: nullableIn(table.Name())}
		core.scan = func(holder any) any { return field(holder.(*O)) }
		core.get = func(holder any) any { return *field(holder.(*O)) }
	}

	if table != nil {
		core.param.name = table.Name() + "." + name
		core.list.name = table.Name() + "." + name
	}

	return columnImpl[O, T]{exprImpl[T]{c: core}}
}

func (c exprImpl[T]) core() *columnCore { return c.c }

// Name returns the physical column name.
func (c exprImpl[T]) Name() string { return c.c.name }

// Table returns the table the column belongs to.
func (c exprImpl[T]) Table() Table { return c.c.table }

// JSONFieldName returns the JSON field name of the column.
func (c exprImpl[T]) JSONFieldName() string { return c.c.json }

// String renders the column for debugging, in SQLite syntax.
func (c exprImpl[T]) String() string { return debugSQL(c.c.info.sql) }

func (columnImpl[O, T]) boundTo(O) {}
func (exprImpl[T]) valueOf(T)      {}
func (exprImpl[T]) rhsValue(T)     {}

func (c exprImpl[T]) operand() exprInfo { return c.c.info }

// derive returns a column whose SQL is sql, keeping the scan target, name and
// parameters of c.
func (c exprImpl[T]) derive(info exprInfo) *columnCore {
	next := *c.c
	next.info = info
	next.plain = false

	return &next
}

// WithTable rebinds the column to table.
func (c columnImpl[O, T]) WithTable(table Table) Column[O, T] {
	return columnImpl[O, T]{exprImpl[T]{c: rebind(c.c, table)}}
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
		next.info = exprInfo{sql: columnRef(table, c.name), tables: map[string]Table{table.Name(): table}, null: nullableIn(table.Name())}
		next.info.null.always = c.info.null.always

		if body := table.cteBody(); body != nil && body.nullableOutput(c.name) {
			next.info.null.always = true
		}
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

func (c exprImpl[T]) compare(op string, rhs exprInfo) Condition {
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
func (c exprImpl[T]) IsNull() Condition {
	return newCondition(c.c.info.withSQL(sqlJoin(c.c.info.sql, sqlText(" IS NULL"))))
}

// IsNotNull matches anything but NULL.
func (c exprImpl[T]) IsNotNull() Condition {
	return newCondition(c.c.info.withSQL(sqlJoin(c.c.info.sql, sqlText(" IS NOT NULL"))))
}

// EQ compares with =.
func (c exprImpl[T]) EQ(rhs RHS[T]) Condition { return c.compare("=", rhsInfo(rhs)) }

// NE compares with <>.
func (c exprImpl[T]) NE(rhs RHS[T]) Condition { return c.compare("<>", rhsInfo(rhs)) }

// GT compares with >.
func (c exprImpl[T]) GT(rhs RHS[T]) Condition { return c.compare(">", rhsInfo(rhs)) }

// GTE compares with >=.
func (c exprImpl[T]) GTE(rhs RHS[T]) Condition { return c.compare(">=", rhsInfo(rhs)) }

// LT compares with <.
func (c exprImpl[T]) LT(rhs RHS[T]) Condition { return c.compare("<", rhsInfo(rhs)) }

// LTE compares with <=.
func (c exprImpl[T]) LTE(rhs RHS[T]) Condition { return c.compare("<=", rhsInfo(rhs)) }

// Like matches with LIKE.
func (c exprImpl[T]) Like(rhs RHS[T]) Condition { return c.compare("LIKE", rhsInfo(rhs)) }

// NotLike matches with NOT LIKE.
func (c exprImpl[T]) NotLike(rhs RHS[T]) Condition {
	return c.compare("NOT LIKE", rhsInfo(rhs))
}

// Between matches the inclusive range.
func (c exprImpl[T]) Between(start, end RHS[T]) Condition {
	return c.between("BETWEEN", rhsInfo(start), rhsInfo(end))
}

// NotBetween matches outside the inclusive range.
func (c exprImpl[T]) NotBetween(start, end RHS[T]) Condition {
	return c.between("NOT BETWEEN", rhsInfo(start), rhsInfo(end))
}

func (c exprImpl[T]) between(op string, start, end exprInfo) Condition {
	info := c.c.info.merge(start).merge(end)

	return newCondition(info.withSQL(sqlJoin(
		c.c.info.sql, sqlText(" "+op+" "), start.sql, sqlText(" AND "), end.sql,
	)))
}

// In matches values in set.
func (c exprImpl[T]) In(set SetRHS[T]) Condition { return c.membership("IN", set, false) }

// NotIn matches values not in set.
func (c exprImpl[T]) NotIn(set SetRHS[T]) Condition {
	return c.membership("NOT IN", set, true)
}

func (c exprImpl[T]) membership(op string, set SetRHS[T], negated bool) Condition {
	if isNilValue(set) {
		return conditionError(errors.New("IN operand cannot be nil"))
	}

	operand := set.setOperand(negated)
	cond := c.compare(op, operand).condition()
	cond.inList = operand.inList

	return newCondition(cond)
}

// Pred builds a custom condition around the column.
func (c exprImpl[T]) Pred(format string, args ...any) Condition {
	info, err := c.format(format, args)
	if err != nil {
		return conditionError(err)
	}

	return newCondition(info)
}

func (c exprImpl[T]) format(format string, args []any) (exprInfo, error) {
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
func (c exprImpl[T]) Expr(format string) Expression[T] {
	return c.Exprf(format)
}

// Exprf wraps the column and further arguments in custom SQL.
func (c exprImpl[T]) Exprf(format string, args ...any) Expression[T] {
	info, err := c.format(format, args)
	if err != nil {
		info = exprInfo{err: err}
	}

	return exprImpl[T]{c: c.derive(info)}
}

// Asc orders by the column ascending.
func (c exprImpl[T]) Asc() OrderBy { return OrderBy{column: c, direction: ASC} }

// Desc orders by the column descending.
func (c exprImpl[T]) Desc() OrderBy { return OrderBy{column: c, direction: DESC} }

// SQLColumns converts typed columns into a slice of SQLColumn.
func SQLColumns[O any](cols ...BoundColumn[O]) []SQLColumn {
	result := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		result = append(result, col)
	}

	return result
}

// MapInto projects source into a field of a result type. Generated result code
// calls it; the result's json name is what PageRequest.OrderBy sorts by. The field
// cannot hold NULL, so a query where source can be NULL (a nullable column, an
// outer-joined table, an aggregate without GROUP BY) refuses to read it: use
// MapIntoNull, or Coalesce.
func MapInto[Target, T any](source ValueColumn[T], field func(*Target) *T, jsonName string) ResultColumn[Target, T] {
	return mapInto[Target, T](source, field, jsonName, false)
}

// MapIntoNull projects source into a field that can hold NULL: F is a nullable
// form of T, as NewNullColumn describes.
func MapIntoNull[Target, T, F any](source ValueColumn[T], field func(*Target) *F, jsonName string) ResultColumn[Target, T] {
	c := mapInto[Target, T](source, field, jsonName, true)

	if value, ok := nullableValueType(reflect.TypeFor[F]()); (!ok || value != reflect.TypeFor[T]()) && c.c.info.err == nil {
		c.c.info.err = fmt.Errorf("projection %s: %v is not a nullable form of %v", jsonName, reflect.TypeFor[F](), reflect.TypeFor[T]())
	}

	return c
}

func mapInto[Target, T, F any](source ValueColumn[T], field func(*Target) *F, jsonName string, nullable bool) columnImpl[Target, T] {
	if isNilValue(source) {
		return columnImpl[Target, T]{exprImpl[T]{c: &columnCore{json: jsonName, info: exprInfo{err: errors.New("projection source cannot be nil")}}}}
	}

	next := *source.core()
	next.json = jsonName
	next.plain = false
	next.nullable = nullable

	if field == nil {
		next.info = exprInfo{err: fmt.Errorf("projection of %s has a nil field accessor", next.name)}
	} else {
		next.scan = func(holder any) any { return field(holder.(*Target)) }
	}

	return columnImpl[Target, T]{exprImpl[T]{c: &next}}
}

// columnInfo reads a column that may be nil.
func columnInfo(col SQLColumn) exprInfo {
	if isNilValue(col) || col.core() == nil {
		return exprInfo{err: errors.New("column cannot be nil")}
	}

	return col.core().info
}
