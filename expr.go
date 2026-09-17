package tsq

import (
	"errors"
	"maps"
)

// exprInfo is a rendered-later SQL expression plus what the query builder needs to
// know about it without rendering it.
type exprInfo struct {
	sql sqlExpr
	// tables are the tables the expression references directly, keyed by the name
	// the query uses for them (the alias when there is one).
	tables    map[string]Table
	aggregate bool
	// inList is the list parameter of a col IN (list) condition, for ListIn. It is
	// deliberately not merged: under AND, OR or NOT the condition is no longer one
	// a query can be split on.
	inList *paramSpec
	err    error
}

func (e exprInfo) withSQL(sql sqlExpr) exprInfo {
	e.sql = sql
	return e
}

// merge folds other's tables, flags and error into e; the SQL is left to the caller.
func (e exprInfo) merge(other exprInfo) exprInfo {
	if e.err == nil {
		e.err = other.err
	}

	if len(other.tables) > 0 {
		tables := make(map[string]Table, len(e.tables)+len(other.tables))
		maps.Copy(tables, e.tables)
		maps.Copy(tables, other.tables)
		e.tables = tables
	}

	e.aggregate = e.aggregate || other.aggregate

	return e
}

// allTables returns the directly referenced tables plus the outer tables of any
// correlated subquery inside the expression: those must be present in the query
// that contains it.
func (e exprInfo) allTables() map[string]Table {
	correlated := e.sql.correlated()
	if len(correlated) == 0 {
		return e.tables
	}

	tables := make(map[string]Table, len(e.tables)+len(correlated))
	maps.Copy(tables, e.tables)
	maps.Copy(tables, correlated)

	return tables
}

// operandOf turns any value accepted where SQL is expected into an expression: a
// TSQ column, parameter, subquery or condition renders as itself, and anything else
// is a bound value.
func operandOf(v any) exprInfo {
	switch x := v.(type) {
	case nil:
		return exprInfo{err: errors.New("nil is not a value; use IsNull/IsNotNull")}
	case interface{ operand() exprInfo }:
		return x.operand()
	case Condition:
		return x.condition()
	case AnySubquery:
		return exprInfo{err: errors.New("a subquery used as a value must be typed; build it with AsSubquery or BuildSubquery")}
	case Arg:
		return exprInfo{err: errors.New("an Arg is a value for execution, not an expression; pass the Param instead")}
	}

	if err := validatePredicateValue(v); err != nil {
		return exprInfo{err: err}
	}

	return exprInfo{sql: sqlValue(v)}
}

// Condition is a SQL boolean expression: a WHERE, HAVING, ON or CASE WHEN term.
// Conditions come from column methods, And, Or, Not, Exists and NotExists.
type Condition interface {
	condition() exprInfo
}

type conditionExpr struct {
	info exprInfo
}

func (c conditionExpr) condition() exprInfo { return c.info }

func newCondition(info exprInfo) Condition { return conditionExpr{info: info} }

func conditionError(err error) Condition { return conditionExpr{info: exprInfo{err: err}} }

// conditionInfo reads a condition that may be nil.
func conditionInfo(c Condition) exprInfo {
	if isNilValue(c) {
		return exprInfo{err: errors.New("condition cannot be nil")}
	}

	return c.condition()
}

// And combines conditions with AND. And() with no arguments is always true, which
// is how an UPDATE or DELETE over every row is spelled.
func And(conds ...Condition) Condition {
	return combine(" AND ", "1 = 1", conds)
}

// Or combines conditions with OR. Or() with no arguments is always false.
func Or(conds ...Condition) Condition {
	return combine(" OR ", "1 = 0", conds)
}

// Not negates a condition.
func Not(cond Condition) Condition {
	info := conditionInfo(cond)

	return newCondition(info.withSQL(sqlJoin(sqlText("NOT ("), info.sql, sqlText(")"))))
}

func combine(sep, empty string, conds []Condition) Condition {
	if len(conds) == 0 {
		return newCondition(exprInfo{sql: sqlText(empty)})
	}

	var info exprInfo

	parts := make([]sqlExpr, 0, len(conds))
	for _, c := range conds {
		ci := conditionInfo(c)
		info = info.merge(ci)
		parts = append(parts, ci.sql)
	}

	if len(parts) == 1 {
		return newCondition(info.withSQL(parts[0]))
	}

	return newCondition(info.withSQL(sqlJoin(sqlText("("), sqlList(sep, parts), sqlText(")"))))
}

// andAll renders conds as one AND-ed term without the outer parentheses a single
// condition does not need.
func andAll(conds []Condition) exprInfo {
	return conditionInfo(combine(" AND ", "1 = 1", conds))
}

// Exists builds EXISTS (subquery). EXISTS asks whether the subquery returns any
// row, so it belongs to no column.
func Exists(sq AnySubquery) Condition {
	return existsCondition("EXISTS ", sq)
}

// NotExists builds NOT EXISTS (subquery).
func NotExists(sq AnySubquery) Condition {
	return existsCondition("NOT EXISTS ", sq)
}

func existsCondition(keyword string, sq AnySubquery) Condition {
	if isNilValue(sq) {
		return conditionError(errors.New("subquery cannot be nil"))
	}

	info := sq.subquery()

	return newCondition(info.withSQL(sqlJoin(sqlText(keyword), info.sql)))
}
