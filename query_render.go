package tsq

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

type joinType string

const (
	leftJoinType  joinType = "LEFT JOIN"
	innerJoinType joinType = "INNER JOIN"
	rightJoinType joinType = "RIGHT JOIN"
	fullJoinType  joinType = "FULL JOIN"
	crossJoinType joinType = "CROSS JOIN"
)

type setOperationType string

const (
	unionType        setOperationType = "UNION"
	unionAllType     setOperationType = "UNION ALL"
	intersectType    setOperationType = "INTERSECT"
	intersectAllType setOperationType = "INTERSECT ALL"
	exceptType       setOperationType = "EXCEPT"
	exceptAllType    setOperationType = "EXCEPT ALL"
)

func (op setOperationType) capability() tsqdialect.Capability {
	switch op {
	case intersectType:
		return tsqdialect.CapabilityIntersect
	case intersectAllType:
		return tsqdialect.CapabilityIntersectAll
	case exceptType:
		return tsqdialect.CapabilityExcept
	case exceptAllType:
		return tsqdialect.CapabilityExceptAll
	default:
		return ""
	}
}

type queryLockStrength string

const (
	queryLockStrengthUpdate queryLockStrength = "FOR UPDATE"
	queryLockStrengthShare  queryLockStrength = "FOR SHARE"
)

type queryLockWaitMode string

const (
	queryLockWaitNoWait     queryLockWaitMode = "NOWAIT"
	queryLockWaitSkipLocked queryLockWaitMode = "SKIP LOCKED"
)

type queryLock struct {
	strength queryLockStrength
	waitMode queryLockWaitMode
}

type join struct {
	kind  joinType
	table Table
	on    []Condition
}

type setOperation[O any] struct {
	op   setOperationType
	spec querySpec[O]
}

// querySpec is the complete, dialect-independent description of a SELECT.
type querySpec[O any] struct {
	From          Table
	Distinct      bool
	Selects       []BoundColumn[O]
	Filters       []Condition
	KeywordSearch []SearchColumn
	Joins         []join
	GroupBy       []SQLColumn
	Having        []Condition
	OrderBys      []OrderBy
	Limit         *int
	Offset        *int
	Lock          queryLock
	SetOps        []setOperation[O]
	Correlated    []Table
}

func (s querySpec[O]) clone() querySpec[O] {
	c := s
	c.Selects = slices.Clone(s.Selects)
	c.Filters = slices.Clone(s.Filters)
	c.KeywordSearch = slices.Clone(s.KeywordSearch)
	c.Joins = slices.Clone(s.Joins)
	c.GroupBy = slices.Clone(s.GroupBy)
	c.Having = slices.Clone(s.Having)
	c.OrderBys = slices.Clone(s.OrderBys)
	c.Correlated = slices.Clone(s.Correlated)

	c.SetOps = make([]setOperation[O], 0, len(s.SetOps))
	for _, op := range s.SetOps {
		c.SetOps = append(c.SetOps, setOperation[O]{op: op.op, spec: op.spec.clone()})
	}

	return c
}

// renderMode selects the statement variant to render.
type renderMode struct {
	count   bool // SELECT COUNT(1) over the query
	keyword bool // include the keyword-search predicate
	single  bool // bound the result to one row
	// exists selects a constant instead of the columns, where that keeps the rows.
	exists bool
	// keyset pages by position: LIMIT without OFFSET, also on the first page.
	keyset bool
	// paged replaces ORDER BY with order and appends LIMIT/OFFSET.
	paged bool
	// seek, for keyset paging after the first page, is ANDed into WHERE.
	seek   *sqlExpr
	order  []orderTerm
	limit  int
	offset int
}

type orderTerm struct {
	expr      sqlExpr
	direction sortOrder
	// nullable terms place NULLs explicitly; nullsFirst says where.
	nullable   bool
	nullsFirst bool
	// compound terms order a set operation by output name.
	compound bool
	// nullKey is the expression MySQL's "IS NULL" key tests. It is the ordered
	// expression itself where expr is a select-list position: "2 IS NULL" is a
	// constant, not the second column, and dropped the requested NULL placement.
	nullKey sqlExpr
	// aggregate reports an ordered expression with an aggregate in it.
	aggregate bool
}

// render writes the term, placing NULLs where nullsFirst says. PostgreSQL and
// SQLite spell it NULLS FIRST / LAST. MySQL has no such clause, but its own
// placement (NULL is the smallest value) is right unless asked otherwise; then an
// "expr IS NULL" key sorts first.
func (t orderTerm) render() sqlExpr {
	plain := sqlJoin(t.expr, sqlText(" "+string(t.direction)))
	if !t.nullable {
		return plain
	}

	clause := " NULLS LAST"
	if t.nullsFirst {
		clause = " NULLS FIRST"
	}

	spelled := map[tsqdialect.Name]sqlExpr{
		tsqdialect.MySQL:    plain,
		tsqdialect.Postgres: sqlJoin(plain, sqlText(clause)),
		tsqdialect.SQLite:   sqlJoin(plain, sqlText(clause)),
	}

	if t.nullsFirst != (t.direction != orderDesc) {
		key := " IS NULL ASC, "
		if t.nullsFirst {
			key = " IS NULL DESC, "
		}

		nullKey := t.expr
		if len(t.nullKey.parts) > 0 {
			nullKey = t.nullKey
		}

		spelled[tsqdialect.MySQL] = sqlJoin(nullKey, sqlText(key), plain)

		// MySQL orders a UNION by output columns only, not by expressions on them.
		if t.compound {
			delete(spelled, tsqdialect.MySQL)
		}
	}

	return sqlByDialect("NULLS FIRST/LAST on a set operation", spelled)
}

// optionalTables returns the tables an outer join can fill with NULLs: the joined
// table of a LEFT JOIN, everything before a RIGHT JOIN, and both sides of a FULL
// JOIN.
func (s *querySpec[O]) optionalTables() map[string]bool {
	optional := map[string]bool{}
	before := []string{s.From.TableName()}

	for _, j := range s.Joins {
		name := j.table.TableName()

		switch j.kind {
		case leftJoinType:
			optional[name] = true
		case rightJoinType, fullJoinType:
			for _, t := range before {
				optional[t] = true
			}

			if j.kind == fullJoinType {
				optional[name] = true
			}
		}

		before = append(before, name)
	}

	return optional
}

// canBeNull reports whether an expression with nullness n can be NULL in this
// query, and why.
func (s *querySpec[O]) canBeNull(n nullness) (bool, string) {
	switch {
	case n.always:
		return true, "it is nullable"
	case n.emptyGroup && len(s.GroupBy) == 0:
		return true, "an aggregate without GROUP BY is NULL over no rows"
	}

	if len(n.tables) > 0 && !isNilValue(s.From) {
		optional := s.optionalTables()
		for t := range n.tables {
			if optional[t] {
				return true, "table " + t + " is outer-joined"
			}
		}
	}

	return false, ""
}

// checkScanTargets refuses to read a value that can be NULL into a field that
// cannot hold it. It runs before rows are read rather than in Build, because a
// query used as a subquery or CTE never scans.
func (s *querySpec[O]) checkScanTargets() error {
	// Every operand of a set operation, nested ones included, is read through
	// the first one's columns.
	specs := s.operands()

	for i, col := range s.Selects {
		target := col.core()
		if target == nil || target.nullable {
			continue
		}

		for _, spec := range specs {
			if i >= len(spec.Selects) {
				continue
			}

			if null, why := spec.canBeNull(columnInfo(spec.Selects[i]).null); null {
				return fmt.Errorf("%s can be NULL here (%s) but is read into a field that cannot hold NULL; "+
					"use MapIntoNull with a nullable field, or Coalesce", target.name, why)
			}
		}
	}

	return nil
}

// sourceTables returns the FROM table and every joined table.
func (s *querySpec[O]) sourceTables() []Table {
	tables := []Table{s.From}
	for _, j := range s.Joins {
		tables = append(tables, j.table)
	}

	return tables
}

// operands returns s and every operand of its set operations, recursively.
func (s *querySpec[O]) operands() []*querySpec[O] {
	specs := []*querySpec[O]{s}
	for i := range s.SetOps {
		specs = append(specs, s.SetOps[i].spec.operands()...)
	}

	return specs
}

func (s *querySpec[O]) grouped() bool {
	if s.Distinct || len(s.SetOps) > 0 || len(s.GroupBy) > 0 || len(s.Having) > 0 {
		return true
	}

	for _, col := range s.Selects {
		info := columnInfo(col)
		if info.aggregate {
			return true
		}
	}

	// ORDER BY COUNT(x) aggregates the whole query as well.
	for _, ob := range s.OrderBys {
		if !isNilValue(ob.column) && columnInfo(ob.column).aggregate {
			return true
		}
	}

	return false
}

// render writes a complete statement, WITH clause included.
func (s *querySpec[O]) render(r *renderer, m renderMode) {
	s.writeWith(r)

	if m.count {
		// A limited query counts the rows it returns, as List reads them: the
		// count used to ignore Limit and Offset.
		if s.Limit != nil {
			r.writeText("SELECT COUNT(1) FROM (")
			s.writeBody(r, m)
			s.writeTail(r, renderMode{keyword: m.keyword})
			r.writeText(") AS _tsq_cnt")

			return
		}

		if s.grouped() {
			r.writeText("SELECT COUNT(1) FROM (")
			s.writeBody(r, m)
			r.writeText(") AS _tsq_cnt")

			return
		}

		r.writeText("SELECT COUNT(1)")
		s.writeFromWhere(r, m)

		return
	}

	// Whether a row exists does not depend on its columns or its order, so a query
	// that reads table rows asks for a constant. A grouped query, a set operation
	// and DISTINCT decide their rows by the select list, and a builder Limit or
	// Offset by the order, so those keep their full shape.
	if m.exists && !s.grouped() && s.Limit == nil {
		r.writeText("SELECT 1")
		s.writeFromWhere(r, m)
		r.writeText(" LIMIT 1")
		s.writeLock(r)

		return
	}

	s.writeBody(r, m)
	s.writeTail(r, m)
	s.writeLock(r)
}

// writeBody writes the query without ORDER BY, LIMIT and locks. Only the keyword
// and seek parts of m apply, and only to the first operand of a set operation.
func (s *querySpec[O]) writeBody(r *renderer, m renderMode) {
	s.writeChain(r, m, len(s.SetOps))
}

// writeChain writes the first operand and the first n set operations. A chain is
// evaluated left to right, as it reads; SQL instead binds INTERSECT tighter than
// UNION and EXCEPT on MySQL and PostgreSQL but not on SQLite. So the operations
// before an INTERSECT that follows a UNION or EXCEPT are grouped as a derived
// table, which every dialect evaluates first.
func (s *querySpec[O]) writeChain(r *renderer, m renderMode, n int) {
	ops := s.SetOps[:n]

	if split := regroupAt(ops); split > 0 {
		r.writeText("SELECT * FROM (")
		s.writeChain(r, m, split)
		r.writeText(") AS ")
		r.writeIdent("tsq_set")

		ops = ops[split:]
	} else {
		s.writeSimple(r, m)
	}

	for _, op := range ops {
		if c := op.op.capability(); c != "" {
			r.require(c)
		}

		r.writeText(" " + string(op.op) + " ")

		if len(op.spec.SetOps) > 0 {
			// A combined operand is grouped as a derived table: SQLite has no
			// parenthesized compound SELECT, and this spelling runs everywhere.
			r.writeText("SELECT * FROM (")
			op.spec.writeBody(r, renderMode{})
			r.writeText(") AS ")
			r.writeIdent("tsq_set")
		} else {
			op.spec.writeSimple(r, renderMode{})
		}
	}
}

// regroupAt returns the index of the last INTERSECT preceded by a UNION or
// EXCEPT, or 0 when the chain means the same flat on every dialect.
func regroupAt[O any](ops []setOperation[O]) int {
	loose := false
	split := 0

	for i, op := range ops {
		if op.op == intersectType || op.op == intersectAllType {
			if loose {
				split = i
			}

			continue
		}

		loose = true
	}

	return split
}

// selectAliases is the name each select item is written AS, empty for one written
// bare (a column reference, which every dialect names by the column).
//
// Rows are read by position, so an output name only matters where the query
// becomes a derived table: counting a grouped query, or grouping a set operation.
// MySQL refuses a derived table with two columns of one name (error 1060), so a
// repeated name is replaced after its first use.
func (s *querySpec[O]) selectAliases() []string {
	aliases := make([]string, len(s.Selects))
	named := make(map[string]bool, len(s.Selects))

	for _, col := range s.Selects {
		named[col.Name()] = true
	}

	used := make(map[string]bool, len(s.Selects))

	for i, col := range s.Selects {
		name := col.Name()
		if name != "" && used[name] {
			aliases[i] = repeatedName(col, named)
			named[aliases[i]] = true

			continue
		}

		used[name] = true

		if core := col.core(); core != nil && !core.plain && !core.bare && core.name != "" {
			aliases[i] = name
		}
	}

	return aliases
}

func (s *querySpec[O]) writeSimple(r *renderer, m renderMode) {
	aliases := s.selectAliases()
	cols := make([]sqlExpr, 0, len(s.Selects))

	for i, col := range s.Selects {
		item, _ := s.overGroups(r.dialect, columnInfo(col).sql, columnInfo(col).aggregate, true)

		if aliases[i] == "" {
			cols = append(cols, item)

			continue
		}

		cols = append(cols, sqlJoin(item, sqlText(" AS "), sqlIdent(aliases[i])))
	}

	r.writeText("SELECT ")

	if s.Distinct {
		r.writeText("DISTINCT ")
	}

	r.write(sqlList(", ", cols))
	s.writeFromWhere(r, m)

	if len(s.GroupBy) > 0 {
		groups := make([]sqlExpr, 0, len(s.GroupBy))
		for _, col := range s.GroupBy {
			groups = append(groups, s.selectedPosition(columnInfo(col).sql))
		}

		r.writeText(" GROUP BY ")
		r.write(sqlList(", ", groups))
	}

	if len(s.Having) > 0 {
		r.writeText(" HAVING ")
		r.write(s.havingOverGroups(r.dialect))
	}
}

// havingOverGroups is the HAVING clause. It is the conditions joined as they
// always were, unless the dialect needs one of them rewritten (see overGroups):
// then each condition stands in parentheses of its own.
func (s *querySpec[O]) havingOverGroups(d sqld.Dialect) sqlExpr {
	conds := make([]sqlExpr, 0, len(s.Having))
	changed := false

	for _, cond := range s.Having {
		info := conditionInfo(cond)
		over, rewritten := s.overGroups(d, info.sql, info.aggregate, false)
		changed = changed || rewritten

		conds = append(conds, sqlJoin(sqlText("("), over, sqlText(")")))
	}

	if !changed {
		return andAll(s.Having).sql
	}

	return sqlList(" AND ", conds)
}

// overGroups is e as the dialect takes it in a grouped query. GROUP BY UPPER(note)
// allows HAVING UPPER(note) <> 'X' and a selected LOWER(UPPER(note)) in the SQL
// standard, and two engines do not follow it all the way:
//
//   - MySQL knows a grouped expression only where it stands whole in the select
//     list or ORDER BY, and refuses the rest (errors 1054 and 1055).
//   - PostgreSQL compares expressions with their parameters, and every bound value
//     is a parameter of its own: GROUP BY qty + $1 and HAVING qty + $2 > $3 are two
//     expressions to it, whatever $1 and $2 hold.
//
// Within a group the expression has one value, so MAX of it is that value, and an
// aggregate is taken anywhere: each such occurrence becomes MAX(expression).
//
// Only what the dialect refuses is rewritten. An expression with an aggregate of
// its own is left alone (COUNT(UPPER(note)) is valid, and an aggregate in an
// aggregate is not); a grouped column needs none of this; and a selected item that
// is the grouped expression itself is what GROUP BY refers to (item is true for
// one of the select list).
func (s *querySpec[O]) overGroups(d sqld.Dialect, e sqlExpr, aggregate, item bool) (sqlExpr, bool) {
	if d.Name() == tsqdialect.SQLite || aggregate || len(s.GroupBy) == 0 {
		return e, false
	}

	// Expressions are compared as text with a mark for each bound value, the same
	// mark for the same value: the marks go back to values once the text is edited.
	bound := map[string]chunk{}

	marked := func(x sqlExpr) (string, bool) {
		r := newRenderer(d)
		r.write(x)

		stmt, err := r.finish()
		if err != nil {
			return "", false
		}

		var b strings.Builder

		for _, c := range stmt.chunks {
			var key string

			switch {
			case c.hasValue:
				key = fmt.Sprintf("\x00%T %#v\x00", c.value, c.value)
			case c.param != nil:
				key = fmt.Sprintf("\x00param %p\x00", c.param)
			default:
				b.WriteString(c.text)

				continue
			}

			bound[key] = c
			b.WriteString(key)
		}

		return b.String(), true
	}

	// The grouped expressions the dialect loses track of, longest first: one inside
	// another is then matched as part of the longer.
	var groups []string

	for _, g := range s.GroupBy {
		if core := g.core(); core != nil && core.plain {
			continue
		}

		spelled, ok := marked(columnInfo(g).sql)
		if !ok || spelled == "" {
			continue
		}

		if d.Name() == tsqdialect.MySQL || strings.Contains(spelled, "\x00") {
			groups = append(groups, spelled)
		}
	}

	if len(groups) == 0 {
		return e, false
	}

	slices.SortFunc(groups, func(a, b string) int { return len(b) - len(a) })

	whole, ok := marked(e)
	if !ok {
		return e, false
	}

	// The grouped expression itself: GROUP BY names the selected one by position,
	// and one without a bound value is known wherever it stands whole.
	if slices.Contains(groups, whole) && (item || !strings.Contains(whole, "\x00")) {
		return e, false
	}

	over := maxOfEach(whole, groups)
	if over == whole {
		return e, false
	}

	parts := []sqlExpr{}

	for i, piece := range strings.Split(over, "\x00") {
		if i%2 == 0 {
			parts = append(parts, sqlText(piece))

			continue
		}

		switch c := bound["\x00"+piece+"\x00"]; {
		case c.hasValue:
			parts = append(parts, sqlValue(c.value))
		default:
			parts = append(parts, sqlParam(c.param))
		}
	}

	// Spelled for this dialect; any other keeps e.
	return sqlForDialect("a grouped expression", func(to sqld.Dialect) (sqlExpr, bool) {
		if to.Name() == d.Name() {
			return sqlJoin(parts...), true
		}

		return e, true
	}), true
}

// maxOfEach wraps every occurrence in text of one of groups in MAX(...). The
// groups are tried longest first at each position and a match is not looked into
// again, and an occurrence that continues an identifier (XUPPER(...) for
// UPPER(...)) is not one.
func maxOfEach(text string, groups []string) string {
	var b strings.Builder

	for i := 0; i < len(text); {
		matched := ""

		if i == 0 || !identifierByte(text[i-1]) {
			for _, g := range groups {
				if strings.HasPrefix(text[i:], g) {
					matched = g

					break
				}
			}
		}

		if matched == "" {
			b.WriteByte(text[i])
			i++

			continue
		}

		b.WriteString("MAX(" + matched + ")")
		i += len(matched)
	}

	return b.String()
}

func identifierByte(c byte) bool {
	return c == '_' || c == '`' || c == '"' || c == '.' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// writeFromWhere writes FROM, the joins and WHERE, keeping the deleted rows of
// soft-delete tables out. Where that filter goes depends on the join: in WHERE for
// the FROM table and inner joins, in ON for a LEFT JOIN (in WHERE it would drop the
// preserved row). With a RIGHT or FULL JOIN in the query, a table can be on the
// preserved side of one join and the optional side of another, so every
// soft-delete table becomes a derived table of its live rows instead.
func (s *querySpec[O]) writeFromWhere(r *renderer, m renderMode) {
	outer := false

	for _, j := range s.Joins {
		if j.kind == rightJoinType || j.kind == fullJoinType {
			outer = true
		}
	}

	var live []Condition

	source := func(t Table) sqlExpr {
		if !t.softDeleted() {
			return t.source()
		}

		if outer {
			return liveSource(t)
		}

		return t.source()
	}

	r.writeText(" FROM ")
	r.write(source(s.From))

	if s.From.softDeleted() && !outer {
		live = append(live, newCondition(exprInfo{sql: liveRows(s.From)}))
	}

	for _, j := range s.Joins {
		if j.kind == fullJoinType {
			r.require(tsqdialect.CapabilityFullJoin)
		}

		r.writeText(" " + string(j.kind) + " ")
		r.write(source(j.table))

		on := j.on

		if j.table.softDeleted() && !outer {
			filter := newCondition(exprInfo{sql: liveRows(j.table)})
			if j.kind == leftJoinType {
				on = append(slices.Clone(on), filter)
			} else {
				live = append(live, filter)
			}
		}

		if len(on) > 0 {
			r.writeText(" ON ")
			r.write(andAll(on).sql)
		}
	}

	conds := slices.Clone(s.Filters)

	if m.keyword && len(s.KeywordSearch) > 0 {
		terms := make([]Condition, 0, len(s.KeywordSearch))
		pattern := sqlJoin(sqlParam(keywordParam.derive(paramContains)), sqlText(likeEscapeClause))

		for _, col := range s.KeywordSearch {
			info := columnInfo(col)
			terms = append(terms, newCondition(info.withSQL(sqlJoin(info.sql, sqlText(" LIKE "), pattern))))
		}

		conds = append(conds, Or(terms...))
	}

	conds = append(conds, live...)

	if m.seek != nil {
		conds = append(conds, newCondition(exprInfo{sql: *m.seek}))
	}

	if len(conds) > 0 {
		r.writeText(" WHERE ")
		r.write(andAll(conds).sql)
	}
}

// selectedPosition is expr for GROUP BY or ORDER BY. An expression with bound
// values that is also selected is written as its position in the select list:
// PostgreSQL numbers every placeholder anew, so CASE WHEN x > $1 in the select
// list and CASE WHEN x > $4 in GROUP BY are different expressions to it, and it
// refused the query. All three dialects take the position.
func (s *querySpec[O]) selectedPosition(expr sqlExpr) sqlExpr {
	if !bindsValues(expr) {
		return expr
	}

	key := exprKey(expr)
	for i, col := range s.Selects {
		if exprKey(columnInfo(col).sql) == key {
			return sqlText(strconv.Itoa(i + 1))
		}
	}

	return expr
}

// bindsValues reports whether e has a bound value or parameter.
func bindsValues(e sqlExpr) bool {
	for _, part := range e.parts {
		switch part.kind {
		case partValue, partParam:
			return true
		case partByDialect:
			for _, spelled := range part.byDialect {
				if bindsValues(spelled) {
					return true
				}
			}
		}
	}

	return false
}

// orderTerm renders an ORDER BY term. A compound query can only be ordered by its
// output column names, so the term drops its table there.
func (s *querySpec[O]) orderTerm(ob OrderBy) orderTerm {
	term := orderTerm{expr: s.selectedPosition(columnInfo(ob.column).sql), nullKey: columnInfo(ob.column).sql, direction: ob.direction, nullsFirst: ob.nulls.first(ob.direction)}
	term.aggregate = columnInfo(ob.column).aggregate
	term.nullable, _ = s.canBeNull(columnInfo(ob.column).null)

	// MySQL's "expr IS NULL" key is not in the select list, and a DISTINCT query
	// may only be ordered by what is (error 3065) unless every column the key
	// reads is selected itself. A selected expression is tested through its alias.
	if s.Distinct && len(s.SetOps) == 0 {
		key := exprKey(term.nullKey)

		for i, alias := range s.selectAliases() {
			if alias != "" && exprKey(columnInfo(s.Selects[i]).sql) == key {
				term.nullKey = sqlIdent(alias)

				break
			}
		}
	}

	if len(s.SetOps) > 0 && !isNilValue(ob.column) {
		term.expr = sqlIdent(ob.column.Name())
		term.nullKey = term.expr
		term.nullable = s.outputCanBeNull(ob.column.Name())
		term.compound = true
	}

	return term
}

// checkCompoundOrder refuses an ORDER BY term a set operation cannot follow. The
// combined result is ordered by its output columns, found by name, so a term must
// be one of them: a selected item, or a column reference named like exactly one.
// An expression such as Upper(col) used to render as the column it wraps, and the
// result was ordered by something else without a word.
func (s *querySpec[O]) checkCompoundOrder(ob OrderBy) error {
	if isNilValue(ob.column) || ob.column.core() == nil {
		return nil
	}

	core := ob.column.core()
	named, selected := 0, false

	var output *columnCore

	for _, col := range s.Selects {
		if col.Name() == ob.column.Name() {
			named++
			output = col.core()
		}

		selected = selected || col.core() == core
	}

	// A column that is not itself selected finds its output by name, and a derived
	// item is named after its source column: LENGTH(name) AS name. Ordering by
	// name then sorted by the length, so the output has to be that column.
	if !selected && named == 1 && (!output.bare || !sameSource(output, core)) {
		return fmt.Errorf("a set operation is ordered by its output columns, and the one named %s is %s, not the column; order by the selected column",
			ob.column.Name(), debugSQL(output.info.sql))
	}

	switch {
	case !selected && !core.plain && !core.bare:
		return fmt.Errorf("a set operation is ordered by its output columns, and %s is an expression; select it and order by the selected column", debugSQL(core.info.sql))
	case named == 0:
		return fmt.Errorf("a set operation is ordered by its output columns, and none is named %s", ob.column.Name())
	case named > 1:
		return fmt.Errorf("a set operation is ordered by its output columns, and more than one is named %s", ob.column.Name())
	}

	return nil
}

// sameSource reports whether two columns read the same column of the same table.
func sameSource(a, b *columnCore) bool {
	return !isNilValue(a.table) && !isNilValue(b.table) && a.table.TableName() == b.table.TableName() && a.name == b.name
}

// outputCanBeNull reports whether the named output column of a set operation can
// be NULL in any of its operands.
func (s *querySpec[O]) outputCanBeNull(name string) bool {
	specs := s.operands()

	for i, col := range s.Selects {
		if col.Name() != name {
			continue
		}

		for _, spec := range specs {
			if i < len(spec.Selects) {
				if null, _ := spec.canBeNull(columnInfo(spec.Selects[i]).null); null {
					return true
				}
			}
		}
	}

	return false
}

func (s *querySpec[O]) writeTail(r *renderer, m renderMode) {
	order := make([]orderTerm, 0, len(s.OrderBys))
	for _, ob := range s.OrderBys {
		order = append(order, s.orderTerm(ob))
	}

	if m.paged && len(m.order) > 0 {
		order = m.order
	}

	if len(order) > 0 {
		terms := make([]sqlExpr, 0, len(order))
		for _, term := range order {
			term.expr, _ = s.overGroups(r.dialect, term.expr, term.aggregate, false)
			term.nullKey, _ = s.overGroups(r.dialect, term.nullKey, term.aggregate, false)
			terms = append(terms, term.render())
		}

		r.writeText(" ORDER BY ")
		r.write(sqlList(", ", terms))
	}

	switch {
	case m.paged:
		r.writeText(" LIMIT ")
		r.writeValue(m.limit)

		if !m.keyset {
			r.writeText(" OFFSET ")
			r.writeValue(m.offset)
		}
	case s.Limit != nil:
		r.writeText(" LIMIT ")
		r.writeValue(*s.Limit)

		if s.Offset != nil {
			r.writeText(" OFFSET ")
			r.writeValue(*s.Offset)
		}
	case m.single:
		r.writeText(" LIMIT 1")
	}
}

func (s *querySpec[O]) writeLock(r *renderer) {
	if s.Lock.strength == "" {
		return
	}

	switch s.Lock.strength {
	case queryLockStrengthUpdate:
		r.require(tsqdialect.CapabilityForUpdate)
	case queryLockStrengthShare:
		r.require(tsqdialect.CapabilityForShare)
	}

	switch s.Lock.waitMode {
	case queryLockWaitNoWait:
		r.require(tsqdialect.CapabilityNoWait)
	case queryLockWaitSkipLocked:
		r.require(tsqdialect.CapabilitySkipLocked)
	}

	r.writeText(" " + string(s.Lock.strength))

	if s.Lock.waitMode != "" {
		r.writeText(" " + string(s.Lock.waitMode))
	}
}

// writeWith hoists every CTE the statement uses, dependencies first.
func (s *querySpec[O]) writeWith(r *renderer) {
	var ctes []cteTable

	seen := make(map[string]bool)

	var visit func(sources []Table)
	visit = func(sources []Table) {
		for _, t := range sources {
			cte, ok := t.(cteTable)
			if !ok || seen[cte.name] {
				continue
			}

			seen[cte.name] = true
			visit(cte.body.sources())
			ctes = append(ctes, cte)
		}
	}

	visit(s.sources())

	if len(ctes) == 0 {
		return
	}

	r.require(tsqdialect.CapabilityCTE)
	r.writeText("WITH ")

	for i, cte := range ctes {
		if i > 0 {
			r.writeText(", ")
		}

		r.writeIdent(cte.name)
		r.writeText(" AS (")
		cte.body.renderQuery(r)
		r.writeText(")")
	}

	r.writeText(" ")
}

// sources lists the FROM and JOIN items of the query and of its set operands.
func (s *querySpec[O]) sources() []Table {
	if isNilValue(s.From) {
		return nil
	}

	result := []Table{s.From}
	for _, j := range s.Joins {
		result = append(result, j.table)
	}

	for _, op := range s.SetOps {
		result = append(result, op.spec.sources()...)
	}

	return result
}

// expressions returns every expression of the query itself (not its set operands).
func (s *querySpec[O]) expressions() []exprInfo {
	var infos []exprInfo

	for _, col := range s.Selects {
		infos = append(infos, columnInfo(col))
	}

	for _, c := range s.Filters {
		infos = append(infos, conditionInfo(c))
	}

	for _, col := range s.KeywordSearch {
		infos = append(infos, columnInfo(col))
	}

	for _, j := range s.Joins {
		for _, c := range j.on {
			infos = append(infos, conditionInfo(c))
		}
	}

	for _, col := range s.GroupBy {
		infos = append(infos, columnInfo(col))
	}

	for _, c := range s.Having {
		infos = append(infos, conditionInfo(c))
	}

	for _, ob := range s.OrderBys {
		infos = append(infos, columnInfo(ob.column))
	}

	return infos
}

// validate checks the structure of the query. Dialect support is not checked
// here: one query may run on several dialects, and each rejects what it lacks when
// the query is rendered for it.
func (s *querySpec[O]) validate(outer map[string]Table) error {
	if isNilValue(s.From) {
		return errors.New("query has no FROM table")
	}

	if len(s.Selects) == 0 {
		return errors.New("query selects no columns")
	}

	for _, t := range s.sources() {
		if err := tableErr(t); err != nil {
			return err
		}
	}

	for _, info := range s.expressions() {
		if info.err != nil {
			return info.err
		}
	}

	if s.Offset != nil && s.Limit == nil {
		return errors.New("offset requires limit: a bare OFFSET is a syntax error on mysql and sqlite")
	}

	if err := s.validateJoinGraph(outer); err != nil {
		return err
	}

	if err := s.checkPlacement(); err != nil {
		return err
	}

	if err := s.checkGrouping(); err != nil {
		return err
	}

	if len(s.SetOps) > 0 {
		for _, ob := range s.OrderBys {
			if err := s.checkCompoundOrder(ob); err != nil {
				return err
			}
		}
	}

	for _, op := range s.SetOps {
		if len(op.spec.Selects) != len(s.Selects) {
			return fmt.Errorf("%s requires matching select column counts: left=%d right=%d",
				op.op, len(s.Selects), len(op.spec.Selects))
		}

		// Every operand's rows are read through the first operand's columns, by
		// position: an operand that selects name and email the other way round
		// had its values read into each other's fields without a word.
		if err := sameScanTargets(s.Selects, op.spec.Selects); err != nil {
			return fmt.Errorf("%s: %w", op.op, err)
		}

		if len(s.KeywordSearch) > 0 || len(op.spec.KeywordSearch) > 0 {
			return errors.New("set operations do not support keyword search")
		}

		if err := op.spec.validate(outer); err != nil {
			return err
		}
	}

	return s.validateCTEs()
}

// checkPlacement refuses what every dialect refuses at execution: an aggregate in
// WHERE, in a JOIN condition or in GROUP BY; a DISTINCT query ordered by what it
// does not select (PostgreSQL and MySQL refuse it, SQLite orders by an arbitrary
// row of each group); and a row lock over an outer join (PostgreSQL cannot lock
// the nullable side).
func (s *querySpec[O]) checkPlacement() error {
	for _, cond := range s.Filters {
		if conditionInfo(cond).aggregate {
			return errors.New("WHERE cannot use an aggregate; filter groups with Having")
		}
	}

	for _, j := range s.Joins {
		for _, cond := range j.on {
			if conditionInfo(cond).aggregate {
				return fmt.Errorf("the condition of the join of %s cannot use an aggregate", j.table.TableName())
			}
		}

		if s.Lock.strength != "" && j.kind != innerJoinType && j.kind != crossJoinType {
			return fmt.Errorf("a row lock cannot cover the %s of %s: PostgreSQL does not lock the side that can be NULL; lock the rows with an inner join, or in a query of their own", j.kind, j.table.TableName())
		}
	}

	for _, g := range s.GroupBy {
		if columnInfo(g).aggregate {
			return errors.New("GROUP BY cannot use an aggregate")
		}
	}

	if s.Distinct && len(s.SetOps) == 0 {
		selected := make(map[string]bool, len(s.Selects))
		for _, col := range s.Selects {
			selected[exprKey(columnInfo(col).sql)] = true
		}

		for _, ob := range s.OrderBys {
			if isNilValue(ob.column) {
				continue
			}

			if info := columnInfo(ob.column); !selected[exprKey(info.sql)] {
				return fmt.Errorf("a DISTINCT query is ordered by %s, which it does not select; select it, or drop DISTINCT", debugSQL(info.sql))
			}
		}
	}

	return nil
}

func (s *querySpec[O]) correlatedNames() map[string]Table {
	tables := make(map[string]Table, len(s.Correlated))
	for _, t := range s.Correlated {
		tables[t.TableName()] = t
	}

	return tables
}

func (s *querySpec[O]) validateJoinGraph(outer map[string]Table) error {
	introduced := map[string]bool{s.From.TableName(): true}

	correlated := s.correlatedNames()
	maps.Copy(correlated, outer)

	for _, j := range s.Joins {
		name := j.table.TableName()
		if introduced[name] {
			return fmt.Errorf("table %s is already in the query; alias it to join it again", name)
		}

		if j.kind != crossJoinType {
			if len(j.on) == 0 {
				return fmt.Errorf("%s %s requires an ON condition", j.kind, name)
			}

			onTables := andAll(j.on).allTables()
			if _, ok := onTables[name]; !ok {
				return fmt.Errorf("%s %s: the ON condition must reference %s", j.kind, name, name)
			}

			connected := false

			for other := range onTables {
				if other == name {
					continue
				}

				// A table of the enclosing query (Correlate) is in scope in ON too.
				if !introduced[other] && correlated[other] == nil {
					return fmt.Errorf("%s %s: the ON condition references %s, which is not joined before it", j.kind, name, other)
				}

				connected = true
			}

			if !connected {
				return fmt.Errorf("%s %s: the ON condition must reference a table already in the query", j.kind, name)
			}
		}

		introduced[name] = true
	}

	for name := range s.correlatedNames() {
		if introduced[name] {
			return fmt.Errorf(
				"table %s is declared with Correlate but is also in this query's own FROM/JOIN; "+
					"the local table would shadow the outer one and the predicate would stop being correlated",
				name)
		}
	}

	for _, info := range s.expressions() {
		for name := range info.allTables() {
			if introduced[name] {
				continue
			}

			if _, ok := correlated[name]; ok {
				continue
			}

			return fmt.Errorf(
				"table %s is referenced but is not in this query's FROM/JOIN; join it, or, "+
					"if it belongs to an enclosing query, declare it with Correlate(%s)",
				name, name)
		}
	}

	return nil
}

func (s *querySpec[O]) validateCTEs() error {
	visiting := make(map[string]bool)
	done := make(map[string]bool)
	// One WITH clause names every CTE of the statement, so two different bodies
	// under one name would render as the first, and the second would run its query
	// with the wrong parameters or none.
	bodies := make(map[string]cteQuery)

	var visit func(sources []Table) error
	visit = func(sources []Table) error {
		for _, t := range sources {
			cte, ok := t.(cteTable)
			if !ok {
				continue
			}

			if body, named := bodies[cte.name]; named && body != cte.body {
				return fmt.Errorf("two different CTEs are named %s; one statement can hold only one", cte.name)
			}

			bodies[cte.name] = cte.body

			if done[cte.name] {
				continue
			}

			if visiting[cte.name] {
				return fmt.Errorf("cte %s depends on itself", cte.name)
			}

			visiting[cte.name] = true

			if err := cte.body.err(); err != nil {
				return fmt.Errorf("cte %s: %w", cte.name, err)
			}

			if err := visit(cte.body.sources()); err != nil {
				return err
			}

			visiting[cte.name] = false
			done[cte.name] = true
		}

		return nil
	}

	return visit(s.sources())
}

// cteSpec is a query used as a CTE body.
type cteSpec[O any] struct {
	spec     querySpec[O]
	buildErr error
}

func (c *cteSpec[O]) err() error {
	if c.buildErr != nil {
		return c.buildErr
	}

	if len(c.spec.KeywordSearch) > 0 {
		return errors.New("a cte cannot use keyword search")
	}

	if c.spec.Lock.strength != "" {
		return errors.New("a cte cannot lock rows")
	}

	// A CTE is written before the query that uses it and sees no outer table.
	if len(c.spec.Correlated) > 0 {
		return errors.New("a cte cannot use Correlate: it sees no outer query; join the tables it needs")
	}

	// Its columns are found by name, so two of one name would make a reference to
	// either ambiguous (SUM(amount) and MAX(amount) are both named amount).
	seen := make(map[string]bool, len(c.spec.Selects))
	for _, name := range c.outputNames() {
		if seen[name] {
			return fmt.Errorf("a cte selects two columns named %s; its columns are found by name, so select one of them from a column of another name", name)
		}

		seen[name] = true
	}

	return c.spec.validate(nil)
}

func (c *cteSpec[O]) renderQuery(r *renderer) {
	c.spec.writeBody(r, renderMode{})
	c.spec.writeTail(r, renderMode{})
}

func (c *cteSpec[O]) correlatedTables() map[string]Table { return nil }

func (c *cteSpec[O]) readsTable(name string) bool {
	return slices.ContainsFunc(c.spec.sources(), func(t Table) bool { return t.TableName() == name })
}

func (c *cteSpec[O]) sources() []Table { return c.spec.sources() }

func (c *cteSpec[O]) nullableOutput(name string) bool {
	return c.spec.outputCanBeNull(name)
}

// selectItem renders one entry of a SELECT list. A column reference is named by
// the column on every dialect; any other expression is named by the dialect (the
// expression's text, or PostgreSQL's "sum"), so it is given its name with AS, the
// name a CTE or a set operation's ORDER BY finds it by.
// repeatedName names a select item whose name an earlier item already has, so it
// reads in the SQL: a plain column of another table is table_column
// (categories_name), anything else column_2, column_3. The name is new among
// every name the select list uses.
func repeatedName(col SQLColumn, taken map[string]bool) string {
	name := col.Name()

	if core := col.core(); core != nil && (core.plain || core.bare) {
		for table := range columnInfo(col).tables {
			if candidate := table + "_" + name; !taken[candidate] {
				return candidate
			}
		}
	}

	for n := 2; ; n++ {
		if candidate := fmt.Sprintf("%s_%d", name, n); !taken[candidate] {
			return candidate
		}
	}
}

func (c *cteSpec[O]) outputNames() []string {
	names := make([]string, 0, len(c.spec.Selects))
	for _, col := range c.spec.Selects {
		names = append(names, col.Name())
	}

	return names
}

// sameScanTargets refuses operands whose columns at one position read into
// different fields of O.
func sameScanTargets[O any](left, right []BoundColumn[O]) error {
	holder := new(O)

	for i := range left {
		l, r := left[i].core(), right[i].core()
		if l == nil || r == nil || l.scan == nil || r.scan == nil {
			continue
		}

		if reflect.ValueOf(l.scan(holder)).Pointer() != reflect.ValueOf(r.scan(holder)).Pointer() {
			return fmt.Errorf("column %d reads into another field than the first operand's (%s, %s); select the operands' columns in the same order",
				i+1, left[i].Name(), right[i].Name())
		}
	}

	return nil
}

// checkGrouping refuses a grouped query that reads a column neither grouped nor
// aggregated: SQLite (and MySQL without ONLY_FULL_GROUP_BY) returns a value from an
// arbitrary row of the group, and PostgreSQL refuses it at execution. A query is
// grouped by GROUP BY, or by an aggregate in its select list or HAVING. A column is
// allowed when an item is a GROUP BY expression itself, when the column is
// grouped, or when its table's primary key is (the functional dependence
// PostgreSQL and MySQL accept). An outer table of a correlated subquery is a
// constant for each outer row.
func (s *querySpec[O]) checkGrouping() error {
	grouped := len(s.GroupBy) > 0 || len(s.Having) > 0
	for _, col := range s.Selects {
		grouped = grouped || columnInfo(col).aggregate
	}

	for _, ob := range s.OrderBys {
		grouped = grouped || (!isNilValue(ob.column) && columnInfo(ob.column).aggregate)
	}

	if !grouped {
		return nil
	}

	exprs := make(map[string]bool, len(s.GroupBy))
	columns := make(map[columnKey]bool, len(s.GroupBy))

	for _, g := range s.GroupBy {
		info := columnInfo(g)
		exprs[exprKey(info.sql)] = true

		if core := g.core(); core != nil && core.plain && !isNilValue(core.table) {
			columns[columnKey{core.table.TableName(), core.name}] = true
		}
	}

	// A soft-delete table under a RIGHT or FULL JOIN is read as a derived table
	// of its live rows (writeFromWhere), and PostgreSQL infers nothing from the
	// key of a derived table: its other columns have to be grouped.
	outerJoined := slices.ContainsFunc(s.Joins, func(j join) bool { return j.kind == rightJoinType || j.kind == fullJoinType })

	keyed := map[string]bool{}

	for _, table := range s.sources() {
		if outerJoined && table.softDeleted() {
			continue
		}

		if def := table.definition(); def != nil && def.primaryKey != nil && columns[columnKey{table.TableName(), def.primaryKey.name}] {
			keyed[table.TableName()] = true
		}
	}

	outer := map[string]bool{}
	for _, table := range s.Correlated {
		outer[table.TableName()] = true
	}

	check := func(what string, info exprInfo) error {
		key := exprKey(info.sql)
		if exprs[key] {
			return nil
		}

		// A column read only inside a grouped expression is grouped: GROUP BY
		// UPPER(note) allows HAVING UPPER(note) <> 'X' and LOWER(UPPER(note)). The
		// SQL standard, PostgreSQL and SQLite take both; MySQL matches a grouped
		// expression only where it is repeated whole in the select list or ORDER BY,
		// and refuses the rest when it runs (errors 1054 and 1055). That is the
		// engine's to say, as every dialect limit is: Build checks structure.
		rest := key
		for grouped := range exprs {
			rest = strings.ReplaceAll(rest, grouped, "{grouped}")
		}

		for _, bare := range info.bare {
			if columns[bare] || keyed[bare.table] || outer[bare.table] {
				continue
			}

			if !strings.Contains(rest, `"`+bare.table+`"."`+bare.column+`"`) {
				continue
			}

			return fmt.Errorf("%s reads %s, which is neither in GROUP BY nor inside an aggregate; group by it or aggregate it", what, bare)
		}

		return nil
	}

	for _, col := range s.Selects {
		if err := check("the selected "+col.Name(), columnInfo(col)); err != nil {
			return err
		}
	}

	for _, cond := range s.Having {
		if err := check("HAVING", conditionInfo(cond)); err != nil {
			return err
		}
	}

	for _, ob := range s.OrderBys {
		if !isNilValue(ob.column) {
			if err := check("ORDER BY", columnInfo(ob.column)); err != nil {
				return err
			}
		}
	}

	return nil
}
