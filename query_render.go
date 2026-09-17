package tsq

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
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
	case intersectType, intersectAllType:
		return tsqdialect.CapabilityIntersect
	case exceptType, exceptAllType:
		return tsqdialect.CapabilityExcept
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
	// paged replaces ORDER BY with order and appends LIMIT/OFFSET.
	paged bool
	// seek, for keyset paging, is ANDed into WHERE and drops the OFFSET.
	seek   *sqlExpr
	order  []orderTerm
	limit  int
	offset int
}

type orderTerm struct {
	expr      sqlExpr
	direction Order
	// nullable terms place NULLs explicitly; nullsFirst says where.
	nullable   bool
	nullsFirst bool
	// compound terms order a set operation by output name.
	compound bool
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

	if t.nullsFirst != (t.direction != DESC) {
		key := " IS NULL ASC, "
		if t.nullsFirst {
			key = " IS NULL DESC, "
		}

		spelled[tsqdialect.MySQL] = sqlJoin(t.expr, sqlText(key), plain)

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
	before := []string{s.From.Name()}

	for _, j := range s.Joins {
		name := j.table.Name()

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
	// Every operand of a set operation is read through the first one's columns.
	specs := []*querySpec[O]{s}
	for i := range s.SetOps {
		specs = append(specs, &s.SetOps[i].spec)
	}

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

	return false
}

// render writes a complete statement, WITH clause included.
func (s *querySpec[O]) render(r *renderer, m renderMode) {
	s.writeWith(r)

	if m.count {
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

	s.writeBody(r, m)
	s.writeTail(r, m)
	s.writeLock(r)
}

// writeBody writes the query without ORDER BY, LIMIT and locks. Only the keyword
// and seek parts of m apply, and only to the first operand of a set operation.
func (s *querySpec[O]) writeBody(r *renderer, m renderMode) {
	s.writeSimple(r, m)

	for _, op := range s.SetOps {
		if c := op.op.capability(); c != "" {
			r.require(c)
		}

		r.writeText(" " + string(op.op) + " ")

		if len(op.spec.SetOps) > 0 {
			r.writeText("(")
			op.spec.writeBody(r, renderMode{})
			r.writeText(")")
		} else {
			op.spec.writeSimple(r, renderMode{})
		}
	}
}

func (s *querySpec[O]) writeSimple(r *renderer, m renderMode) {
	cols := make([]sqlExpr, 0, len(s.Selects))
	for _, col := range s.Selects {
		cols = append(cols, columnInfo(col).sql)
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
			groups = append(groups, columnInfo(col).sql)
		}

		r.writeText(" GROUP BY ")
		r.write(sqlList(", ", groups))
	}

	if len(s.Having) > 0 {
		r.writeText(" HAVING ")
		r.write(andAll(s.Having).sql)
	}
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
			r.require(tsqdialect.CapabilityFullOuterJoin)
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

// orderTerm renders an ORDER BY term. A compound query can only be ordered by its
// output column names, so the term drops its table there.
func (s *querySpec[O]) orderTerm(ob OrderBy) orderTerm {
	term := orderTerm{expr: columnInfo(ob.column).sql, direction: ob.direction, nullsFirst: ob.nulls.first(ob.direction)}
	term.nullable, _ = s.canBeNull(columnInfo(ob.column).null)

	if len(s.SetOps) > 0 && !isNilValue(ob.column) {
		term.expr = sqlIdent(ob.column.Name())
		term.nullable = s.outputCanBeNull(ob.column.Name())
		term.compound = true
	}

	return term
}

// outputCanBeNull reports whether the named output column of a set operation can
// be NULL in any of its operands.
func (s *querySpec[O]) outputCanBeNull(name string) bool {
	specs := []*querySpec[O]{s}
	for i := range s.SetOps {
		specs = append(specs, &s.SetOps[i].spec)
	}

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
			terms = append(terms, term.render())
		}

		r.writeText(" ORDER BY ")
		r.write(sqlList(", ", terms))
	}

	switch {
	case m.paged:
		r.writeText(" LIMIT ")
		r.writeValue(m.limit)

		if m.seek == nil {
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
		r.require(tsqdialect.CapabilitySelectForUpdate)
	case queryLockStrengthShare:
		r.require(tsqdialect.CapabilitySelectForShare)
	}

	switch s.Lock.waitMode {
	case queryLockWaitNoWait:
		r.require(tsqdialect.CapabilitySelectForNoWait)
	case queryLockWaitSkipLocked:
		r.require(tsqdialect.CapabilitySelectForSkipLocked)
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
			if a, ok := t.(aliasTable); ok {
				t = a.base
			}

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

	for _, op := range s.SetOps {
		if len(op.spec.Selects) != len(s.Selects) {
			return fmt.Errorf("%s requires matching select column counts: left=%d right=%d",
				op.op, len(s.Selects), len(op.spec.Selects))
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

func (s *querySpec[O]) correlatedNames() map[string]Table {
	tables := make(map[string]Table, len(s.Correlated))
	for _, t := range s.Correlated {
		tables[t.Name()] = t
	}

	return tables
}

func (s *querySpec[O]) validateJoinGraph(outer map[string]Table) error {
	introduced := map[string]bool{s.From.Name(): true}

	correlated := s.correlatedNames()
	maps.Copy(correlated, outer)

	for _, j := range s.Joins {
		name := j.table.Name()
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

				if !introduced[other] {
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

	var visit func(sources []Table) error
	visit = func(sources []Table) error {
		for _, t := range sources {
			if a, ok := t.(aliasTable); ok {
				t = a.base
			}

			cte, ok := t.(cteTable)
			if !ok || done[cte.name] {
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

	return c.spec.validate(nil)
}

func (c *cteSpec[O]) renderQuery(r *renderer) {
	c.spec.writeBody(r, renderMode{})
	c.spec.writeTail(r, renderMode{})
}

func (c *cteSpec[O]) correlatedTables() map[string]Table { return nil }

func (c *cteSpec[O]) sources() []Table { return c.spec.sources() }

func (c *cteSpec[O]) nullableOutput(name string) bool {
	for _, col := range c.spec.Selects {
		if col.Name() == name {
			null, _ := c.spec.canBeNull(columnInfo(col).null)
			return null
		}
	}

	return false
}

func (c *cteSpec[O]) outputNames() []string {
	names := make([]string, 0, len(c.spec.Selects))
	for _, col := range c.spec.Selects {
		names = append(names, col.Name())
	}

	return names
}
