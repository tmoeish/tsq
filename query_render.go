package tsq

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
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
	direction sortOrder
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

	if t.nullsFirst != (t.direction != orderDesc) {
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

func (s *querySpec[O]) writeSimple(r *renderer, m renderMode) {
	// Rows are read by position, so an output name only matters where the query
	// becomes a derived table: counting a grouped query, or grouping a set
	// operation. MySQL refuses a derived table with two columns of one name
	// (error 1060), so a repeated name is replaced after its first use.
	cols := make([]sqlExpr, 0, len(s.Selects))
	named := make(map[string]bool, len(s.Selects))

	for i, col := range s.Selects {
		name := col.Name()
		if name != "" && named[name] {
			cols = append(cols, sqlJoin(columnInfo(col).sql, sqlText(" AS "), sqlIdent(fmt.Sprintf("tsq_c%d", i+1))))
			continue
		}

		named[name] = true

		cols = append(cols, selectItem(col))
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

	for _, col := range s.Selects {
		if col.Name() == ob.column.Name() {
			named++
		}

		selected = selected || col.core() == core
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
func selectItem(col SQLColumn) sqlExpr {
	info := columnInfo(col)
	if core := col.core(); core == nil || core.plain || core.bare || core.name == "" {
		return info.sql
	}

	return sqlJoin(info.sql, sqlText(" AS "), sqlIdent(col.Name()))
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

	if !grouped {
		return nil
	}

	exprs := make(map[string]bool, len(s.GroupBy))
	columns := make(map[columnKey]bool, len(s.GroupBy))

	for _, g := range s.GroupBy {
		info := columnInfo(g)
		exprs[debugSQL(info.sql)] = true

		if core := g.core(); core != nil && core.plain && !isNilValue(core.table) {
			columns[columnKey{core.table.TableName(), core.name}] = true
		}
	}

	keyed := map[string]bool{}

	for _, table := range s.sources() {
		if def := table.definition(); def != nil && def.primaryKey != nil && columns[columnKey{table.TableName(), def.primaryKey.name}] {
			keyed[table.TableName()] = true
		}
	}

	outer := map[string]bool{}
	for _, table := range s.Correlated {
		outer[table.TableName()] = true
	}

	check := func(what string, info exprInfo) error {
		if exprs[debugSQL(info.sql)] {
			return nil
		}

		for _, key := range info.bare {
			if !columns[key] && !keyed[key.table] && !outer[key.table] {
				return fmt.Errorf("%s reads %s, which is neither in GROUP BY nor inside an aggregate; group by it or aggregate it", what, key)
			}
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
