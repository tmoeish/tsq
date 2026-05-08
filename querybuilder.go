package tsq

import "github.com/juju/errors"

// JoinType represents different types of SQL joins.
type JoinType string

const (
	LeftJoinType  JoinType = "LEFT JOIN"
	InnerJoinType JoinType = "INNER JOIN"
	RightJoinType JoinType = "RIGHT JOIN"
	FullJoinType  JoinType = "FULL JOIN"
	CrossJoinType JoinType = "CROSS JOIN"
)

// SetOperationType represents SQL compound query operators.
type SetOperationType string

const (
	// UnionType combines distinct rows from both queries.
	UnionType SetOperationType = "UNION"
	// UnionAllType combines all rows from both queries, preserving duplicates.
	UnionAllType SetOperationType = "UNION ALL"
	// IntersectType keeps only rows present in both queries.
	IntersectType SetOperationType = "INTERSECT"
	// IntersectAllType keeps rows present in both queries, preserving duplicate counts.
	IntersectAllType SetOperationType = "INTERSECT ALL"
	// ExceptType removes rows present in the right query from the left query.
	ExceptType SetOperationType = "EXCEPT"
	// ExceptAllType removes rows present in the right query from the left query while preserving duplicate counts.
	ExceptAllType SetOperationType = "EXCEPT ALL"
)

// QueryBuilder builds a structured query specification for a result owner.
type QueryBuilder[O Owner] struct {
	spec     QuerySpec[O]
	buildErr error

	selectCalled   bool
	fromCalled     bool
	whereCalled    bool
	groupByCalled  bool
	havingCalled   bool
	kwSearchCalled bool
}

// join represents any type of JOIN operation.
type join struct {
	joinType JoinType
	table    Table
	on       []Condition
}

type setOperation[O Owner] struct {
	op   SetOperationType
	spec QuerySpec[O]
}

func newQueryBuilder[O Owner]() *QueryBuilder[O] {
	return &QueryBuilder[O]{
		spec: QuerySpec[O]{
			Selects: make([]BoundColumn[O], 0),
			Joins:   make([]join, 0),
			GroupBy: make([]SQLColumn, 0),
			Having:  make([]Condition, 0),
			SetOps:  make([]setOperation[O], 0),
		},
	}
}

func (qb *QueryBuilder[O]) ensureInitialized() *QueryBuilder[O] {
	if qb == nil {
		qb = newQueryBuilder[O]()
		qb.buildErr = errors.New("query builder cannot be nil")

		return qb
	}

	if qb.spec.Selects == nil {
		qb.spec.Selects = make([]BoundColumn[O], 0)
	}

	if qb.spec.Joins == nil {
		qb.spec.Joins = make([]join, 0)
	}

	if qb.spec.GroupBy == nil {
		qb.spec.GroupBy = make([]SQLColumn, 0)
	}

	if qb.spec.Having == nil {
		qb.spec.Having = make([]Condition, 0)
	}

	if qb.spec.SetOps == nil {
		qb.spec.SetOps = make([]setOperation[O], 0)
	}

	return qb
}

// Select creates a new QueryBuilder with the specified owner-constrained columns.
func Select[O Owner](cols ...BoundColumn[O]) *QueryBuilder[O] {
	qb := newQueryBuilder[O]()
	qb.Select(cols...)

	return qb
}

// From creates a new QueryBuilder with the specified base table.
func From[O Owner](table Table) *QueryBuilder[O] {
	return newQueryBuilder[O]().From(table)
}

// Select sets the projected columns for the query.
// Existing selected columns are replaced.
func (qb *QueryBuilder[O]) Select(cols ...BoundColumn[O]) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if qb.selectCalled {
		qb.setBuildError(errors.New("Select() can only be called once"))
		return qb
	}
	qb.selectCalled = true

	qb.spec.Selects = make([]BoundColumn[O], 0, len(cols))
	qb.addSelectColumns(cols...)

	return qb
}

func (qb *QueryBuilder[O]) appendSetOperation(op SetOperationType, other *QueryBuilder[O]) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if other == nil {
		qb.setBuildError(errors.New("set operation query builder cannot be nil"))
		return qb
	}

	other = other.ensureInitialized()
	if other.buildErr != nil {
		qb.setBuildError(errors.Trace(other.buildErr))
		return qb
	}

	if len(qb.spec.Selects) == 0 || len(other.spec.Selects) == 0 {
		qb.setBuildError(errors.New("set operations require both queries to select at least one column"))
		return qb
	}

	if len(qb.spec.Selects) != len(other.spec.Selects) {
		qb.setBuildError(errors.Errorf(
			"set operation %s requires matching select column counts: left=%d right=%d",
			op,
			len(qb.spec.Selects),
			len(other.spec.Selects),
		))

		return qb
	}

	if len(qb.spec.KeywordSearch) > 0 || len(other.spec.KeywordSearch) > 0 {
		qb.setBuildError(errors.New("set operations do not support keyword search"))
		return qb
	}

	qb.spec.SetOps = append(qb.spec.SetOps, setOperation[O]{
		op:   op,
		spec: cloneQuerySpec(other.spec),
	})

	return qb
}

// Union appends a UNION clause to the current query.
func (qb *QueryBuilder[O]) Union(other *QueryBuilder[O]) *QueryBuilder[O] {
	return qb.appendSetOperation(UnionType, other)
}

// UnionAll appends a UNION ALL clause to the current query.
func (qb *QueryBuilder[O]) UnionAll(other *QueryBuilder[O]) *QueryBuilder[O] {
	return qb.appendSetOperation(UnionAllType, other)
}

// Intersect appends an INTERSECT clause to the current query.
func (qb *QueryBuilder[O]) Intersect(other *QueryBuilder[O]) *QueryBuilder[O] {
	return qb.appendSetOperation(IntersectType, other)
}

// IntersectAll appends an INTERSECT ALL clause to the current query.
func (qb *QueryBuilder[O]) IntersectAll(other *QueryBuilder[O]) *QueryBuilder[O] {
	return qb.appendSetOperation(IntersectAllType, other)
}

// Except appends an EXCEPT clause to the current query.
func (qb *QueryBuilder[O]) Except(other *QueryBuilder[O]) *QueryBuilder[O] {
	return qb.appendSetOperation(ExceptType, other)
}

// ExceptAll appends an EXCEPT ALL clause to the current query.
func (qb *QueryBuilder[O]) ExceptAll(other *QueryBuilder[O]) *QueryBuilder[O] {
	return qb.appendSetOperation(ExceptAllType, other)
}

func (qb *QueryBuilder[O]) setBuildError(err error) {
	if qb == nil || err == nil || qb.buildErr != nil {
		return
	}

	qb.buildErr = err
}

func (qb *QueryBuilder[O]) addSelectColumns(cols ...BoundColumn[O]) {
	for _, col := range cols {
		if err := validateBoundColumn(col); err != nil {
			qb.setBuildError(errors.Trace(err))
			continue
		}

		qb.spec.Selects = append(qb.spec.Selects, col)
	}
}

func (qb *QueryBuilder[O]) appendColumn(target *[]SQLColumn, col SQLColumn) {
	if _, err := validateColumnInput(col); err != nil {
		qb.setBuildError(errors.Trace(err))
		return
	}

	*target = append(*target, col)
}

func (qb *QueryBuilder[O]) appendSearchColumn(target *[]SearchColumn, col SearchColumn) {
	if err := validateSearchColumn(col); err != nil {
		qb.setBuildError(errors.Trace(err))
		return
	}

	*target = append(*target, col)
}

func (qb *QueryBuilder[O]) appendCondition(target *[]Condition, cond Condition) {
	if _, _, _, err := validateConditionInput(cond); err != nil {
		qb.setBuildError(errors.Trace(err))
		return
	}

	*target = append(*target, cond)
}

// From sets the base table for the query.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) From(table Table) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if qb.fromCalled {
		qb.setBuildError(errors.New("From() can only be called once"))
		return qb
	}
	qb.fromCalled = true

	if err := validateTableInput(table, "from table"); err != nil {
		qb.setBuildError(errors.Trace(err))
		return qb
	}

	qb.spec.From = table

	return qb
}

// Join adds an INNER JOIN clause.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) Join(table Table, conds ...Condition) *QueryBuilder[O] {
	return qb.addJoin(InnerJoinType, table, conds...)
}

// LeftJoin adds a LEFT JOIN clause with ON conditions joined by AND.
// To join a table to itself, pass an aliased table and rebound columns.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) LeftJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	return qb.addJoin(LeftJoinType, table, conds...)
}

// InnerJoin adds an INNER JOIN clause with ON conditions joined by AND.
// To join a table to itself, pass an aliased table and rebound columns.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) InnerJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	return qb.addJoin(InnerJoinType, table, conds...)
}

// RightJoin adds a RIGHT JOIN clause with ON conditions joined by AND.
// To join a table to itself, pass an aliased table and rebound columns.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) RightJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	return qb.addJoin(RightJoinType, table, conds...)
}

// FullJoin adds a FULL JOIN clause with ON conditions joined by AND. SQL generation is supported,
// but execution still depends on the target dialect supporting FULL JOIN.
// To join a table to itself, pass an aliased table and rebound columns.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) FullJoin(table Table, conds ...Condition) *QueryBuilder[O] {
	return qb.addJoin(FullJoinType, table, conds...)
}

func (qb *QueryBuilder[O]) addJoin(joinType JoinType, table Table, conds ...Condition) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if err := validateTableInput(table, "join table"); err != nil {
		qb.setBuildError(errors.Trace(err))
		return qb
	}

	on := make([]Condition, 0, len(conds))
	for _, cond := range conds {
		if _, _, _, err := validateConditionInput(cond); err != nil {
			qb.setBuildError(errors.Trace(err))
			return qb
		}

		on = append(on, cond)
	}

	qb.spec.Joins = append(qb.spec.Joins, join{
		joinType: joinType,
		table:    table,
		on:       on,
	})

	return qb
}

// CrossJoin adds a CROSS JOIN clause.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) CrossJoin(table Table) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if err := validateTableInput(table, "cross join table"); err != nil {
		qb.setBuildError(errors.Trace(err))
		return qb
	}

	qb.spec.Joins = append(qb.spec.Joins, join{
		joinType: CrossJoinType,
		table:    table,
	})

	return qb
}

// GroupBy adds GROUP BY clause with the specified columns.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) GroupBy(cols ...SQLColumn) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if qb.groupByCalled {
		qb.setBuildError(errors.New("GroupBy() can only be called once"))
		return qb
	}
	qb.groupByCalled = true

	for _, col := range cols {
		qb.appendColumn(&qb.spec.GroupBy, col)
	}

	return qb
}

// Having adds HAVING clause with the specified conditions.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) Having(conds ...Condition) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if qb.havingCalled {
		qb.setBuildError(errors.New("Having() can only be called once"))
		return qb
	}
	qb.havingCalled = true

	for _, cond := range conds {
		qb.appendCondition(&qb.spec.Having, cond)
	}

	return qb
}

// Where replaces any existing WHERE conditions for the query.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) Where(conds ...Condition) *QueryBuilder[O] {
	return qb.SetWhere(conds...)
}

// SetWhere replaces any existing WHERE conditions for the query.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) SetWhere(conds ...Condition) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if qb.whereCalled {
		qb.setBuildError(errors.New("Where() or SetWhere() can only be called once"))
		return qb
	}
	qb.whereCalled = true

	filters := make([]Condition, 0, len(conds))
	for _, cond := range conds {
		qb.appendCondition(&filters, cond)
	}

	qb.spec.Filters = filters

	return qb
}

// And adds additional conditions with AND logic.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) And(conds ...Condition) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	for _, cond := range conds {
		qb.appendCondition(&qb.spec.Filters, cond)
	}

	return qb
}

// AndIf conditionally adds conditions with AND logic.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) AndIf(ok bool, conds ...Condition) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if ok {
		return qb.And(conds...)
	}

	return qb
}

// KwSearch replaces any existing keyword-search columns.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) KwSearch(cols ...SearchColumn) *QueryBuilder[O] {
	return qb.SetKwSearch(cols...)
}

// SetKwSearch replaces any existing keyword-search columns.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) SetKwSearch(cols ...SearchColumn) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if qb.kwSearchCalled {
		qb.setBuildError(errors.New("KwSearch() or SetKwSearch() can only be called once"))
		return qb
	}
	qb.kwSearchCalled = true

	qb.spec.KeywordSearch = make([]SearchColumn, 0, len(cols))

	for _, col := range cols {
		qb.appendSearchColumn(&qb.spec.KeywordSearch, col)
	}

	return qb
}

// AppendKwSearch adds keyword-search columns without replacing existing ones.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder[O]) AppendKwSearch(cols ...SearchColumn) *QueryBuilder[O] {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	for _, col := range cols {
		qb.appendSearchColumn(&qb.spec.KeywordSearch, col)
	}

	return qb
}
