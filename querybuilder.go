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

// QueryBuilder builds a structured query specification.
type QueryBuilder struct {
	spec                  QuerySpec
	buildErr              error
	allowCartesianProduct bool
}

// join represents any type of JOIN operation.
type join struct {
	joinType JoinType
	left     Column
	right    Column
	table    Table
}

func newQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		spec: QuerySpec{
			Selects: make([]Column, 0),
			Joins:   make([]join, 0),
			GroupBy: make([]Column, 0),
			Having:  make([]Condition, 0),
		},
	}
}

func (qb *QueryBuilder) ensureInitialized() *QueryBuilder {
	if qb == nil {
		qb = newQueryBuilder()
		qb.buildErr = errors.New("query builder cannot be nil")

		return qb
	}

	if qb.spec.Selects == nil {
		qb.spec.Selects = make([]Column, 0)
	}

	if qb.spec.Joins == nil {
		qb.spec.Joins = make([]join, 0)
	}

	if qb.spec.GroupBy == nil {
		qb.spec.GroupBy = make([]Column, 0)
	}

	if qb.spec.Having == nil {
		qb.spec.Having = make([]Condition, 0)
	}

	return qb
}

// AllowCartesianProduct keeps the legacy comma-separated FROM behavior.
func (qb *QueryBuilder) AllowCartesianProduct() *QueryBuilder {
	qb = qb.ensureInitialized()
	qb.allowCartesianProduct = true

	return qb
}

// Select creates a new QueryBuilder with the specified columns.
func Select(cols ...Column) *QueryBuilder {
	qb := newQueryBuilder()
	qb.spec.Selects = make([]Column, 0, len(cols))
	qb.addSelectColumns(cols...)

	return qb
}

func (qb *QueryBuilder) setBuildError(err error) {
	if qb == nil || err == nil || qb.buildErr != nil {
		return
	}

	qb.buildErr = err
}

func (qb *QueryBuilder) addSelectColumns(cols ...Column) {
	for _, col := range cols {
		if _, err := validateColumnInput(col); err != nil {
			qb.setBuildError(err)
			continue
		}

		qb.spec.Selects = append(qb.spec.Selects, col)
	}
}

func (qb *QueryBuilder) appendColumn(target *[]Column, col Column) {
	if _, err := validateColumnInput(col); err != nil {
		qb.setBuildError(err)
		return
	}

	*target = append(*target, col)
}

func (qb *QueryBuilder) appendCondition(target *[]Condition, cond Condition) {
	if _, _, _, err := validateConditionInput(cond); err != nil {
		qb.setBuildError(err)
		return
	}

	*target = append(*target, cond)
}

// LeftJoin adds a LEFT JOIN clause. Equivalent to `FROM left.Table LEFT JOIN right.Table ON left=right`.
// The columns must belong to different tables; to join a table to itself, use aliases.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder) LeftJoin(left Column, right Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if err := validateJoinColumns(left, right); err != nil {
		qb.setBuildError(err)
		return qb
	}

	qb.spec.Joins = append(qb.spec.Joins, join{
		joinType: LeftJoinType,
		left:     left,
		right:    right,
	})

	return qb
}

// InnerJoin adds an INNER JOIN clause.
// The columns must belong to different tables; to join a table to itself, use aliases.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder) InnerJoin(left Column, right Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if err := validateJoinColumns(left, right); err != nil {
		qb.setBuildError(err)
		return qb
	}

	qb.spec.Joins = append(qb.spec.Joins, join{
		joinType: InnerJoinType,
		left:     left,
		right:    right,
	})

	return qb
}

// RightJoin adds a RIGHT JOIN clause.
// The columns must belong to different tables; to join a table to itself, use aliases.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder) RightJoin(left Column, right Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if err := validateJoinColumns(left, right); err != nil {
		qb.setBuildError(err)
		return qb
	}

	qb.spec.Joins = append(qb.spec.Joins, join{
		joinType: RightJoinType,
		left:     left,
		right:    right,
	})

	return qb
}

// FullJoin adds a FULL JOIN clause. SQL generation is supported, but execution
// still depends on the target dialect supporting FULL JOIN.
// The columns must belong to different tables; to join a table to itself, use aliases.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder) FullJoin(left Column, right Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if err := validateJoinColumns(left, right); err != nil {
		qb.setBuildError(err)
		return qb
	}

	qb.spec.Joins = append(qb.spec.Joins, join{
		joinType: FullJoinType,
		left:     left,
		right:    right,
	})

	return qb
}

// CrossJoin adds a CROSS JOIN clause.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder) CrossJoin(table Table) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	if isNilValue(table) {
		qb.setBuildError(errors.New("cross join table cannot be nil"))
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
func (qb *QueryBuilder) GroupBy(cols ...Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	for _, col := range cols {
		qb.appendColumn(&qb.spec.GroupBy, col)
	}

	return qb
}

// Having adds HAVING clause with the specified conditions.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder) Having(conds ...Condition) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	for _, cond := range conds {
		qb.appendCondition(&qb.spec.Having, cond)
	}

	return qb
}

// Where replaces any existing WHERE conditions for the query.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder) Where(conds ...Condition) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	filters := make([]Condition, 0, len(conds))
	for _, cond := range conds {
		qb.appendCondition(&filters, cond)
	}

	qb.spec.Filters = filters

	return qb
}

// And adds additional conditions with AND logic.
// If the builder is in an error state, this method returns immediately without modifying the query.
func (qb *QueryBuilder) And(conds ...Condition) *QueryBuilder {
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
func (qb *QueryBuilder) AndIf(ok bool, conds ...Condition) *QueryBuilder {
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
func (qb *QueryBuilder) KwSearch(cols ...Column) *QueryBuilder {
	qb = qb.ensureInitialized()

	if qb.buildErr != nil {
		return qb
	}

	qb.spec.KeywordSearch = make([]Column, 0, len(cols))

	for _, col := range cols {
		qb.appendColumn(&qb.spec.KeywordSearch, col)
	}

	return qb
}
