package tsq

import (
	"fmt"
	"slices"
)

// QuerySpec is the single source of truth for a query definition before planning.
type QuerySpec[O Owner] struct {
	From          Table             // From stores the base table used by the query.
	Selects       []BoundColumn[O]  // Selects stores the projected columns in output order.
	Filters       []Condition       // Filters stores WHERE predicates.
	KeywordSearch []SearchColumn    // KeywordSearch stores columns used by keyword-search helpers.
	Joins         []join            // Joins stores JOIN clauses in declaration order.
	GroupBy       []SQLColumn       // GroupBy stores GROUP BY expressions.
	Having        []Condition       // Having stores HAVING predicates.
	Lock          queryLock         // Lock stores the optional row-lock clause.
	SetOps        []setOperation[O] // SetOps stores UNION/INTERSECT/EXCEPT operations appended to the query.
}

func (spec QuerySpec[O]) selectCount() int        { return len(spec.Selects) }
func (spec QuerySpec[O]) filterCount() int        { return len(spec.Filters) }
func (spec QuerySpec[O]) joinCount() int          { return len(spec.Joins) }
func (spec QuerySpec[O]) groupCount() int         { return len(spec.GroupBy) }
func (spec QuerySpec[O]) havingCount() int        { return len(spec.Having) }
func (spec QuerySpec[O]) keywordSearchCount() int { return len(spec.KeywordSearch) }

type queryPlan struct {
	cntSQL     string
	listSQL    string
	kwCntSQL   string
	kwListSQL  string
	cntArgs    []any
	listArgs   []any
	kwCntArgs  []any
	kwListArgs []any
}

func buildQueryPlan[O Owner](spec QuerySpec[O]) (*queryPlan, error) {
	if len(spec.Selects) == 0 {
		return nil, fmt.Errorf("empty select fields: %+v", spec)
	}

	if err := spec.validateJoinGraph(); err != nil {
		return nil, err
	}

	if err := spec.validateSetOperations(); err != nil {
		return nil, err
	}

	cntSQL, cntArgs, err := spec.buildCntSQL()
	if err != nil {
		return nil, err
	}

	listSQL, listArgs, err := spec.buildListSQL()
	if err != nil {
		return nil, err
	}

	kwCntSQL, kwCntArgs, err := spec.buildKwCntSQL()
	if err != nil {
		return nil, err
	}

	kwListSQL, kwListArgs, err := spec.buildKwListSQL()
	if err != nil {
		return nil, err
	}

	return &queryPlan{
		cntSQL:     cntSQL,
		listSQL:    listSQL,
		kwCntSQL:   kwCntSQL,
		kwListSQL:  kwListSQL,
		cntArgs:    slices.Clone(cntArgs),
		listArgs:   slices.Clone(listArgs),
		kwCntArgs:  slices.Clone(kwCntArgs),
		kwListArgs: slices.Clone(kwListArgs),
	}, nil
}
