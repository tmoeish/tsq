package tsq

import (
	"context"
	"database/sql"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/gorp.v2"
)

func (qb *QueryBuilder) MustBuild() *Query {
	q, err := qb.Build()
	if err != nil {
		logrus.WithField("module", "main").Fatal(errors.ErrorStack(err))
	}
	return q
}

func (qb *QueryBuilder) Build() (*Query, error) {
	if len(qb.selectTables) == 0 {
		return nil, errors.Errorf("empty select fields: %+v", qb)
	}

	if len(qb.conditionTables) > 0 {
		for _, f := range qb.selectFields {
			if _, ok := qb.conditionTables[f.Table().Table()]; !ok {
				// TODO alias?
				return nil, errors.Errorf(
					"can not select fields: %s", f.FullName(),
				)
			}
		}
	}

	cntQuery := qb.buildCntQuery()
	listQuery := qb.buildListQuery()
	kwCntQuery := qb.buildKwCntQuery()
	kwListQuery := qb.buildKwListQuery()

	n := &Query{
		cntQuery:    cntQuery,
		listQuery:   listQuery,
		kwCntQuery:  kwCntQuery,
		kwListQuery: kwListQuery,

		selectFields:         slices.Clone(qb.selectFields),
		selectTables:         maps.Clone(qb.selectTables),
		selectFieldFullNames: slices.Clone(qb.selectFieldFullNames),
		kwFields:             slices.Clone(qb.kwFields),
		kwTables:             maps.Clone(qb.kwTables),
		kwFieldFullNames:     slices.Clone(qb.kwFieldFullNames),
	}

	return n, nil
}

type Query struct {
	cntQuery    string
	listQuery   string
	kwCntQuery  string
	kwListQuery string

	selectFields         []IColumn
	selectTables         map[string]Table
	selectFieldFullNames []string

	kwFields         []IColumn
	kwTables         map[string]Table
	kwFieldFullNames []string
}

func (q *Query) QueryInt(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int64, error) {
	logrus.Tracef("QueryInt:\n%s\n%v", q.listQuery, args)

	i, err := tx.WithContext(ctx).SelectInt(q.listQuery, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", q.cntQuery, args)
	}

	return i, nil
}

func (q *Query) QueryFloat(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (float64, error) {
	logrus.Tracef("QueryFloat:\n%s\n%v", q.listQuery, args)

	i, err := tx.WithContext(ctx).SelectFloat(q.listQuery, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", q.cntQuery, args)
	}

	return i, nil
}

func (q *Query) QueryStr(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (string, error) {
	logrus.Tracef("QueryStr:\n%s\n%v", q.listQuery, args)

	s, err := tx.WithContext(ctx).SelectStr(q.listQuery, args...)
	if err != nil {
		return "", errors.Annotatef(err, "\n%s\n%v", q.cntQuery, args)
	}

	return s, nil
}

func (q *Query) Count(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int, error) {
	logrus.Tracef("Count:\n%s\n%v", q.cntQuery, args)

	count, err := tx.WithContext(ctx).SelectInt(q.cntQuery, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", q.cntQuery, args)
	}

	return int(count), nil
}

func (q *Query) Exists(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (bool, error) {
	logrus.Tracef("Exists:\n%s\n%v", q.cntQuery, args)

	count, err := tx.WithContext(ctx).SelectInt(q.cntQuery, args...)
	if err != nil {
		return false, errors.Annotatef(err, "\n%s\n%v", q.cntQuery, args)
	}

	return count > 0, nil
}

func Page[T Table](
	ctx context.Context,
	tx gorp.SqlExecutor,
	page *PageReq,
	q *Query,
	args ...any,
) (*PageResp[T], error) {
	cntQuery, listQuery, err := q.pageQueryStr(page)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(q.kwFields) > 0 && len(page.Keyword) > 0 {
		like := "%" + page.Keyword + "%"
		for range len(q.kwFields) {
			args = append(args, like)
		}
	}

	logrus.Tracef("Count:\n%s\n%v", cntQuery, args)
	count, err := tx.WithContext(ctx).SelectInt(cntQuery, args...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", cntQuery, args)
	}

	logrus.Tracef("List:\n%s\n%v", listQuery, args)
	rows, err := tx.WithContext(ctx).Query(listQuery, args...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", listQuery, args)
	}
	if rows.Err() != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", listQuery, args)
	}
	defer func() {
		_ = rows.Close()
	}()

	var list []*T
	for rows.Next() {
		r := new(T)
		dest := make([]any, len(q.selectFields))
		for i, f := range q.selectFields {
			dest[i] = f.Ptr()(r)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Annotate(err, "rows.Scan")
		}
		list = append(list, r)
	}

	return NewResponse(page, count, list), nil
}

func (qb *Query) pageQueryStr(page *PageReq) (string, string, error) {
	var cntQuery, listQuery string
	if len(qb.kwFields) > 0 && len(page.Keyword) > 0 {
		cntQuery = qb.kwCntQuery
		listQuery = qb.kwListQuery
	} else {
		cntQuery = qb.cntQuery
		listQuery = qb.listQuery
	}

	// sort
	if len(page.OrderBy) != 0 {
		orderbys := strings.Split(page.OrderBy, ",")
		var fullNames []string
		for _, ob := range orderbys {
			// find sort full field name
			var sf string
			for _, f := range qb.selectFields {
				if f.Name() == ob {
					sf = f.FullName()
					break
				}
			}
			if len(sf) == 0 {
				return "", "", errors.Errorf("unknown sort field: %v", ob)
			}
			fullNames = append(fullNames, sf)
		}

		listQuery += "\nORDER BY " + strings.Join(fullNames, ", ")
		if len(page.Order) != 0 {
			listQuery += " " + page.Order
		}
	}

	// limit
	listQuery += "\nLIMIT " + strconv.Itoa(page.Offset()) + ", " + strconv.Itoa(page.Size)

	return cntQuery, listQuery, nil
}

func List[T Table](
	ctx context.Context,
	tx gorp.SqlExecutor,
	qb *Query,
	args ...any,
) ([]*T, error) {
	logrus.Tracef("List:\n%s\n%v", qb.listQuery, args)

	rows, err := tx.WithContext(ctx).Query(qb.listQuery, args...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", qb.listQuery, args)
	}
	if rows.Err() != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", qb.listQuery, args)
	}
	defer func() {
		_ = rows.Close()
	}()

	var list []*T
	for rows.Next() {
		r := new(T)
		dest := make([]any, len(qb.selectFields))
		for i, f := range qb.selectFields {
			dest[i] = f.Ptr()(r)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Annotate(err, "rows.Scan")
		}
		list = append(list, r)
	}

	return list, nil
}

func GetOrErr[T Table](
	ctx context.Context,
	tx gorp.SqlExecutor,
	qb *Query,
	args ...any,
) (*T, error) {
	logrus.Tracef("GetOrErr:\n%s\n%v", qb.listQuery, args)

	row := tx.WithContext(ctx).QueryRow(qb.listQuery, args...)
	if err := row.Err(); err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return nil, sql.ErrNoRows
		}
		return nil, errors.Annotatef(err, "\n%s\n%v", qb.listQuery, args)
	}

	r := new(T)
	dest := make([]any, len(qb.selectFields))
	for i, f := range qb.selectFields {
		dest[i] = f.Ptr()(r)
	}
	if err := row.Scan(dest...); err != nil {
		return nil, errors.Annotate(err, "row.Scan")
	}

	return r, nil
}

func (qb *Query) Load(
	ctx context.Context,
	tx gorp.SqlExecutor,
	holder any,
	args ...any,
) error {
	logrus.Tracef("Load:\n%s\n%v", qb.listQuery, args)

	row := tx.WithContext(ctx).QueryRow(qb.listQuery, args...)
	if err := row.Err(); err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return sql.ErrNoRows
		}
		return errors.Annotatef(err, "\n%s\n%v", qb.listQuery, args)
	}

	dest := make([]any, len(qb.selectFields))
	for i, f := range qb.selectFields {
		dest[i] = f.Ptr()(holder)
	}
	if err := row.Scan(dest...); err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return sql.ErrNoRows
		}
		return errors.Annotate(err, "row.Scan")
	}

	return nil
}
