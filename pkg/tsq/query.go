package tsq

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/gorp.v2"
)

func (qb *QueryBuilder) MustBuild() *Query {
	q, err := qb.Build()
	if err != nil {
		logrus.WithField("module", "main").Fatalf(errors.ErrorStack(err))
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

		selectFields:         append([]IColumn{}, qb.selectFields...),
		selectTables:         make(map[string]Table),
		selectFieldFullNames: qb.selectFieldFullNames[:],
		kwFields:             qb.kwFields,
		kwTables:             qb.kwTables,
		kwFieldFullNames:     qb.kwFieldFullNames,
	}
	for tn, t := range qb.selectTables {
		n.selectTables[tn] = t
	}
	for tn, t := range qb.kwTables {
		n.kwTables[tn] = t
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

func (qb *Query) QueryInt(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int64, error) {
	logrus.Tracef("QueryInt:\n%s\n%v", qb.listQuery, args)

	i, err := tx.WithContext(ctx).SelectInt(qb.listQuery, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", qb.cntQuery, args)
	}

	return i, nil
}

func (qb *Query) QueryFloat(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (float64, error) {
	logrus.Tracef("QueryFloat:\n%s\n%v", qb.listQuery, args)

	i, err := tx.WithContext(ctx).SelectFloat(qb.listQuery, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", qb.cntQuery, args)
	}

	return i, nil
}

func (qb *Query) QueryStr(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (string, error) {
	logrus.Tracef("QueryStr:\n%s\n%v", qb.listQuery, args)

	s, err := tx.WithContext(ctx).SelectStr(qb.listQuery, args...)
	if err != nil {
		return "", errors.Annotatef(err, "\n%s\n%v", qb.cntQuery, args)
	}

	return s, nil
}

func (qb *Query) Count(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (int, error) {
	logrus.Tracef("Count:\n%s\n%v", qb.cntQuery, args)

	count, err := tx.WithContext(ctx).SelectInt(qb.cntQuery, args...)
	if err != nil {
		return 0, errors.Annotatef(err, "\n%s\n%v", qb.cntQuery, args)
	}

	return int(count), nil
}

func (qb *Query) Exists(
	ctx context.Context,
	tx gorp.SqlExecutor,
	args ...any,
) (bool, error) {
	logrus.Tracef("Exists:\n%s\n%v", qb.cntQuery, args)

	count, err := tx.WithContext(ctx).SelectInt(qb.cntQuery, args...)
	if err != nil {
		return false, errors.Annotatef(err, "\n%s\n%v", qb.cntQuery, args)
	}

	return count > 0, nil
}

func Page[T Table](
	ctx context.Context,
	tx gorp.SqlExecutor,
	page *PageReq,
	qb *Query,
	args ...any,
) (*PageResp[T], error) {
	countQuery, dataQuery, err := qb.pageQueryStr(page)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(qb.kwFields) > 0 && len(page.Keyword) > 0 {
		like := "%" + page.Keyword + "%"
		for i := 0; i < len(qb.kwFields); i++ {
			args = append(args, like)
		}
	}

	logrus.Tracef("Count:\n%s\n%v", countQuery, args)
	count, err := tx.WithContext(ctx).SelectInt(countQuery, args...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", countQuery, args)
	}

	logrus.Tracef("List:\n%s\n%v", dataQuery, args)
	rows, err := tx.WithContext(ctx).Query(dataQuery, args...)
	if err != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", dataQuery, args)
	}
	if rows.Err() != nil {
		return nil, errors.Annotatef(err, "\n%s\n%v", dataQuery, args)
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
		// find sort full field name
		var sf string
		for _, f := range qb.selectFields {
			// TODO
			if strings.HasSuffix(f.FullName(), "."+page.OrderBy) {
				sf = f.FullName()
			}
		}
		if len(sf) == 0 {
			return "", "", errors.Errorf("unknown sort field: %v", page.OrderBy)
		}

		listQuery += "\nORDER BY " + sf
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
