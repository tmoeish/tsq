package tsq

import (
	"context"
	"errors"
	"fmt"
)

// AttachMany reads the children of parents with one extra query and hands each
// parent its own, which is how a list of parents avoids one query per row.
//
// children is a query whose only list parameter is childKey, as
// Where(childKey.In(childKey.ListParam())) makes it; it decides which children
// count, and args bind its other parameters. The keys of parents are collected from
// parentKey, deduplicated and read through Query.ListIn, so any number of parents
// works.
//
//	err := tsq.AttachMany(ctx, db, courses, TableCourse.ID, enrollmentsByCourse, TableEnrollment.CourseID,
//		func(c *Course, es []*Enrollment) { c.Enrollments = es })
//
// Within one statement the children keep the order of the child query; a key list
// large enough to be split over several statements has no overall order, so order
// the children per parent afterwards when it matters.
func AttachMany[P, C any, K comparable](
	ctx context.Context,
	db Executor,
	parents []*P,
	parentKey TypedColumn[P, K],
	children *Query[C],
	childKey Column[C, K],
	assign func(parent *P, children []*C),
	args ...Arg,
) error {
	byKey, err := loadChildren(ctx, db, parents, parentKey, children, childKey, args)
	if err != nil {
		return err
	}

	if assign == nil {
		return errors.New("attach: assign cannot be nil")
	}

	for _, parent := range parents {
		key, err := columnValue[P, K](parentKey, parent)
		if err != nil {
			return err
		}

		assign(parent, byKey[key])
	}

	return nil
}

// AttachOne is AttachMany for a single child per parent, such as the row a foreign
// key points at. Where a key matches several children, the first in the child
// query's order wins; a parent with none is left alone.
func AttachOne[P, C any, K comparable](
	ctx context.Context,
	db Executor,
	parents []*P,
	parentKey TypedColumn[P, K],
	children *Query[C],
	childKey Column[C, K],
	assign func(parent *P, child *C),
	args ...Arg,
) error {
	byKey, err := loadChildren(ctx, db, parents, parentKey, children, childKey, args)
	if err != nil {
		return err
	}

	if assign == nil {
		return errors.New("attach: assign cannot be nil")
	}

	for _, parent := range parents {
		key, err := columnValue[P, K](parentKey, parent)
		if err != nil {
			return err
		}

		if group := byKey[key]; len(group) > 0 {
			assign(parent, group[0])
		}
	}

	return nil
}

// loadChildren reads the children of every parent key and groups them by it.
func loadChildren[P, C any, K comparable](
	ctx context.Context,
	db Executor,
	parents []*P,
	parentKey TypedColumn[P, K],
	children *Query[C],
	childKey Column[C, K],
	args []Arg,
) (map[K][]*C, error) {
	switch {
	case isNilValue(parentKey) || isNilValue(childKey):
		return nil, errors.New("attach: the keys cannot be nil")
	case children == nil:
		return nil, errors.New("attach: the child query cannot be nil")
	}

	keys := make([]K, 0, len(parents))

	for _, parent := range parents {
		key, err := columnValue[P, K](parentKey, parent)
		if err != nil {
			return nil, err
		}

		keys = append(keys, key)
	}

	byKey := map[K][]*C{}

	if len(keys) == 0 {
		return byKey, nil
	}

	rows, err := children.ListIn(ctx, db, childKey.ListParam(), keys, args...)
	if err != nil {
		return nil, fmt.Errorf("attach: %w", err)
	}

	for _, row := range rows {
		key, err := columnValue[C, K](childKey, row)
		if err != nil {
			return nil, err
		}

		byKey[key] = append(byKey[key], row)
	}

	return byKey, nil
}

// columnValue reads the value col scans into, from the row it belongs to.
func columnValue[O, T any](col TypedColumn[O, T], row *O) (T, error) {
	var zero T

	if row == nil {
		return zero, errors.New("attach: a row is nil")
	}

	core := col.core()
	if err := core.err(); err != nil {
		return zero, err
	}

	if core.scan == nil {
		return zero, fmt.Errorf("attach: %s is an expression, not a column of the row", core.name)
	}

	pointer, ok := core.scan(row).(*T)
	if !ok {
		return zero, fmt.Errorf("attach: %s does not read into the row", core.name)
	}

	return *pointer, nil
}
