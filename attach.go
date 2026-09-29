package tsq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
//
// childKey must be selected by the child query. A nullable key (a NullColumn) is
// unwrapped, and NULL matches nothing. Keys are matched in Go, exactly: under a
// case-insensitive collation (MySQL's default) the child query finds "ABC" for
// the key "abc", and that child is then attached to no parent, so give string keys
// a binary collation.
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
	// Checked before the query runs: a nil assign would throw the rows away.
	if assign == nil {
		return errors.New("attach: assign cannot be nil")
	}

	byKey, err := loadChildren(ctx, db, parents, parentKey, children, childKey, args)
	if err != nil {
		return err
	}

	for _, parent := range parents {
		key, ok, err := columnValue[P, K](parentKey, parent)
		if err != nil {
			return err
		}

		if !ok {
			assign(parent, nil)
			continue
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
	// Checked before the query runs: a nil assign would throw the rows away.
	if assign == nil {
		return errors.New("attach: assign cannot be nil")
	}

	byKey, err := loadChildren(ctx, db, parents, parentKey, children, childKey, args)
	if err != nil {
		return err
	}

	for _, parent := range parents {
		key, ok, err := columnValue[P, K](parentKey, parent)
		if err != nil {
			return err
		}

		if group := byKey[key]; ok && len(group) > 0 {
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

	// Every child is grouped by the value its childKey field holds; a child query
	// that does not select childKey left it zero, and no parent got any child.
	if !selectsField(children.spec.Selects, childKey.core()) {
		return nil, fmt.Errorf("attach: the child query does not select %s, the key children are grouped by", childKey.Name())
	}

	keys := make([]K, 0, len(parents))

	for _, parent := range parents {
		key, ok, err := columnValue[P, K](parentKey, parent)
		if err != nil {
			return nil, err
		}

		if ok {
			keys = append(keys, key)
		}
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
		key, ok, err := columnValue[C, K](childKey, row)
		if err != nil {
			return nil, err
		}

		if ok {
			byKey[key] = append(byKey[key], row)
		}
	}

	return byKey, nil
}

// columnValue reads the value col scans into, from the row it belongs to. A
// nullable column's value is unwrapped, and a NULL reports false: a NULL key
// matches nothing.
func columnValue[O, T any](col TypedColumn[O, T], row *O) (T, bool, error) {
	var zero T

	if row == nil {
		return zero, false, errors.New("attach: a row is nil")
	}

	core := col.core()
	if err := core.err(); err != nil {
		return zero, false, err
	}

	if core.scan == nil {
		return zero, false, fmt.Errorf("attach: %s is an expression, not a column of the row", core.name)
	}

	field := reflect.ValueOf(core.scan(row)).Elem()

	if core.nullable {
		value, valid := nullableValue(field)
		if !valid {
			return zero, false, nil
		}

		field = value
	}

	key, ok := reflect.TypeAssert[T](field)
	if !ok {
		return zero, false, fmt.Errorf("attach: %s does not hold a %T", core.name, zero)
	}

	return key, true, nil
}

// nullableValue unwraps a nullable form: the element of a pointer, or the value
// of a struct with a Valid flag (sql.Null[T], sql.NullString, null.String), which
// is its first field.
func nullableValue(v reflect.Value) (reflect.Value, bool) {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Value{}, false
		}

		return v.Elem(), true
	case reflect.Struct:
		valid := v.FieldByName("Valid")
		if !valid.IsValid() || valid.Kind() != reflect.Bool || !valid.Bool() {
			return reflect.Value{}, false
		}

		// The value field is found the way nullableValueType finds it, promoted
		// fields included: a type embedding sql.NullString is nullable too.
		for _, f := range reflect.VisibleFields(v.Type()) {
			if !f.Anonymous && f.IsExported() && f.Name != "Valid" {
				return v.FieldByIndex(f.Index), true
			}
		}
	}

	return reflect.Value{}, false
}

// selectsField reports whether one of selects reads into the field key reads into.
func selectsField[O any](selects []BoundColumn[O], key *columnCore) bool {
	if key == nil || key.scan == nil {
		return false
	}

	holder := new(O)
	want := reflect.ValueOf(key.scan(holder)).Pointer()

	for _, col := range selects {
		if core := col.core(); core != nil && core.scan != nil && reflect.ValueOf(core.scan(holder)).Pointer() == want {
			return true
		}
	}

	return false
}
