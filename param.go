package tsq

import (
	"errors"
	"fmt"
	"maps"
	"strings"
)

// Param is a named, typed placeholder whose value is supplied when the query runs.
//
// A Param is an RHS, so it goes wherever a column or subquery could:
//
//	minPrice := tsq.NewParam[int64]("min_price")
//	q := tsq.Select(Course__Cols...).From(TableCourse).
//		Where(Course_Price.GTE(minPrice)).MustBuild()
//	courses, err := q.List(ctx, db, minPrice.Bind(1000))
//
// Values are matched to placeholders by parameter identity, not by position, and
// Bind only accepts a T, so a query cannot silently receive its arguments in the
// wrong order or of the wrong type. Every column also carries its own parameter; see
// Column.Param.
type Param[T any] struct {
	spec *paramSpec
}

// NewParam declares a parameter. The name appears in error messages; parameters are
// told apart by identity, so two parameters may share a name only if they never
// meet in one statement.
func NewParam[T any](name string) Param[T] {
	return Param[T]{spec: newParamSpec(name, paramScalar)}
}

// Bind supplies the parameter's value for one execution.
func (p Param[T]) Bind(value T) Arg {
	return newArg(p.spec, value)
}

// Name returns the parameter name.
func (p Param[T]) Name() string { return p.spec.name }

func (p Param[T]) rhsValue(T) {}

func (p Param[T]) operand() exprInfo {
	if p.spec == nil {
		return exprInfo{err: errors.New("parameter is not initialized; use tsq.NewParam")}
	}

	return exprInfo{sql: sqlParam(p.spec)}
}

// ListParam is a named, typed placeholder for the right-hand side of IN and NOT IN.
//
// An empty or nil list keeps the filter: IN matches nothing and NOT IN matches
// everything. Neither ever drops the predicate, because a query that silently loses
// its filter returns the whole table.
type ListParam[T any] struct {
	spec *paramSpec
}

// NewListParam declares a list parameter.
func NewListParam[T any](name string) ListParam[T] {
	return ListParam[T]{spec: newParamSpec(name, paramList)}
}

// Bind supplies the parameter's values for one execution.
func (p ListParam[T]) Bind(values ...T) Arg {
	boxed := make([]any, len(values))
	for i, v := range values {
		boxed[i] = v
	}

	return newArg(p.spec, boxed)
}

// Name returns the parameter name.
func (p ListParam[T]) Name() string { return p.spec.name }

func (p ListParam[T]) setValue(T) {}

func (p ListParam[T]) setOperand(negated bool) exprInfo {
	if p.spec == nil {
		return exprInfo{err: errors.New("list parameter is not initialized; use tsq.NewListParam")}
	}

	spec := p.spec
	if negated {
		spec = spec.derive(paramNotInList)
	}

	return exprInfo{sql: sqlJoin(sqlText("("), sqlParam(spec), sqlText(")"))}
}

// Arg is one parameter value for one execution, made by Param.Bind,
// ListParam.Bind, Column.Bind or Column.BindList.
type Arg struct {
	spec  *paramSpec
	value any
	err   error
}

func newArg(spec *paramSpec, value any) Arg {
	if spec == nil {
		return Arg{err: errors.New("cannot bind an uninitialized parameter")}
	}

	if spec.mode == paramList {
		for i, v := range value.([]any) {
			if err := validatePredicateValue(v); err != nil {
				return Arg{spec: spec, err: fmt.Errorf("parameter %s[%d]: %w", spec.name, i, err)}
			}
		}

		return Arg{spec: spec, value: value}
	}

	if err := validatePredicateValue(value); err != nil {
		return Arg{spec: spec, err: fmt.Errorf("parameter %s: %w", spec.name, err)}
	}

	return Arg{spec: spec, value: value}
}

type paramMode uint8

const (
	paramScalar paramMode = iota
	paramList
	paramNotInList
	paramPrefix
	paramSuffix
	paramContains
)

// paramSpec identifies a parameter. A derived spec (a LIKE pattern built from a
// string parameter, or the NOT IN form of a list) renders differently but takes
// its value from its base.
type paramSpec struct {
	name string
	mode paramMode
	base *paramSpec
}

func newParamSpec(name string, mode paramMode) *paramSpec {
	return &paramSpec{name: strings.TrimSpace(name), mode: mode}
}

func (p *paramSpec) root() *paramSpec {
	if p.base != nil {
		return p.base
	}

	return p
}

func (p *paramSpec) derive(mode paramMode) *paramSpec {
	return &paramSpec{name: p.name, mode: mode, base: p.root()}
}

func (p *paramSpec) label() string {
	if p.name == "" {
		return "(unnamed)"
	}

	return p.name
}

// write renders the placeholders for value.
func (p *paramSpec) write(value any, placeholder func(any), sql *strings.Builder) error {
	switch p.mode {
	case paramScalar:
		placeholder(value)
	case paramPrefix, paramSuffix, paramContains:
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("parameter %s: pattern value must be a string, got %T", p.label(), value)
		}

		s = escapeLikePattern(s)

		switch p.mode {
		case paramPrefix:
			s += "%"
		case paramSuffix:
			s = "%" + s
		default:
			s = "%" + s + "%"
		}

		placeholder(s)
	case paramList, paramNotInList:
		values, ok := value.([]any)
		if !ok {
			return fmt.Errorf("parameter %s: expected a list value, got %T", p.label(), value)
		}

		if len(values) == 0 {
			// An empty list must neither be a syntax error nor drop the filter:
			// IN (NULL) matches nothing, and NOT IN over an empty subquery matches
			// everything, on every supported dialect.
			if p.mode == paramList {
				sql.WriteString("NULL")
			} else {
				sql.WriteString("SELECT 1 WHERE 1 = 0")
			}

			return nil
		}

		for i, v := range values {
			if i > 0 {
				sql.WriteString(", ")
			}

			placeholder(v)
		}
	}

	return nil
}

// likeEscapeChar is the LIKE escape character TSQ declares on every pattern it
// builds. Backslash cannot serve: SQLite has no default escape character, and MySQL
// cannot spell ESCAPE '\' because a backslash escapes the closing quote. A tilde is
// inert inside string literals in all three dialects.
const (
	likeEscapeChar   = "~"
	likeEscapeClause = " ESCAPE '" + likeEscapeChar + "'"
)

// escapeLikePattern escapes LIKE wildcards so that s matches literally.
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, likeEscapeChar, likeEscapeChar+likeEscapeChar)
	s = strings.ReplaceAll(s, "%", likeEscapeChar+"%")

	return strings.ReplaceAll(s, "_", likeEscapeChar+"_")
}

// keywordParam is the search term of Page; it is bound from PageRequest.Keyword.
var keywordParam = newParamSpec("keyword", paramScalar)

// argSet resolves parameter values for one execution.
type argSet struct {
	values map[*paramSpec]any
}

// bindArgs matches args to the parameters a statement uses. A missing value, a
// value for a parameter the statement does not use, and two values for one
// parameter are all errors: each is a caller mistake that would otherwise run a
// different query than the one intended.
func bindArgs(used []*paramSpec, args []Arg, builtin map[*paramSpec]any) (argSet, error) {
	values := make(map[*paramSpec]any, len(args)+len(builtin))
	maps.Copy(values, builtin)

	for _, arg := range args {
		if arg.err != nil {
			return argSet{}, arg.err
		}

		if arg.spec == nil {
			return argSet{}, errors.New("argument is not bound to a parameter; use Param.Bind or Column.Bind")
		}

		if _, dup := values[arg.spec]; dup {
			return argSet{}, fmt.Errorf("parameter %s is bound more than once", arg.spec.label())
		}

		values[arg.spec] = arg.value
	}

	needed := make(map[*paramSpec]struct{}, len(used))
	for _, spec := range used {
		root := spec.root()
		needed[root] = struct{}{}

		if _, ok := values[root]; !ok {
			return argSet{}, fmt.Errorf("parameter %s has no value; pass it with Bind", root.label())
		}
	}

	for _, arg := range args {
		if _, ok := needed[arg.spec]; !ok {
			return argSet{}, fmt.Errorf("parameter %s is not used by this statement", arg.spec.label())
		}
	}

	return argSet{values: values}, nil
}

func (s argSet) value(spec *paramSpec) (any, error) {
	v, ok := s.values[spec.root()]
	if !ok {
		return nil, fmt.Errorf("parameter %s has no value; pass it with Bind", spec.label())
	}

	if spec.mode == paramNotInList || spec.mode == paramList {
		if _, isList := v.([]any); !isList {
			return nil, fmt.Errorf("parameter %s is a list parameter; bind it with BindList", spec.label())
		}
	}

	return v, nil
}
