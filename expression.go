package tsq

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type queryArgMarker string

const (
	externalArgMarker queryArgMarker = "external"
	keywordArgMarker  queryArgMarker = "keyword"
)

// Expression represents a SQL fragment plus the args needed to render it safely.
type Expression interface {
	Expr() string // Expr returns the SQL fragment text.
	Args() []any  // Args returns the bind arguments referenced by Expr.
}

type expressionError struct {
	err error
}

// Expr returns an empty fragment so expressionError can flow through builders until build time.
func (e expressionError) Expr() string { return "" }

// Args returns nil because expressionError carries only a deferred build error.
func (e expressionError) Args() []any { return nil }

func (e expressionError) buildError() error {
	return e.err
}

// variableExpression marks a single runtime-supplied bind placeholder.
type variableExpression struct{}

// Expr returns the placeholder emitted into the SQL fragment.
func (v variableExpression) Expr() string { return "?" }

// Args returns the marker consumed later by runtime argument resolution.
func (v variableExpression) Args() []any { return []any{externalArgMarker} }

var varMarker variableExpression

// variableSliceExpression marks a runtime-supplied slice placeholder used by IN predicates.
type variableSliceExpression struct{}

// Expr returns the placeholder emitted into the SQL fragment before slice expansion.
func (v variableSliceExpression) Expr() string { return "?" }

// Args returns the marker consumed later by runtime slice expansion.
func (v variableSliceExpression) Args() []any { return []any{externalSliceArgMarker{}} }

var varSliceMarker variableSliceExpression

// valuesExpression stores a fully expanded placeholder list such as "?, ?, ?".
type valuesExpression struct {
	expr string
	args []any
}

// Expr returns the expanded placeholder list.
func (a valuesExpression) Expr() string {
	return a.expr
}

// Args returns the bind arguments that correspond to the placeholder list.
func (a valuesExpression) Args() []any {
	return append([]any(nil), a.args...)
}

func newValuesExpression(args any) Expression {
	v := reflect.ValueOf(args)
	if v.Kind() != reflect.Slice {
		return expressionError{err: fmt.Errorf("expected slice, got %T", args)}
	}

	values := make([]string, v.Len())
	bindArgs := make([]any, v.Len())

	for i := range v.Len() {
		value := v.Index(i).Interface()
		if err := validatePredicateValue(value); err != nil {
			return expressionError{err: err}
		}

		values[i] = "?"
		bindArgs[i] = value
	}

	return valuesExpression{
		expr: strings.Join(values, ", "),
		args: bindArgs,
	}
}

type rawExpression struct {
	expr string
	args []any
}

// Expr returns the raw SQL fragment.
func (r rawExpression) Expr() string { return r.expr }

// Args returns the bind arguments referenced by the raw fragment.
func (r rawExpression) Args() []any { return append([]any(nil), r.args...) }

// Bind returns a parameterized expression for value.
func Bind(value any) Expression {
	if err := validatePredicateValue(value); err != nil {
		return expressionError{err: err}
	}

	return rawExpression{expr: "?", args: []any{value}}
}

// BindSlice returns a comma-separated placeholder list for a slice value.
func BindSlice(values any) Expression {
	return newValuesExpression(values)
}

// argumentToExpression converts various argument types to their SQL expression representation.
func argumentToExpression(arg any) Expression {
	switch v := arg.(type) {
	case Expression:
		return v
	case SQLColumn:
		return rawExpression{expr: rawColumnQualifiedName(v), args: expressionArgs(v)}
	case validatedSubquery:
		expr, args, err := buildSubqueryExpression(v.query, v.usage)
		if err != nil {
			return expressionError{err: err}
		}

		return rawExpression{expr: expr, args: args}
	case subquery:
		return expressionError{err: errors.New(
			"raw subqueries are not allowed in Pred; use EQSub/NESub/GTSub/InSub/ExistsSub helpers",
		)}
	default:
		return Bind(v)
	}
}

func collectExpressionArgs(args ...any) []any {
	var result []any

	for _, arg := range args {
		result = append(result, argumentToExpression(arg).Args()...)
	}

	return result
}

func expressionArgs(col SQLColumn) []any {
	type expressionArgser interface {
		expressionArgs() []any
	}

	if withArgs, ok := col.(expressionArgser); ok {
		return withArgs.expressionArgs()
	}

	return nil
}

func expressionBuildError(expr Expression) error {
	if expr == nil {
		return errors.New("expression cannot be nil")
	}

	if carrier, ok := expr.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return carrier.buildError()
	}

	return nil
}

func validatePredicateValue(arg any) error {
	if isNilValue(arg) {
		return errors.New("null predicate values are not supported; use IsNull/IsNotNull explicitly")
	}

	if valuer, ok := arg.(driver.Valuer); ok {
		value, err := valuer.Value()
		if err != nil {
			return fmt.Errorf("failed to evaluate predicate value %T: %w", arg, err)
		}

		if value == nil {
			return errors.New("null predicate values are not supported; use IsNull/IsNotNull explicitly")
		}

		return validatePredicateScalar(value)
	}

	return validatePredicateScalar(arg)
}

func validatePredicateScalar(arg any) error {
	v := reflect.ValueOf(arg)
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return errors.New("null predicate values are not supported; use IsNull/IsNotNull explicitly")
		}

		v = v.Elem()
		arg = v.Interface()
	}

	if !v.IsValid() {
		return errors.New("null predicate values are not supported; use IsNull/IsNotNull explicitly")
	}

	if _, ok := arg.(time.Time); ok {
		return nil
	}

	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}

		return fmt.Errorf("unsupported predicate slice type %v; use In/InVar for collections", v.Type())
	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}

		return fmt.Errorf("unsupported predicate array type %v", v.Type())
	case reflect.Map, reflect.Struct:
		return fmt.Errorf("unsupported predicate value type %v", v.Type())
	default:
		return nil
	}
}
