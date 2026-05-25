package tsq

import "errors"

// projectedColumn reuses an existing expression while scanning into another owner.
type projectedColumn[O Owner, T any] struct {
	col columnImpl[O, T]
}

// MapInto creates a result-owned projection column from another typed expression.
func MapInto[Target Owner, T any](
	source ValueColumn[T],
	fieldPointer func(*Target) *T,
	jsonFieldName string,
) ResultColumn[Target, T] {
	pointer := toScanPointer(fieldPointer)
	if isNilValue(source) {
		return projectedColumn[Target, T]{col: columnImpl[Target, T]{
			fieldPointer:  pointer,
			jsonFieldName: jsonFieldName,
			buildErr:      errors.New("source column cannot be nil"),
		}}
	}

	tables := source.referencedTables()

	aggregate := false
	if agg, ok := source.(interface{ isAggregateExpression() bool }); ok {
		aggregate = agg.isAggregateExpression()
	}

	distinct := false
	if d, ok := source.(interface{ isDistinctExpression() bool }); ok {
		distinct = d.isDistinctExpression()
	}

	transformed := false
	if t, ok := source.(interface{ isTransformedExpression() bool }); ok {
		transformed = t.isTransformedExpression()
	}

	buildErr := error(nil)
	if carrier, ok := source.(buildErrorCarrier); ok {
		buildErr = carrier.buildError()
	}

	if pointer == nil {
		buildErr = errors.New("field pointer cannot be nil")
	}

	return projectedColumn[Target, T]{col: columnImpl[Target, T]{
		table:         columnPrimaryTable(source),
		name:          source.OutputName(),
		qualifiedName: rawColumnQualifiedName(source),
		fieldPointer:  pointer,
		jsonFieldName: jsonFieldName,
		args:          expressionArgs(source),
		tables:        tables,
		aggregate:     aggregate,
		distinct:      distinct,
		transformed:   transformed,
		buildErr:      buildErr,
	}}
}

// SQLExpr renders the projected expression in canonical SQL form.
func (c projectedColumn[O, T]) SQLExpr() string { return c.col.SQLExpr() }

// Table returns the primary table that owns the source expression.
func (c projectedColumn[O, T]) Table() Table { return c.col.Table() }

// Name returns the source expression's output name.
func (c projectedColumn[O, T]) Name() string { return c.col.Name() }

// QualifiedName returns the rendered SQL expression for the projection.
func (c projectedColumn[O, T]) QualifiedName() string { return c.col.QualifiedName() }

// FieldPointer returns the runtime scan adapter for the projected destination field.
func (c projectedColumn[O, T]) FieldPointer() scanPointer { return c.col.FieldPointer() }

// OutputName returns the default scan alias inherited from the source expression.
func (c projectedColumn[O, T]) OutputName() string { return c.col.OutputName() }

// JSONFieldName returns the JSON-facing field label used for the projection.
func (c projectedColumn[O, T]) JSONFieldName() string { return c.col.JSONFieldName() }

func (c projectedColumn[O, T]) scanPointer() scanPointer { return c.col.scanPointer() }

func (c projectedColumn[O, T]) columnValue(T) {}

func (c projectedColumn[O, T]) rhsValue(T) {}

func (c projectedColumn[O, T]) rhsPredicateArg() any { return c }

func (c projectedColumn[O, T]) selectOwner(O) {}

func (c projectedColumn[O, T]) rawQualifiedName() string { return c.col.rawQualifiedName() }

func (c projectedColumn[O, T]) expressionArgs() []any { return c.col.expressionArgs() }

func (c projectedColumn[O, T]) buildError() error { return c.col.buildError() }

func (c projectedColumn[O, T]) tableSource() Table { return c.col.tableSource() }

func (c projectedColumn[O, T]) referencedTables() map[string]Table {
	return c.col.referencedTables()
}

func (c projectedColumn[O, T]) isAggregateExpression() bool {
	return c.col.isAggregateExpression()
}

func (c projectedColumn[O, T]) isDistinctExpression() bool {
	return c.col.isDistinctExpression()
}

func (c projectedColumn[O, T]) isTransformedExpression() bool {
	return c.col.isTransformedExpression()
}
