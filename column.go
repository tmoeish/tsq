package tsq

import "github.com/juju/errors"

// FieldPointer returns a typed pointer into an owner value.
type FieldPointer[O Owner, T any] func(*O) *T

type scanPointer func(holder any) any

// AnyColumn is the erased runtime view of a SQL expression that can be rendered
// and, when selected, scanned into an owner field.
type AnyColumn interface {
	SQLExpr() string
	OutputName() string
	JSONFieldName() string
	Table() Table
	Name() string
	QualifiedName() string
	scanPointer() scanPointer
	referencedTables() map[string]Table
}

// SelectableColumn is a SQL expression selectable into an owner.
type SelectableColumn[O Owner] interface {
	AnyColumn
	selectOwner(O)
}

// Column is a value-typed selectable column.
type Column[O Owner, T any] interface {
	SelectableColumn[O]
	columnValue(T)
}

// TableColumn is an owner-typed physical/source column.
type TableColumn[O Table] interface {
	SelectableColumn[O]
	SearchColumn
	tableColumnOwner(O)
	tableSource() Table
	columnName() string
}

// SearchColumn is a SQL expression allowed in keyword search.
type SearchColumn interface {
	AnyColumn
	searchColumn()
}

type typedColumn[T any] interface {
	AnyColumn
	columnValue(T)
}

// Col represents a typed source expression.
type Col[O Owner, T any] struct {
	table         Table
	name          string
	qualifiedName string
	jsonFieldName string
	fieldPointer  scanPointer
	args          []any
	tables        map[string]Table
	aggregate     bool
	distinct      bool
	transformed   bool
	buildErr      error
}

// ResultCol is a projection-only column selected into a result owner.
type ResultCol[O Owner, T any] struct {
	col Col[O, T]
}

// NewCol creates a new typed column for the table represented by O.
func NewCol[O Table, T any](baseName, jsonFieldName string, fieldPointer FieldPointer[O, T]) Col[O, T] {
	var table O

	return newColForTable[O, T](table, baseName, jsonFieldName, toScanPointer(fieldPointer))
}

func toScanPointer[O Owner, T any](fieldPointer FieldPointer[O, T]) scanPointer {
	if fieldPointer == nil {
		return nil
	}

	return func(holder any) any {
		return fieldPointer(holder.(*O))
	}
}

func newColForTable[O Owner, T any](table Table, baseName, jsonFieldName string, fieldPointer scanPointer) Col[O, T] {
	if isNilValue(table) {
		return Col[O, T]{
			name:          baseName,
			jsonFieldName: jsonFieldName,
			fieldPointer:  fieldPointer,
			buildErr:      errors.New("column table cannot be nil"),
		}
	}

	return Col[O, T]{
		table:         table,
		name:          baseName,
		qualifiedName: rawQualifiedIdentifierForTable(table, baseName),
		jsonFieldName: jsonFieldName,
		fieldPointer:  fieldPointer,
		tables:        map[string]Table{table.Table(): table},
	}
}

func (c Col[O, T]) SQLExpr() string {
	return renderCanonicalSQL(c.qualifiedName)
}

func (c Col[O, T]) Table() Table {
	return c.table
}

func (c Col[O, T]) Name() string {
	return c.name
}

func (c Col[O, T]) QualifiedName() string {
	return c.SQLExpr()
}

func (c Col[O, T]) FieldPointer() scanPointer {
	return c.scanPointer()
}

func (c Col[O, T]) OutputName() string {
	return c.name
}

func (c Col[O, T]) JSONFieldName() string {
	return c.jsonFieldName
}

func (c Col[O, T]) scanPointer() scanPointer {
	return c.fieldPointer
}

func (c Col[O, T]) rawQualifiedName() string {
	return c.qualifiedName
}

func (c Col[O, T]) columnValue(T) {}

func (c Col[O, T]) selectOwner(O) {}

func (c Col[O, T]) tableColumnOwner(O) {}

func (c Col[O, T]) searchColumn() {}

func (c Col[O, T]) tableSource() Table {
	return c.table
}

func (c Col[O, T]) columnName() string {
	return c.name
}

// WithTable returns a copy of the column rebound to a different table source.
func (c Col[O, T]) WithTable(table Table) Col[O, T] {
	if isNilValue(table) {
		c.buildErr = errors.New("column table cannot be nil")
		return c
	}

	if c.transformed {
		c.buildErr = errors.New("cannot rebind transformed column; alias the base column before applying expressions")
		return c
	}

	tables := cloneTableMap(c.tables)
	if len(tables) == 0 {
		tables = make(map[string]Table, 1)
	}

	if !isNilValue(c.table) {
		delete(tables, c.table.Table())
	}
	tables[table.Table()] = table

	return Col[O, T]{
		table:         table,
		name:          c.name,
		qualifiedName: rawQualifiedIdentifierForTable(table, c.name),
		jsonFieldName: c.jsonFieldName,
		fieldPointer:  c.fieldPointer,
		args:          append([]any(nil), c.args...),
		tables:        tables,
		aggregate:     c.aggregate,
		distinct:      c.distinct,
		transformed:   c.transformed,
		buildErr:      c.buildErr,
	}
}

// As returns a copy of the column that targets an aliased table reference.
func (c Col[O, T]) As(alias string) Col[O, T] {
	return c.WithTable(AliasTable(c.table, alias))
}

func (c Col[O, T]) Into(fieldPointer FieldPointer[O, T], jsonFieldName string) ResultCol[O, T] {
	return Into[O](c, fieldPointer, jsonFieldName)
}

// Into creates a result-owned projection column from another typed column.
func Into[Target, Source Owner, T any](
	source Column[Source, T],
	fieldPointer FieldPointer[Target, T],
	jsonFieldName string,
) ResultCol[Target, T] {
	pointer := toScanPointer(fieldPointer)
	if isNilValue(source) {
		return ResultCol[Target, T]{col: Col[Target, T]{
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
		buildErr = errors.Trace(carrier.buildError())
	}

	if pointer == nil {
		buildErr = errors.New("field pointer cannot be nil")
	}

	return ResultCol[Target, T]{col: Col[Target, T]{
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

func (c ResultCol[O, T]) SQLExpr() string { return c.col.SQLExpr() }

func (c ResultCol[O, T]) Table() Table { return c.col.Table() }

func (c ResultCol[O, T]) Name() string { return c.col.Name() }

func (c ResultCol[O, T]) QualifiedName() string { return c.col.QualifiedName() }

func (c ResultCol[O, T]) FieldPointer() scanPointer { return c.col.FieldPointer() }

func (c ResultCol[O, T]) OutputName() string { return c.col.OutputName() }

func (c ResultCol[O, T]) JSONFieldName() string { return c.col.JSONFieldName() }

func (c ResultCol[O, T]) scanPointer() scanPointer { return c.col.scanPointer() }

func (c ResultCol[O, T]) columnValue(T) {}

func (c ResultCol[O, T]) selectOwner(O) {}

func (c ResultCol[O, T]) rawQualifiedName() string { return c.col.rawQualifiedName() }

func (c ResultCol[O, T]) expressionArgs() []any { return c.col.expressionArgs() }

func (c ResultCol[O, T]) buildError() error { return c.col.buildError() }

func (c ResultCol[O, T]) tableSource() Table { return c.col.tableSource() }

func (c ResultCol[O, T]) referencedTables() map[string]Table {
	return c.col.referencedTables()
}

func (c ResultCol[O, T]) isAggregateExpression() bool {
	return c.col.isAggregateExpression()
}

func (c ResultCol[O, T]) isDistinctExpression() bool {
	return c.col.isDistinctExpression()
}

func (c ResultCol[O, T]) isTransformedExpression() bool {
	return c.col.isTransformedExpression()
}

func (c Col[O, T]) expressionArgs() []any {
	return append([]any(nil), c.args...)
}

func (c Col[O, T]) buildError() error {
	return errors.Trace(c.buildErr)
}

func (c Col[O, T]) referencedTables() map[string]Table {
	return cloneTableMap(c.tables)
}

func (c Col[O, T]) withTable(table Table) AnyColumn {
	return c.WithTable(table)
}

func (c Col[O, T]) isAggregateExpression() bool {
	return c.aggregate
}

func (c Col[O, T]) isDistinctExpression() bool {
	return c.distinct
}

func (c Col[O, T]) isTransformedExpression() bool {
	return c.transformed
}
