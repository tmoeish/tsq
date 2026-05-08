package tsq

import "github.com/juju/errors"

// FieldPointer returns a typed pointer into an owner value.
type FieldPointer[O Owner, T any] func(*O) *T

type scanPointer func(holder any) any

// SQLColumn is the erased runtime view of a SQL expression that can be rendered
// and, when selected, scanned into an owner field.
type SQLColumn interface {
	SQLExpr() string
	OutputName() string
	JSONFieldName() string
	Table() Table
	Name() string
	QualifiedName() string
	scanPointer() scanPointer
	referencedTables() map[string]Table
}

// SQLColumns erases a typed column slice to runtime columns.
func SQLColumns[O Owner](cols ...BoundColumn[O]) []SQLColumn {
	result := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		result = append(result, col)
	}

	return result
}

// BoundColumn is a SQL expression selectable into an owner.
type BoundColumn[O Owner] interface {
	SQLColumn
	selectOwner(O)
}

// TypedColumn is a value-typed selectable column.
type TypedColumn[O Owner, T any] interface {
	BoundColumn[O]
	columnValue(T)
}

// TableColumn is an owner-typed physical/source column.
type TableColumn[O Table] interface {
	BoundColumn[O]
	SearchColumn
	tableColumnOwner(O)
	tableSource() Table
	columnName() string
}

// SearchColumn is a SQL expression allowed in keyword search.
type SearchColumn interface {
	SQLColumn
	searchColumn()
}

type typedColumnInternal[T any] interface {
	SQLColumn
	columnValue(T)
}

// ColumnImpl represents a typed source expression.
type ColumnImpl[O Owner, T any] struct {
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

// ProjectedColumn is a projection-only column selected into a result owner.
type ProjectedColumn[O Owner, T any] struct {
	col ColumnImpl[O, T]
}

// NewCol creates a new typed column for the table represented by O.
func NewCol[O Table, T any](baseName, jsonFieldName string, fieldPointer FieldPointer[O, T]) ColumnImpl[O, T] {
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

func newColForTable[O Owner, T any](table Table, baseName, jsonFieldName string, fieldPointer scanPointer) ColumnImpl[O, T] {
	if isNilValue(table) {
		return ColumnImpl[O, T]{
			name:          baseName,
			jsonFieldName: jsonFieldName,
			fieldPointer:  fieldPointer,
			buildErr:      errors.New("column table cannot be nil"),
		}
	}

	return ColumnImpl[O, T]{
		table:         table,
		name:          baseName,
		qualifiedName: rawQualifiedIdentifierForTable(table, baseName),
		jsonFieldName: jsonFieldName,
		fieldPointer:  fieldPointer,
		tables:        map[string]Table{table.Table(): table},
	}
}

func (c ColumnImpl[O, T]) SQLExpr() string {
	return renderCanonicalSQL(c.qualifiedName)
}

func (c ColumnImpl[O, T]) Table() Table {
	return c.table
}

func (c ColumnImpl[O, T]) Name() string {
	return c.name
}

func (c ColumnImpl[O, T]) QualifiedName() string {
	return c.SQLExpr()
}

func (c ColumnImpl[O, T]) FieldPointer() scanPointer {
	return c.scanPointer()
}

func (c ColumnImpl[O, T]) OutputName() string {
	return c.name
}

func (c ColumnImpl[O, T]) JSONFieldName() string {
	return c.jsonFieldName
}

func (c ColumnImpl[O, T]) scanPointer() scanPointer {
	return c.fieldPointer
}

func (c ColumnImpl[O, T]) rawQualifiedName() string {
	return c.qualifiedName
}

func (c ColumnImpl[O, T]) columnValue(T) {}

func (c ColumnImpl[O, T]) selectOwner(O) {}

func (c ColumnImpl[O, T]) tableColumnOwner(O) {}

func (c ColumnImpl[O, T]) searchColumn() {}

func (c ColumnImpl[O, T]) tableSource() Table {
	return c.table
}

func (c ColumnImpl[O, T]) columnName() string {
	return c.name
}

// WithTable returns a copy of the column rebound to a different table source.
func (c ColumnImpl[O, T]) WithTable(table Table) ColumnImpl[O, T] {
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

	return ColumnImpl[O, T]{
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
func (c ColumnImpl[O, T]) As(alias string) ColumnImpl[O, T] {
	return c.WithTable(AliasTable(c.table, alias))
}

// Into creates a result-owned projection column from another typed column.
func Into[Target, Source Owner, T any](
	source TypedColumn[Source, T],
	fieldPointer FieldPointer[Target, T],
	jsonFieldName string,
) ProjectedColumn[Target, T] {
	pointer := toScanPointer(fieldPointer)
	if isNilValue(source) {
		return ProjectedColumn[Target, T]{col: ColumnImpl[Target, T]{
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

	return ProjectedColumn[Target, T]{col: ColumnImpl[Target, T]{
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

func (c ProjectedColumn[O, T]) SQLExpr() string { return c.col.SQLExpr() }

func (c ProjectedColumn[O, T]) Table() Table { return c.col.Table() }

func (c ProjectedColumn[O, T]) Name() string { return c.col.Name() }

func (c ProjectedColumn[O, T]) QualifiedName() string { return c.col.QualifiedName() }

func (c ProjectedColumn[O, T]) FieldPointer() scanPointer { return c.col.FieldPointer() }

func (c ProjectedColumn[O, T]) OutputName() string { return c.col.OutputName() }

func (c ProjectedColumn[O, T]) JSONFieldName() string { return c.col.JSONFieldName() }

func (c ProjectedColumn[O, T]) scanPointer() scanPointer { return c.col.scanPointer() }

func (c ProjectedColumn[O, T]) columnValue(T) {}

func (c ProjectedColumn[O, T]) selectOwner(O) {}

func (c ProjectedColumn[O, T]) rawQualifiedName() string { return c.col.rawQualifiedName() }

func (c ProjectedColumn[O, T]) expressionArgs() []any { return c.col.expressionArgs() }

func (c ProjectedColumn[O, T]) buildError() error { return c.col.buildError() }

func (c ProjectedColumn[O, T]) tableSource() Table { return c.col.tableSource() }

func (c ProjectedColumn[O, T]) referencedTables() map[string]Table {
	return c.col.referencedTables()
}

func (c ProjectedColumn[O, T]) isAggregateExpression() bool {
	return c.col.isAggregateExpression()
}

func (c ProjectedColumn[O, T]) isDistinctExpression() bool {
	return c.col.isDistinctExpression()
}

func (c ProjectedColumn[O, T]) isTransformedExpression() bool {
	return c.col.isTransformedExpression()
}

func (c ColumnImpl[O, T]) expressionArgs() []any {
	return append([]any(nil), c.args...)
}

func (c ColumnImpl[O, T]) buildError() error {
	return errors.Trace(c.buildErr)
}

func (c ColumnImpl[O, T]) referencedTables() map[string]Table {
	return cloneTableMap(c.tables)
}

func (c ColumnImpl[O, T]) withTable(table Table) SQLColumn {
	return c.WithTable(table)
}

func (c ColumnImpl[O, T]) isAggregateExpression() bool {
	return c.aggregate
}

func (c ColumnImpl[O, T]) isDistinctExpression() bool {
	return c.distinct
}

func (c ColumnImpl[O, T]) isTransformedExpression() bool {
	return c.transformed
}
