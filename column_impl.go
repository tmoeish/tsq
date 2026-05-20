package tsq

import "errors"

// columnImpl stores the metadata and scan mapping for a typed column expression.
type columnImpl[O Owner, T any] struct {
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

// NewCol creates a new typed column for the table represented by O.
func NewCol[O Table, T any](baseName, jsonFieldName string, fieldPointer FieldPointer[O, T]) Column[O, T] {
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

func newColForTable[O Owner, T any](table Table, baseName, jsonFieldName string, fieldPointer scanPointer) columnImpl[O, T] {
	if isNilValue(table) {
		return columnImpl[O, T]{
			name:          baseName,
			jsonFieldName: jsonFieldName,
			fieldPointer:  fieldPointer,
			buildErr:      errors.New("column table cannot be nil"),
		}
	}

	return columnImpl[O, T]{
		table:         table,
		name:          baseName,
		qualifiedName: rawQualifiedIdentifierForTable(table, baseName),
		jsonFieldName: jsonFieldName,
		fieldPointer:  fieldPointer,
		tables:        map[string]Table{table.Table(): table},
	}
}

// SQLExpr renders the column expression in canonical SQL form.
func (c columnImpl[O, T]) SQLExpr() string {
	return renderCanonicalSQL(c.qualifiedName)
}

// Table returns the primary table that owns the column.
func (c columnImpl[O, T]) Table() Table {
	return c.table
}

// Name returns the underlying physical column name.
func (c columnImpl[O, T]) Name() string {
	return c.name
}

// QualifiedName returns the rendered column reference, including table qualifier.
func (c columnImpl[O, T]) QualifiedName() string {
	return c.SQLExpr()
}

// FieldPointer returns the runtime scan adapter for the bound destination field.
func (c columnImpl[O, T]) FieldPointer() scanPointer {
	return c.scanPointer()
}

// OutputName returns the default column label used in result scans.
func (c columnImpl[O, T]) OutputName() string {
	return c.name
}

// JSONFieldName returns the stable JSON-facing field label for the column.
func (c columnImpl[O, T]) JSONFieldName() string {
	return c.jsonFieldName
}

func (c columnImpl[O, T]) scanPointer() scanPointer {
	return c.fieldPointer
}

func (c columnImpl[O, T]) rawQualifiedName() string {
	return c.qualifiedName
}

func (c columnImpl[O, T]) columnValue(T) {}

func (c columnImpl[O, T]) selectOwner(O) {}

func (c columnImpl[O, T]) tableColumnOwner(O) {}

func (c columnImpl[O, T]) searchColumn() {}

func (c columnImpl[O, T]) tableSource() Table {
	return c.table
}

func (c columnImpl[O, T]) columnName() string {
	return c.name
}

func (c columnImpl[O, T]) withTable(table Table) columnImpl[O, T] {
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

	return columnImpl[O, T]{
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

// WithTable returns a copy of the column rebound to a different table source.
func (c columnImpl[O, T]) WithTable(table Table) Column[O, T] {
	return c.withTable(table)
}

// As returns a copy of the column that targets an aliased table reference.
func (c columnImpl[O, T]) As(alias string) Column[O, T] {
	return c.withTable(AliasTable(c.table, alias))
}

func (c columnImpl[O, T]) expressionArgs() []any {
	return append([]any(nil), c.args...)
}

func (c columnImpl[O, T]) buildError() error {
	return c.buildErr
}

func (c columnImpl[O, T]) referencedTables() map[string]Table {
	return cloneTableMap(c.tables)
}

func (c columnImpl[O, T]) isAggregateExpression() bool {
	return c.aggregate
}

func (c columnImpl[O, T]) isDistinctExpression() bool {
	return c.distinct
}

func (c columnImpl[O, T]) isTransformedExpression() bool {
	return c.transformed
}
