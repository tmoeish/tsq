package tsq

import "github.com/juju/errors"

// ================================================
// 字段指针类型
// ================================================

// FieldPointer is a function that returns a pointer to a field in a struct.
type FieldPointer func(holder any) any

// ================================================
// 字段接口定义
// ================================================

// Column represents a column in a database table
type Column interface {
	Table() Table               // Returns the table this column belongs to
	Name() string               // Returns the column name
	QualifiedName() string      // Returns the fully qualified column name
	JSONFieldName() string      // Returns the JSON tag for serialization
	FieldPointer() FieldPointer // Returns the pointer function for scanning
}

// ================================================
// 字段实现结构体
// ================================================

// TypedColumn is implemented by TSQ columns that carry a Go value type.
type typedColumn[T any] interface {
	Column
	columnValue(T)
}

// Col represents a typed column in a database table.
// Owner is the Go type that owns the generated column, normally the table
// struct type. T is the Go value type stored in the column.
type Col[Owner, T any] struct {
	table         Table        // 所属表
	name          string       // 列名
	qualifiedName string       // 完整列名（包含表名）
	jsonFieldName string       // JSON 标签
	fieldPointer  FieldPointer // 指针函数
	args          []any
	tables        map[string]Table
	aggregate     bool
	distinct      bool
	transformed   bool
	buildErr      error
}

// ResultCol is a projection-only column produced by Col.Into.
// It can be selected and scanned, but intentionally does not expose predicate
// methods such as EQVar so Result fields cannot be used as query inputs.
type ResultCol[Owner, T any] struct {
	col Col[Owner, T]
}

// NewCol creates a new typed column for a table
func NewCol[Owner, T any](table Table, baseName, jsonFieldName string, fieldPointer FieldPointer) Col[Owner, T] {
	if isNilValue(table) {
		return Col[Owner, T]{
			name:          baseName,
			jsonFieldName: jsonFieldName,
			fieldPointer:  fieldPointer,
			buildErr:      errors.New("column table cannot be nil"),
		}
	}

	return Col[Owner, T]{
		table:         table,
		name:          baseName,
		qualifiedName: rawQualifiedIdentifierForTable(table, baseName),
		jsonFieldName: jsonFieldName,
		fieldPointer:  fieldPointer,
	}
}

// ================================================
// 字段属性方法
// ================================================

// Table returns the table this column belongs to
func (c Col[Owner, T]) Table() Table {
	return c.table
}

// Name returns the column name
func (c Col[Owner, T]) Name() string {
	return c.name
}

// QualifiedName returns the fully qualified column name in TSQ's canonical SQL form.
func (c Col[Owner, T]) QualifiedName() string {
	return renderCanonicalSQL(c.qualifiedName)
}

// JSONFieldName returns the JSON tag for this column
func (c Col[Owner, T]) JSONFieldName() string {
	return c.jsonFieldName
}

// FieldPointer returns the pointer function for scanning database results
func (c Col[Owner, T]) FieldPointer() FieldPointer {
	return c.fieldPointer
}

func (c Col[Owner, T]) rawQualifiedName() string {
	return c.qualifiedName
}

func (c Col[Owner, T]) columnValue(T) {}

// ================================================
// 字段转换方法
// ================================================

// WithTable returns a copy of the column rebound to a different table source.
// Rebinding transformed expressions is intentionally unsupported; alias or
// rebind the base column before applying functions such as Fn/Count/Distinct.
func (c Col[Owner, T]) WithTable(table Table) Col[Owner, T] {
	if isNilValue(table) {
		c.buildErr = errors.New("column table cannot be nil")
		return c
	}

	if c.transformed {
		c.buildErr = errors.New("cannot rebind transformed column; alias the base column before applying expressions")
		return c
	}

	return Col[Owner, T]{
		table:         table,
		name:          c.name,
		qualifiedName: rawQualifiedIdentifierForTable(table, c.name),
		jsonFieldName: c.jsonFieldName,
		fieldPointer:  c.fieldPointer,
		args:          append([]any(nil), c.args...),
		tables:        cloneTableMap(c.tables),
		aggregate:     c.aggregate,
		distinct:      c.distinct,
		transformed:   c.transformed,
		buildErr:      c.buildErr,
	}
}

// As returns a copy of the column that targets an aliased table reference.
func (c Col[Owner, T]) As(alias string) Col[Owner, T] {
	return c.WithTable(AliasTable(c.table, alias))
}

// Into creates a new column with different pointer function and JSON tag
// This is useful for results and custom result mapping
func (c Col[Owner, T]) Into(fieldPointer FieldPointer, jsonFieldName string) ResultCol[Owner, T] {
	if fieldPointer == nil {
		return ResultCol[Owner, T]{col: Col[Owner, T]{
			table:         c.table,
			name:          c.name,
			qualifiedName: c.qualifiedName,
			fieldPointer:  fieldPointer,
			jsonFieldName: jsonFieldName,
			args:          append([]any(nil), c.args...),
			tables:        cloneTableMap(c.tables),
			aggregate:     c.aggregate,
			distinct:      c.distinct,
			transformed:   c.transformed,
			buildErr:      errors.New("field pointer cannot be nil"),
		}}
	}

	return ResultCol[Owner, T]{col: Col[Owner, T]{
		table:         c.table,
		name:          c.name,
		qualifiedName: c.qualifiedName,
		fieldPointer:  fieldPointer,
		jsonFieldName: jsonFieldName,
		args:          append([]any(nil), c.args...),
		tables:        cloneTableMap(c.tables),
		aggregate:     c.aggregate,
		distinct:      c.distinct,
		transformed:   c.transformed,
		buildErr:      c.buildErr,
	}}
}

func (c ResultCol[Owner, T]) Table() Table { return c.col.Table() }

func (c ResultCol[Owner, T]) Name() string { return c.col.Name() }

func (c ResultCol[Owner, T]) QualifiedName() string { return c.col.QualifiedName() }

func (c ResultCol[Owner, T]) JSONFieldName() string { return c.col.JSONFieldName() }

func (c ResultCol[Owner, T]) FieldPointer() FieldPointer { return c.col.FieldPointer() }

func (c ResultCol[Owner, T]) rawQualifiedName() string { return c.col.rawQualifiedName() }

func (c ResultCol[Owner, T]) expressionArgs() []any { return c.col.expressionArgs() }

func (c ResultCol[Owner, T]) buildError() error { return c.col.buildError() }

func (c ResultCol[Owner, T]) referencedTables() map[string]Table {
	return c.col.referencedTables()
}

func (c ResultCol[Owner, T]) isAggregateExpression() bool {
	return c.col.isAggregateExpression()
}

func (c ResultCol[Owner, T]) isDistinctExpression() bool {
	return c.col.isDistinctExpression()
}

func (c ResultCol[Owner, T]) isTransformedExpression() bool {
	return c.col.isTransformedExpression()
}

func (c Col[Owner, T]) expressionArgs() []any {
	return append([]any(nil), c.args...)
}

func (c Col[Owner, T]) buildError() error {
	return errors.Trace(c.buildErr)
}

func (c Col[Owner, T]) referencedTables() map[string]Table {
	return cloneTableMap(c.tables)
}

func (c Col[Owner, T]) withTable(table Table) Column {
	rebound := c.WithTable(table)
	return rebound
}

func (c Col[Owner, T]) isAggregateExpression() bool {
	return c.aggregate
}

func (c Col[Owner, T]) isDistinctExpression() bool {
	return c.distinct
}

func (c Col[Owner, T]) isTransformedExpression() bool {
	return c.transformed
}
