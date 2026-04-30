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

// Col represents a typed column in a database table
type Col[T any] struct {
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

// NewCol creates a new typed column for a table
func NewCol[T any](table Table, baseName, jsonFieldName string, fieldPointer FieldPointer) Col[T] {
	if isNilValue(table) {
		return Col[T]{
			name:          baseName,
			jsonFieldName: jsonFieldName,
			fieldPointer:  fieldPointer,
			buildErr:      errors.New("column table cannot be nil"),
		}
	}

	return Col[T]{
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
func (c Col[T]) Table() Table {
	return c.table
}

// Name returns the column name
func (c Col[T]) Name() string {
	return c.name
}

// QualifiedName returns the fully qualified column name in TSQ's canonical SQL form.
func (c Col[T]) QualifiedName() string {
	return renderCanonicalSQL(c.qualifiedName)
}

// JSONFieldName returns the JSON tag for this column
func (c Col[T]) JSONFieldName() string {
	return c.jsonFieldName
}

// FieldPointer returns the pointer function for scanning database results
func (c Col[T]) FieldPointer() FieldPointer {
	return c.fieldPointer
}

func (c Col[T]) rawQualifiedName() string {
	return c.qualifiedName
}

// ================================================
// 字段转换方法
// ================================================

// WithTable returns a copy of the column rebound to a different table source.
// Rebinding transformed expressions is intentionally unsupported; alias or
// rebind the base column before applying functions such as Fn/Count/Distinct.
func (c Col[T]) WithTable(table Table) Col[T] {
	if isNilValue(table) {
		c.buildErr = errors.New("column table cannot be nil")
		return c
	}

	if c.transformed {
		c.buildErr = errors.New("cannot rebind transformed column; alias the base column before applying expressions")
		return c
	}

	return Col[T]{
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
func (c Col[T]) As(alias string) Col[T] {
	return c.WithTable(AliasTable(c.table, alias))
}

// Into creates a new column with different pointer function and JSON tag
// This is useful for results and custom result mapping
func (c Col[T]) Into(fieldPointer FieldPointer, jsonFieldName string) *Col[T] {
	if fieldPointer == nil {
		return &Col[T]{
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
		}
	}

	return &Col[T]{
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
	}
}

func (c Col[T]) expressionArgs() []any {
	return append([]any(nil), c.args...)
}

func (c Col[T]) buildError() error {
	return errors.Trace(c.buildErr)
}

func (c Col[T]) referencedTables() map[string]Table {
	return cloneTableMap(c.tables)
}

func (c Col[T]) withTable(table Table) Column {
	rebound := c.WithTable(table)
	return rebound
}

func (c Col[T]) isAggregateExpression() bool {
	return c.aggregate
}

func (c Col[T]) isDistinctExpression() bool {
	return c.distinct
}
