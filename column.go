package tsq

import "fmt"

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
}

// NewCol creates a new typed column for a table
func NewCol[T any](table Table, baseName string, jsonFieldName string, fieldPointer FieldPointer) Col[T] {
	return Col[T]{
		table:         table,
		name:          baseName,
		qualifiedName: fmt.Sprintf("`%s`.`%s`", table.Table(), baseName),
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

// QualifiedName returns the fully qualified column name (e.g., `table`.`column`)
func (c Col[T]) QualifiedName() string {
	return c.qualifiedName
}

// JSONFieldName returns the JSON tag for this column
func (c Col[T]) JSONFieldName() string {
	return c.jsonFieldName
}

// FieldPointer returns the pointer function for scanning database results
func (c Col[T]) FieldPointer() FieldPointer {
	return c.fieldPointer
}

// ================================================
// 字段转换方法
// ================================================

// Into creates a new column with different pointer function and JSON tag
// This is useful for DTOs and custom result mapping
func (c Col[T]) Into(fieldPointer FieldPointer, jsonFieldName string) *Col[T] {
	return &Col[T]{
		table:         c.table,
		name:          c.name,
		qualifiedName: c.qualifiedName,
		fieldPointer:  fieldPointer,
		jsonFieldName: jsonFieldName,
	}
}
