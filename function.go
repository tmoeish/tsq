package tsq

import (
	"fmt"
)

// ================================================
// 基础函数构建方法
// ================================================

// Fn creates a custom SQL function by applying the format string to the column
func (c Col[T]) Fn(format string) Col[T] {
	return Col[T]{
		table:         c.table,
		qualifiedName: fmt.Sprintf(format, c.QualifiedName()),
		name:          c.name,          // 保持原始名称
		fieldPointer:  c.fieldPointer,  // 保持原始指针函数
		jsonFieldName: c.jsonFieldName, // 保持原始JSON标签
	}
}

// Fn0 fn 不带参数
func (c Col[T]) Fn0(fn string) Col[T] {
	return Col[T]{
		table:         c.table,
		qualifiedName: fn,
		name:          c.name,          // 保持原始名称
		fieldPointer:  c.fieldPointer,  // 保持原始指针函数
		jsonFieldName: c.jsonFieldName, // 保持原始JSON标签
	}
}

// ================================================
// 聚合函数 (Aggregate Functions)
// ================================================

// Count returns COUNT(column) - counts non-null values
func (c Col[T]) Count() Col[T] {
	return c.Fn("COUNT(%s)")
}

// Sum returns SUM(column) - calculates sum of numeric values
func (c Col[T]) Sum() Col[T] {
	return c.Fn("SUM(%s)")
}

// Avg returns AVG(column) - calculates average of numeric values
func (c Col[T]) Avg() Col[T] {
	return c.Fn("AVG(%s)")
}

// Max returns MAX(column) - finds maximum value
func (c Col[T]) Max() Col[T] {
	return c.Fn("MAX(%s)")
}

// Min returns MIN(column) - finds minimum value
func (c Col[T]) Min() Col[T] {
	return c.Fn("MIN(%s)")
}

// Distinct returns DISTINCT(column) - returns unique values
func (c Col[T]) Distinct() Col[T] {
	return c.Fn("DISTINCT(%s)")
}

// ================================================
// 字符串函数 (String Functions)
// ================================================

// Upper returns UPPER(column) - converts to uppercase
func (c Col[T]) Upper() Col[T] {
	return c.Fn("UPPER(%s)")
}

// Lower returns LOWER(column) - converts to lowercase
func (c Col[T]) Lower() Col[T] {
	return c.Fn("LOWER(%s)")
}

// Substring returns SUBSTRING(column, start, length) - extracts substring
func (c Col[T]) Substring(start, length int) Col[T] {
	return c.Fn(fmt.Sprintf("SUBSTRING(%%s, %d, %d)", start, length))
}

// Length returns LENGTH(column) - returns string length
func (c Col[T]) Length() Col[T] {
	return c.Fn("LENGTH(%s)")
}

// Trim returns TRIM(column) - removes leading and trailing spaces
func (c Col[T]) Trim() Col[T] {
	return c.Fn("TRIM(%s)")
}

// Concat returns CONCAT(column, 'other') - concatenates strings
func (c Col[T]) Concat(other string) Col[T] {
	return c.Fn(fmt.Sprintf("CONCAT(%%s, '%s')", other))
}

// ================================================
// 日期和时间函数 (Date/Time Functions)
// ================================================

// Now returns NOW() - current timestamp (usually used as static function)
func (c Col[T]) Now() Col[T] {
	return c.Fn0("NOW()")
}

// Date returns DATE(column) - extracts date part from datetime
func (c Col[T]) Date() Col[T] {
	return c.Fn("DATE(%s)")
}

// Year returns YEAR(column) - extracts year from date
func (c Col[T]) Year() Col[T] {
	return c.Fn("YEAR(%s)")
}

// Month returns MONTH(column) - extracts month from date
func (c Col[T]) Month() Col[T] {
	return c.Fn("MONTH(%s)")
}

// Day returns DAY(column) - extracts day from date
func (c Col[T]) Day() Col[T] {
	return c.Fn("DAY(%s)")
}

// ================================================
// 数学函数 (Math Functions)
// ================================================

// Round returns ROUND(column, precision) - rounds to specified decimal places
func (c Col[T]) Round(precision int) Col[T] {
	return c.Fn(fmt.Sprintf("ROUND(%%s, %d)", precision))
}

// Ceil returns CEIL(column) - rounds up to nearest integer
func (c Col[T]) Ceil() Col[T] {
	return c.Fn("CEIL(%s)")
}

// Floor returns FLOOR(column) - rounds down to nearest integer
func (c Col[T]) Floor() Col[T] {
	return c.Fn("FLOOR(%s)")
}

// Abs returns ABS(column) - returns absolute value
func (c Col[T]) Abs() Col[T] {
	return c.Fn("ABS(%s)")
}

// ================================================
// 条件函数 (Conditional Functions)
// ================================================

// Coalesce returns COALESCE(column, 'value') - returns first non-null value
func (c Col[T]) Coalesce(value string) Col[T] {
	return c.Fn(fmt.Sprintf("COALESCE(%%s, '%s')", value))
}

// NullIf returns NULLIF(column, 'value') - returns NULL if values are equal
func (c Col[T]) NullIf(value string) Col[T] {
	return c.Fn(fmt.Sprintf("NULLIF(%%s, '%s')", value))
}
