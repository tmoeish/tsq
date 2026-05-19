package tsq

import (
	"errors"
	"fmt"
	"maps"
	"strings"
)

// ================================================
// 基础函数构建方法
// ================================================

// Fn wraps the receiver column in a single-argument SQL function template.
func (c ColumnImpl[Owner, T]) Fn(format string) ColumnImpl[Owner, T] {
	if strings.TrimSpace(format) == "" {
		c.buildErr = errors.New("function format cannot be empty")
		return c
	}

	placeholderCount, err := countStringFormatPlaceholders(format)
	if err != nil {
		c.buildErr = err
		return c
	}

	if placeholderCount != 1 {
		c.buildErr = errors.New("function format must contain %s for the column expression")
		return c
	}

	return ColumnImpl[Owner, T]{
		table:         c.table,
		qualifiedName: fmt.Sprintf(format, c.rawQualifiedName()),
		name:          c.name,
		fieldPointer:  c.fieldPointer,
		jsonFieldName: c.jsonFieldName,
		args:          append([]any(nil), c.args...),
		tables:        cloneTableMap(c.tables),
		aggregate:     c.aggregate,
		distinct:      c.distinct,
		transformed:   true,
		buildErr:      c.buildErr,
	}
}

// FnRaw returns a raw SQL expression that does not format the receiver column into it.
func (c ColumnImpl[Owner, T]) FnRaw(fn string) ColumnImpl[Owner, T] {
	if strings.TrimSpace(fn) == "" {
		c.buildErr = errors.New("function expression cannot be empty")
		return c
	}

	placeholderCount, err := countStringFormatPlaceholders(fn)
	if err != nil {
		c.buildErr = err
		return c
	}

	if placeholderCount != 0 {
		c.buildErr = errors.New("function expression cannot contain format placeholders")
		return c
	}

	return ColumnImpl[Owner, T]{
		table:         c.table,
		qualifiedName: fn,
		name:          c.name,
		fieldPointer:  c.fieldPointer,
		jsonFieldName: c.jsonFieldName,
		args:          append([]any(nil), c.args...),
		tables:        cloneTableMap(c.tables),
		aggregate:     c.aggregate,
		distinct:      c.distinct,
		transformed:   true,
		buildErr:      c.buildErr,
	}
}

// FnExpr formats the receiver column plus extra SQL expressions into format.
func (c ColumnImpl[Owner, T]) FnExpr(format string, args ...any) ColumnImpl[Owner, T] {
	if strings.TrimSpace(format) == "" {
		c.buildErr = errors.New("function format cannot be empty")
		return c
	}

	placeholderCount, err := countStringFormatPlaceholders(format)
	if err != nil {
		c.buildErr = err
		return c
	}

	if placeholderCount != len(args)+1 {
		c.buildErr = errors.New("function format placeholder count mismatch")
		return c
	}

	formatArgs := make([]any, 0, len(args)+1)
	formatArgs = append(formatArgs, c.rawQualifiedName())

	resultArgs := append([]any(nil), c.args...)

	for _, arg := range args {
		expr := argumentToExpression(arg)
		if err := expressionBuildError(expr); err != nil {
			c.buildErr = err
			return c
		}

		formatArgs = append(formatArgs, expr.Expr())
		resultArgs = append(resultArgs, expr.Args()...)
	}

	return ColumnImpl[Owner, T]{
		table:         c.table,
		qualifiedName: fmt.Sprintf(format, formatArgs...),
		name:          c.name,
		fieldPointer:  c.fieldPointer,
		jsonFieldName: c.jsonFieldName,
		args:          resultArgs,
		tables:        mergeTableMaps(c.tables, collectExpressionArgTables(args...)),
		aggregate:     c.aggregate,
		distinct:      c.distinct,
		transformed:   true,
		buildErr:      c.buildErr,
	}
}

func collectExpressionArgTables(args ...any) map[string]Table {
	result := make(map[string]Table)
	for _, arg := range args {
		maps.Copy(result, expressionTables(arg))
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func mergeTableMaps(base, extras map[string]Table) map[string]Table {
	if len(base) == 0 && len(extras) == 0 {
		return nil
	}

	merged := make(map[string]Table, len(base)+len(extras))
	maps.Copy(merged, base)
	maps.Copy(merged, extras)

	return merged
}

// ================================================
// 聚合函数 (Aggregate Functions)
// ================================================

// Count wraps the column in COUNT and marks it as an aggregate expression.
func (c ColumnImpl[Owner, T]) Count() ColumnImpl[Owner, int64] {
	result := ColumnImpl[Owner, int64](c.Fn("COUNT(%s)"))
	result.aggregate = true

	return result
}

// Sum wraps the column in SUM and marks it as an aggregate expression.
func (c ColumnImpl[Owner, T]) Sum() ColumnImpl[Owner, T] {
	result := c.Fn("SUM(%s)")
	result.aggregate = true

	return result
}

// Avg wraps the column in AVG and marks it as an aggregate expression.
func (c ColumnImpl[Owner, T]) Avg() ColumnImpl[Owner, float64] {
	result := ColumnImpl[Owner, float64](c.Fn("AVG(%s)"))
	result.aggregate = true

	return result
}

// Max wraps the column in MAX and marks it as an aggregate expression.
func (c ColumnImpl[Owner, T]) Max() ColumnImpl[Owner, T] {
	result := c.Fn("MAX(%s)")
	result.aggregate = true

	return result
}

// Min wraps the column in MIN and marks it as an aggregate expression.
func (c ColumnImpl[Owner, T]) Min() ColumnImpl[Owner, T] {
	result := c.Fn("MIN(%s)")
	result.aggregate = true

	return result
}

// Distinct wraps the column in DISTINCT and marks it as a distinct expression.
func (c ColumnImpl[Owner, T]) Distinct() ColumnImpl[Owner, T] {
	result := c.Fn("DISTINCT(%s)")
	result.distinct = true

	return result
}

// ================================================
// 字符串函数 (String Functions)
// ================================================

// Upper applies UPPER to the column.
func (c ColumnImpl[Owner, T]) Upper() ColumnImpl[Owner, T] {
	return c.Fn("UPPER(%s)")
}

// Lower applies LOWER to the column.
func (c ColumnImpl[Owner, T]) Lower() ColumnImpl[Owner, T] {
	return c.Fn("LOWER(%s)")
}

// Substring applies SUBSTRING(column, start, length).
func (c ColumnImpl[Owner, T]) Substring(start, length int) ColumnImpl[Owner, T] {
	return c.Fn(fmt.Sprintf("SUBSTRING(%%s, %d, %d)", start, length))
}

// Length applies LENGTH to the column.
func (c ColumnImpl[Owner, T]) Length() ColumnImpl[Owner, T] {
	return c.Fn("LENGTH(%s)")
}

// Trim applies TRIM to the column.
func (c ColumnImpl[Owner, T]) Trim() ColumnImpl[Owner, T] {
	return c.Fn("TRIM(%s)")
}

// Concat is intentionally unsupported because portable string concatenation
// differs across TSQ's built-in dialects.
func (c ColumnImpl[Owner, T]) Concat(_ string) ColumnImpl[Owner, T] {
	c.buildErr = errors.New("concat is not portable across TSQ's built-in dialects; use Fn with a dialect-specific expression instead")
	return c
}

// ================================================
// 日期和时间函数 (Date/Time Functions)
// ================================================

// Now replaces the receiver with the CURRENT_TIMESTAMP expression.
func (c ColumnImpl[Owner, T]) Now() ColumnImpl[Owner, T] {
	return c.FnRaw("CURRENT_TIMESTAMP")
}

// Date extracts the date portion of the column with DATE(...).
func (c ColumnImpl[Owner, T]) Date() ColumnImpl[Owner, T] {
	return c.Fn("DATE(%s)")
}

// Year extracts the four-digit year from the column using portable SQL.
func (c ColumnImpl[Owner, T]) Year() ColumnImpl[Owner, T] {
	return c.Fn("SUBSTR(DATE(%s), 1, 4)")
}

// Month extracts the two-digit month from the column using portable SQL.
func (c ColumnImpl[Owner, T]) Month() ColumnImpl[Owner, T] {
	return c.Fn("SUBSTR(DATE(%s), 6, 2)")
}

// Day extracts the two-digit day from the column using portable SQL.
func (c ColumnImpl[Owner, T]) Day() ColumnImpl[Owner, T] {
	return c.Fn("SUBSTR(DATE(%s), 9, 2)")
}

// ================================================
// 数学函数 (Math Functions)
// ================================================

// Round applies ROUND(column, precision).
func (c ColumnImpl[Owner, T]) Round(precision int) ColumnImpl[Owner, T] {
	if precision < 0 {
		c.buildErr = errors.New("round precision cannot be negative")
		return c
	}

	return c.Fn(fmt.Sprintf("ROUND(%%s, %d)", precision))
}

// Ceil applies CEIL to the column.
func (c ColumnImpl[Owner, T]) Ceil() ColumnImpl[Owner, T] {
	return c.Fn("CEIL(%s)")
}

// Floor applies FLOOR to the column.
func (c ColumnImpl[Owner, T]) Floor() ColumnImpl[Owner, T] {
	return c.Fn("FLOOR(%s)")
}

// Abs applies ABS to the column.
func (c ColumnImpl[Owner, T]) Abs() ColumnImpl[Owner, T] {
	return c.Fn("ABS(%s)")
}

// ================================================
// 条件函数 (Conditional Functions)
// ================================================

// Coalesce applies COALESCE(column, value).
func (c ColumnImpl[Owner, T]) Coalesce(value any) ColumnImpl[Owner, T] {
	return c.FnExpr("COALESCE(%s, %s)", value)
}

// NullIf applies NULLIF(column, value).
func (c ColumnImpl[Owner, T]) NullIf(value any) ColumnImpl[Owner, T] {
	return c.FnExpr("NULLIF(%s, %s)", value)
}
