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

func (c columnImpl[Owner, T]) expr(format string) columnImpl[Owner, T] {
	if strings.TrimSpace(format) == "" {
		c.buildErr = errors.New("expression format cannot be empty")
		return c
	}

	placeholderCount, err := countStringFormatPlaceholders(format)
	if err != nil {
		c.buildErr = err
		return c
	}

	if placeholderCount != 1 {
		c.buildErr = errors.New("expression format must contain %s for the receiver column")
		return c
	}

	return columnImpl[Owner, T]{
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

// Expr formats the receiver column into a custom SQL expression template.
func (c columnImpl[Owner, T]) Expr(format string) Column[Owner, T] {
	return c.expr(format)
}

// rawExpr replaces the receiver SQL with a raw expression while preserving the
// column's owner, scan metadata, and tracked tables.
func (c columnImpl[Owner, T]) rawExpr(expr string) columnImpl[Owner, T] {
	if strings.TrimSpace(expr) == "" {
		c.buildErr = errors.New("expression cannot be empty")
		return c
	}

	placeholderCount, err := countStringFormatPlaceholders(expr)
	if err != nil {
		c.buildErr = err
		return c
	}

	if placeholderCount != 0 {
		c.buildErr = errors.New("raw expression cannot contain format placeholders")
		return c
	}

	return columnImpl[Owner, T]{
		table:         c.table,
		qualifiedName: expr,
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

// Exprf formats the receiver column plus extra SQL expressions into format.
func (c columnImpl[Owner, T]) exprf(format string, args ...any) columnImpl[Owner, T] {
	if strings.TrimSpace(format) == "" {
		c.buildErr = errors.New("expression format cannot be empty")
		return c
	}

	placeholderCount, err := countStringFormatPlaceholders(format)
	if err != nil {
		c.buildErr = err
		return c
	}

	if placeholderCount != len(args)+1 {
		c.buildErr = errors.New("expression format placeholder count mismatch")
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

	return columnImpl[Owner, T]{
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

// Exprf formats the receiver column plus extra SQL expressions into format.
func (c columnImpl[Owner, T]) Exprf(format string, args ...any) Column[Owner, T] {
	return c.exprf(format, args...)
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
func (c columnImpl[Owner, T]) Count() Column[Owner, int64] {
	result := columnImpl[Owner, int64](c.expr("COUNT(%s)"))
	result.aggregate = true

	return result
}

// Sum wraps the column in SUM and marks it as an aggregate expression.
func (c columnImpl[Owner, T]) Sum() Column[Owner, T] {
	result := c.expr("SUM(%s)")
	result.aggregate = true

	return result
}

// Avg wraps the column in AVG and marks it as an aggregate expression.
func (c columnImpl[Owner, T]) Avg() Column[Owner, float64] {
	result := columnImpl[Owner, float64](c.expr("AVG(%s)"))
	result.aggregate = true

	return result
}

// Max wraps the column in MAX and marks it as an aggregate expression.
func (c columnImpl[Owner, T]) Max() Column[Owner, T] {
	result := c.expr("MAX(%s)")
	result.aggregate = true

	return result
}

// Min wraps the column in MIN and marks it as an aggregate expression.
func (c columnImpl[Owner, T]) Min() Column[Owner, T] {
	result := c.expr("MIN(%s)")
	result.aggregate = true

	return result
}

// Distinct wraps the column in DISTINCT and marks it as a distinct expression.
func (c columnImpl[Owner, T]) Distinct() Column[Owner, T] {
	result := c.expr("DISTINCT(%s)")
	result.distinct = true

	return result
}

// ================================================
// 字符串函数 (String Functions)
// ================================================

// Upper applies UPPER to the column.
func (c columnImpl[Owner, T]) Upper() Column[Owner, T] {
	return c.expr("UPPER(%s)")
}

// Lower applies LOWER to the column.
func (c columnImpl[Owner, T]) Lower() Column[Owner, T] {
	return c.expr("LOWER(%s)")
}

// Substring applies SUBSTRING(column, start, length).
func (c columnImpl[Owner, T]) Substring(start, length int) Column[Owner, T] {
	return c.expr(fmt.Sprintf("SUBSTRING(%%s, %d, %d)", start, length))
}

// Length applies LENGTH to the column.
func (c columnImpl[Owner, T]) Length() Column[Owner, T] {
	return c.expr("LENGTH(%s)")
}

// Trim applies TRIM to the column.
func (c columnImpl[Owner, T]) Trim() Column[Owner, T] {
	return c.expr("TRIM(%s)")
}

// Concat is intentionally unsupported because portable string concatenation
// differs across TSQ's built-in dialects.
func (c columnImpl[Owner, T]) Concat(_ string) Column[Owner, T] {
	c.buildErr = errors.New("concat is not portable across TSQ's built-in dialects; use Expr with a dialect-specific expression instead")
	return c
}

// ================================================
// 日期和时间函数 (Date/Time Functions)
// ================================================

// Now replaces the receiver with the CURRENT_TIMESTAMP expression.
func (c columnImpl[Owner, T]) Now() Column[Owner, T] {
	return c.rawExpr("CURRENT_TIMESTAMP")
}

// Date extracts the date portion of the column with DATE(...).
func (c columnImpl[Owner, T]) Date() Column[Owner, T] {
	return c.expr("DATE(%s)")
}

// Year extracts the four-digit year from the column using portable SQL.
func (c columnImpl[Owner, T]) Year() Column[Owner, T] {
	return c.expr("SUBSTR(DATE(%s), 1, 4)")
}

// Month extracts the two-digit month from the column using portable SQL.
func (c columnImpl[Owner, T]) Month() Column[Owner, T] {
	return c.expr("SUBSTR(DATE(%s), 6, 2)")
}

// Day extracts the two-digit day from the column using portable SQL.
func (c columnImpl[Owner, T]) Day() Column[Owner, T] {
	return c.expr("SUBSTR(DATE(%s), 9, 2)")
}

// ================================================
// 数学函数 (Math Functions)
// ================================================

// Round applies ROUND(column, precision).
func (c columnImpl[Owner, T]) Round(precision int) Column[Owner, T] {
	if precision < 0 {
		c.buildErr = errors.New("round precision cannot be negative")
		return c
	}

	return c.expr(fmt.Sprintf("ROUND(%%s, %d)", precision))
}

// Ceil applies CEIL to the column.
func (c columnImpl[Owner, T]) Ceil() Column[Owner, T] {
	return c.expr("CEIL(%s)")
}

// Floor applies FLOOR to the column.
func (c columnImpl[Owner, T]) Floor() Column[Owner, T] {
	return c.expr("FLOOR(%s)")
}

// Abs applies ABS to the column.
func (c columnImpl[Owner, T]) Abs() Column[Owner, T] {
	return c.expr("ABS(%s)")
}

// ================================================
// 条件函数 (Conditional Functions)
// ================================================

// Coalesce applies COALESCE(column, value).
func (c columnImpl[Owner, T]) Coalesce(value any) Column[Owner, T] {
	return c.exprf("COALESCE(%s, %s)", value)
}

// NullIf applies NULLIF(column, value).
func (c columnImpl[Owner, T]) NullIf(value any) Column[Owner, T] {
	return c.exprf("NULLIF(%s, %s)", value)
}
