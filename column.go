package tsq

import "github.com/juju/errors"

// FieldPointer 返回指向 Owner 类型中某个字段的类型安全指针。
// 架构意图：它使用 Go 1.18+ 的范型和闭包，建立了 SQL 列与 struct 字段之间的直接联系，
// 而不需要使用低效的反射字符串查找。
type FieldPointer[O Owner, T any] func(*O) *T

// scanPointer 是擦除了类型的运行时扫描函数。
type scanPointer func(holder any) any

// SQLColumn 是 SQL 表达式在运行时的视图。
// 它定义了一个表达式如何被渲染，以及被选中后如何扫描回 Go 对象。
type SQLColumn interface {
	SQLExpr() string           // 渲染后的 SQL 表达式（例如 "users.name"）
	OutputName() string        // 默认的输出列名（别名）
	JSONFieldName() string     // 对应的 JSON 标签名，用于排序白名单
	Table() Table              // 所属的主表
	Name() string              // 原始物理列名
	QualifiedName() string     // 限定名（带表名）
	scanPointer() scanPointer  // 获取扫描函数
	referencedTables() map[string]Table // 表达式中涉及的所有表
}

// SQLColumns 将一组类型安全的列转换为擦除类型的运行时列列表。
func SQLColumns[O Owner](cols ...BoundColumn[O]) []SQLColumn {
	result := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		result = append(result, col)
	}

	return result
}

// BoundColumn 是一个绑定到 Owner [O] 的可选择 SQL 表达式。
type BoundColumn[O Owner] interface {
	SQLColumn
	selectOwner(O) // 幻影方法，用于范型类型约束
}

// ColumnImpl 是类型安全列的基础实现。
// 它存储了列的所有静态元数据和映射逻辑。
type ColumnImpl[O Owner, T any] struct {
	table         Table
	name          string
	qualifiedName string
	jsonFieldName string
	fieldPointer  scanPointer
	args          []any
	tables        map[string]Table
	aggregate     bool // 是否为聚合函数（SUM, COUNT等）
	distinct      bool // 是否包含 DISTINCT
	transformed   bool // 是否经过了某种变换（如 UPPER()），通常不能再被重定向
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
