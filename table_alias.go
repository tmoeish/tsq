package tsq

import "strings"

type physicalTabler interface {
	PhysicalTable() string
}

type tableAliaser interface {
	Alias() string
}

type tableRebinder interface {
	withTable(Table) SQLColumn
}

type reboundColumn struct {
	source SQLColumn
	table  Table
}

type reboundSearchColumn struct {
	reboundColumn
}

type aliasedTable struct {
	base  Table
	alias string
}

// AliasTable returns table wrapped with the provided SQL alias.
func AliasTable(table Table, alias string) Table {
	if isNilValue(table) {
		return nil
	}

	alias = strings.TrimSpace(alias)
	if alias == "" || alias == logicalTableName(table) {
		return table
	}

	return aliasedTable{
		base:  table,
		alias: alias,
	}
}

// AliasColumns rebinds each column onto table while preserving order.
func AliasColumns(cols []SQLColumn, table Table) []SQLColumn {
	result := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		result = append(result, RebindColumn(col, table))
	}

	return result
}

// RebindColumn returns col rebound to table when the column supports rebinding.
func RebindColumn(col SQLColumn, table Table) SQLColumn {
	if isNilValue(col) || isNilValue(table) {
		return col
	}

	if transformed, ok := col.(transformedColumn); ok && transformed.isTransformedExpression() {
		return col
	}

	if rebinder, ok := col.(tableRebinder); ok {
		return rebinder.withTable(table)
	}

	if strings.TrimSpace(col.Name()) == "" {
		return col
	}

	if _, ok := col.(SearchColumn); ok {
		return reboundSearchColumn{
			reboundColumn: reboundColumn{
				source: col,
				table:  table,
			},
		}
	}

	return reboundColumn{
		source: col,
		table:  table,
	}
}

// Table returns the SQL identifier that should be used after aliasing.
func (t aliasedTable) Table() string {
	return t.alias
}

// TSQOwner marks aliasedTable as a valid tsq owner.
func (t aliasedTable) TSQOwner() {}

// SearchColumns returns the base table's search columns rebound to the alias.
func (t aliasedTable) SearchColumns() []SearchColumn {
	result := make([]SearchColumn, 0, len(t.base.SearchColumns()))
	for _, col := range t.base.SearchColumns() {
		rebound, ok := RebindColumn(col, t).(SearchColumn)
		if ok {
			result = append(result, rebound)
		}
	}

	return result
}

// Cols returns the base table's columns rebound to the alias.
func (t aliasedTable) Cols() []SQLColumn {
	return AliasColumns(t.base.Cols(), t)
}

// PrimaryKeys returns a defensive copy of the base table's primary key list.
func (t aliasedTable) PrimaryKeys() []string {
	return append([]string(nil), t.base.PrimaryKeys()...)
}

// AutoIncrement reports whether inserts rely on database-generated IDs.
func (t aliasedTable) AutoIncrement() bool {
	return t.base.AutoIncrement()
}

// VersionColumn returns the optimistic-lock version column name, if any.
func (t aliasedTable) VersionColumn() string {
	return t.base.VersionColumn()
}

// Alias returns the SQL alias applied to the base table.
func (t aliasedTable) Alias() string {
	return t.alias
}

// PhysicalTable returns the underlying table name without the SQL alias.
func (t aliasedTable) PhysicalTable() string {
	return physicalTableName(t.base)
}

// Schema returns the base table schema when the base table exposes one.
func (t aliasedTable) Schema() string {
	if schemaTable, ok := t.base.(schemaTabler); ok {
		return schemaTable.Schema()
	}

	return ""
}

func logicalTableName(table Table) string {
	if isNilValue(table) {
		return ""
	}

	return strings.TrimSpace(table.Table())
}

func physicalTableName(table Table) string {
	if isNilValue(table) {
		return ""
	}

	if physical, ok := table.(physicalTabler); ok {
		name := strings.TrimSpace(physical.PhysicalTable())
		if name != "" {
			return name
		}
	}

	return logicalTableName(table)
}

func tableAliasName(table Table) string {
	if isNilValue(table) {
		return ""
	}

	if aliased, ok := table.(tableAliaser); ok {
		return strings.TrimSpace(aliased.Alias())
	}

	return ""
}

func (c reboundColumn) SQLExpr() string {
	return renderCanonicalSQL(rawQualifiedIdentifierForTable(c.table, c.source.Name()))
}

func (c reboundColumn) OutputName() string {
	return c.source.OutputName()
}

func (c reboundColumn) JSONFieldName() string {
	return c.source.JSONFieldName()
}

func (c reboundColumn) Table() Table {
	return c.table
}

func (c reboundColumn) Name() string {
	return c.source.Name()
}

func (c reboundColumn) QualifiedName() string {
	return c.SQLExpr()
}

func (c reboundColumn) scanPointer() scanPointer {
	return c.source.scanPointer()
}

func (c reboundColumn) referencedTables() map[string]Table {
	return map[string]Table{c.table.Table(): c.table}
}

func (c reboundSearchColumn) searchColumn() {}
