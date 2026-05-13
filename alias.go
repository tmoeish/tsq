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

// AliasColumns rebinds cols onto table.
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

	if rebinder, ok := col.(tableRebinder); ok {
		return rebinder.withTable(table)
	}

	return col
}

func (t aliasedTable) Table() string {
	return t.alias
}

func (t aliasedTable) TSQOwner() {}

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

func (t aliasedTable) Cols() []SQLColumn {
	return AliasColumns(t.base.Cols(), t)
}

func (t aliasedTable) PrimaryKeys() []string {
	return append([]string(nil), t.base.PrimaryKeys()...)
}

func (t aliasedTable) AutoIncrement() bool {
	return t.base.AutoIncrement()
}

func (t aliasedTable) VersionColumn() string {
	return t.base.VersionColumn()
}

func (t aliasedTable) Alias() string {
	return t.alias
}

func (t aliasedTable) PhysicalTable() string {
	return physicalTableName(t.base)
}

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
