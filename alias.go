package tsq

import "strings"

type physicalTabler interface {
	PhysicalTable() string
}

type tableAliaser interface {
	Alias() string
}

type tableRebinder interface {
	withTable(Table) Column
}

type aliasedTable struct {
	base  Table
	alias string
}

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

func AliasColumns(cols []Column, table Table) []Column {
	result := make([]Column, 0, len(cols))
	for _, col := range cols {
		result = append(result, RebindColumn(col, table))
	}

	return result
}

func RebindColumn(col Column, table Table) Column {
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

func (t aliasedTable) KwList() []Column {
	return AliasColumns(t.base.KwList(), t)
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
