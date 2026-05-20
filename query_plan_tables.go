package tsq

import "maps"

func (spec querySpec[O]) selectTables() map[string]Table {
	cols := make([]SQLColumn, 0, len(spec.Selects))
	for _, col := range spec.Selects {
		cols = append(cols, col)
	}

	return spec.tablesForColumns(cols)
}

func (spec querySpec[O]) fromTables() map[string]Table {
	if isNilValue(spec.From) {
		return map[string]Table{}
	}

	return map[string]Table{spec.From.Table(): spec.From}
}

func (spec querySpec[O]) conditionTables() map[string]Table {
	return spec.tablesForConditions(spec.Filters)
}

func (spec querySpec[O]) joinTables() map[string]Table {
	tables := make(map[string]Table, len(spec.Joins)*2)

	for _, item := range spec.Joins {
		if !isNilValue(item.table) {
			tables[item.table.Table()] = item.table
		}

		maps.Copy(tables, spec.tablesForConditions(item.on))
	}

	return tables
}

func (spec querySpec[O]) keywordTables() map[string]Table {
	cols := make([]SQLColumn, 0, len(spec.KeywordSearch))
	for _, col := range spec.KeywordSearch {
		cols = append(cols, col)
	}

	return spec.tablesForColumns(cols)
}

func (spec querySpec[O]) listQueryTables() map[string]Table {
	tables := spec.fromTables()
	maps.Copy(tables, spec.selectTables())
	maps.Copy(tables, spec.conditionTables())
	maps.Copy(tables, spec.joinTables())
	maps.Copy(tables, spec.tablesForColumns(spec.GroupBy))
	maps.Copy(tables, spec.tablesForConditions(spec.Having))

	return tables
}

func (spec querySpec[O]) pageQueryTables() map[string]Table {
	tables := spec.listQueryTables()
	maps.Copy(tables, spec.keywordTables())

	return tables
}

func (spec querySpec[O]) tablesForColumns(cols []SQLColumn) map[string]Table {
	tables := make(map[string]Table, len(cols))

	for _, col := range cols {
		table, err := validateColumnInput(col)
		if err != nil {
			continue
		}

		tables[table.Table()] = table
		if refs, ok := col.(interface{ referencedTables() map[string]Table }); ok {
			maps.Copy(tables, refs.referencedTables())
		}
	}

	return tables
}

func (spec querySpec[O]) tablesForConditions(conds []Condition) map[string]Table {
	tables := make(map[string]Table)

	for _, cond := range conds {
		_, condTables, _, err := validateConditionInput(cond)
		if err != nil {
			continue
		}

		maps.Copy(tables, condTables)
	}

	return tables
}
