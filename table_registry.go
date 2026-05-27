package tsq

import (
	"fmt"
	"sort"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

type registeredTable struct {
	Table
	Columns []tsqdialect.DDLColumnSpec
	Indexes []TableIndex
}

func buildRegisteredTables(registrations []TableRegistration) ([]*registeredTable, error) {
	if len(registrations) == 0 {
		return nil, nil
	}

	tables := make(map[string]*registeredTable, len(registrations))

	for _, registration := range registrations {
		table := registration.Table
		if isNilValue(table) {
			return nil, &RegistrationError{
				Type:    RegistrationErrorNilTable,
				Message: "registered table cannot be nil",
			}
		}

		if err := validateRegisteredIndexes(table, registration.Indexes); err != nil {
			return nil, err
		}

		if err := validateRegisteredColumns(table, registration.Columns); err != nil {
			return nil, err
		}

		key := registeredTableKey(table)
		if _, exists := tables[key]; exists {
			return nil, &RegistrationError{
				Type:      RegistrationErrorDuplicate,
				TableName: key,
				Message:   fmt.Sprintf("table %s is already registered", key),
			}
		}

		tables[key] = &registeredTable{
			Table:   table,
			Columns: cloneDDLColumnSpecs(registration.Columns),
			Indexes: cloneTableIndexes(registration.Indexes),
		}
	}

	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}

	sort.Strings(names)

	result := make([]*registeredTable, 0, len(names))
	for _, name := range names {
		result = append(result, tables[name])
	}

	return result, nil
}

func registeredTableKey(table Table) string {
	if table == nil {
		return ""
	}

	if schemaTable, ok := table.(schemaTabler); ok && schemaTable.Schema() != "" {
		return schemaTable.Schema() + "." + table.Table()
	}

	return table.Table()
}

func validateRegisteredIndexes(table Table, indexes []TableIndex) error {
	if len(indexes) == 0 {
		return nil
	}

	tableName := physicalTableName(table)

	availableColumns := make(map[string]struct{}, len(table.Cols()))
	for _, col := range table.Cols() {
		if col == nil {
			continue
		}

		availableColumns[col.OutputName()] = struct{}{}
	}

	for _, index := range indexes {
		if err := validateBuiltInIdentifier(index.Name); err != nil {
			return &RegistrationError{
				Type:      RegistrationErrorInvalidIndex,
				TableName: tableName,
				Message:   fmt.Sprintf("invalid index %q on table %s: %v", index.Name, tableName, err),
			}
		}

		if len(index.Fields) == 0 {
			return &RegistrationError{
				Type:      RegistrationErrorInvalidIndex,
				TableName: tableName,
				Message:   fmt.Sprintf("index %q on table %s must declare at least one field", index.Name, tableName),
			}
		}

		for _, field := range index.Fields {
			if err := validateBuiltInIdentifier(field); err != nil {
				return &RegistrationError{
					Type:      RegistrationErrorInvalidIndex,
					TableName: tableName,
					Message:   fmt.Sprintf("invalid field %q in index %q on table %s: %v", field, index.Name, tableName, err),
				}
			}

			if _, ok := availableColumns[field]; !ok {
				return &RegistrationError{
					Type:      RegistrationErrorInvalidIndex,
					TableName: tableName,
					Message:   fmt.Sprintf("index %q on table %s references unknown field %q", index.Name, tableName, field),
				}
			}
		}
	}

	return nil
}

func cloneTableIndexes(indexes []TableIndex) []TableIndex {
	if len(indexes) == 0 {
		return nil
	}

	result := make([]TableIndex, 0, len(indexes))
	for _, index := range indexes {
		fields := append([]string(nil), index.Fields...)
		result = append(result, TableIndex{
			Name:   index.Name,
			Fields: fields,
			Unique: index.Unique,
		})
	}

	return result
}

func validateRegisteredColumns(table Table, columns []tsqdialect.DDLColumnSpec) error {
	if len(columns) == 0 {
		return nil
	}

	tableName := physicalTableName(table)

	availableColumns := make(map[string]struct{}, len(table.Cols()))
	for _, col := range table.Cols() {
		if col == nil {
			continue
		}

		availableColumns[col.OutputName()] = struct{}{}
	}

	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if err := validateBuiltInIdentifier(column.Name); err != nil {
			return &RegistrationError{
				Type:      RegistrationErrorInvalidIndex,
				TableName: tableName,
				Message:   fmt.Sprintf("invalid column %q on table %s: %v", column.Name, tableName, err),
			}
		}

		if _, ok := availableColumns[column.Name]; !ok {
			return &RegistrationError{
				Type:      RegistrationErrorInvalidIndex,
				TableName: tableName,
				Message:   fmt.Sprintf("column %q on table %s is not exposed by Cols()", column.Name, tableName),
			}
		}

		if _, ok := seen[column.Name]; ok {
			return &RegistrationError{
				Type:      RegistrationErrorInvalidIndex,
				TableName: tableName,
				Message:   fmt.Sprintf("column %q on table %s is declared more than once", column.Name, tableName),
			}
		}
		seen[column.Name] = struct{}{}
	}

	return nil
}

func cloneDDLColumnSpecs(columns []tsqdialect.DDLColumnSpec) []tsqdialect.DDLColumnSpec {
	if len(columns) == 0 {
		return nil
	}

	result := make([]tsqdialect.DDLColumnSpec, 0, len(columns))

	return append(result, columns...)
}
