package tsq

import (
	"fmt"
	"sort"
)

type registeredTable struct {
	Table
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
