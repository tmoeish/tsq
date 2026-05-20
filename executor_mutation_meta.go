package tsq

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func mutationMetadata(dst Table) (mutationRecord, error) {
	if isNilValue(dst) {
		return mutationRecord{}, errMutationItemNil
	}

	value := reflect.ValueOf(dst)
	if value.Kind() != reflect.Ptr || value.IsNil() {
		return mutationRecord{}, errMutationItemPointer
	}

	if value.Elem().Kind() != reflect.Struct {
		return mutationRecord{}, errMutationItemStructPointer
	}

	fields, err := collectMutationFields(dst)
	if err != nil {
		return mutationRecord{}, err
	}

	if len(fields) == 0 {
		return mutationRecord{}, errMutationItemNoTaggedFields
	}

	pkField, err := primaryMutationField(dst.PrimaryKeys(), fields)
	if err != nil {
		return mutationRecord{}, err
	}

	versionField, err := optimisticLockMutationField(dst.VersionColumn(), fields)
	if err != nil {
		return mutationRecord{}, err
	}

	return mutationRecord{
		tableName:    dst.Table(),
		fields:       fields,
		pkField:      pkField,
		versionField: versionField,
		autoIncr:     dst.AutoIncrement(),
	}, nil
}

func collectMutationFields(dst Table) ([]mutationField, error) {
	fields := make([]mutationField, 0, len(dst.Cols()))
	for _, col := range dst.Cols() {
		if isNilValue(col) {
			continue
		}

		ptr, err := mutationFieldPointer(col, dst)
		if err != nil {
			return nil, err
		}

		fields = append(fields, mutationField{
			column: col.Name(),
			value:  reflect.ValueOf(ptr).Elem(),
		})
	}

	return fields, nil
}

func insertFieldsForRecord(record mutationRecord) []mutationField {
	fields := make([]mutationField, 0, len(record.fields))
	for _, field := range record.fields {
		if field.column == record.pkField.column && record.autoIncr && isZeroMutationValue(field.value) {
			continue
		}

		fields = append(fields, field)
	}

	return fields
}

func updateFieldsForRecord(record mutationRecord) []mutationField {
	fields := make([]mutationField, 0, len(record.fields)-1)
	for _, field := range record.fields {
		if field.column == record.pkField.column || field.column == record.versionField.column {
			continue
		}

		fields = append(fields, field)
	}

	return fields
}

func optimisticLockMutationField(column string, fields []mutationField) (mutationField, error) {
	column = strings.TrimSpace(column)
	if column == "" {
		return mutationField{}, nil
	}

	for _, field := range fields {
		if field.column == column {
			return field, nil
		}
	}

	return mutationField{}, fmt.Errorf("mutation item is missing optimistic lock column %s", column)
}

func mutationFieldColumns(fields []mutationField) []string {
	cols := make([]string, 0, len(fields))
	for _, field := range fields {
		cols = append(cols, field.column)
	}

	return cols
}

func mutationFieldColumnsEqual(expected, actual []mutationField) bool {
	if len(expected) != len(actual) {
		return false
	}

	for i := range expected {
		if expected[i].column != actual[i].column {
			return false
		}
	}

	return true
}

func mutationFieldByColumn(fields []mutationField, column string) mutationField {
	for _, field := range fields {
		if field.column == column {
			return field
		}
	}

	return mutationField{}
}

func hasOptimisticMutation(record mutationRecord) bool {
	return record.versionField.column != ""
}

func buildMutationWhereClause(exec SQLExecutor, records []mutationRecord, argIndex *int) (string, []any, error) {
	pkSQL, err := quoteMutationIdentifier(exec, records[0].pkField.column)
	if err != nil {
		return "", nil, err
	}

	if !hasOptimisticMutation(records[0]) {
		placeholders := make([]string, 0, len(records))

		args := make([]any, 0, len(records))
		for _, record := range records {
			placeholders = append(placeholders, nextBindVar(exec, argIndex))
			args = append(args, record.pkField.value.Interface())
		}

		return pkSQL + " IN (" + strings.Join(placeholders, ", ") + ")", args, nil
	}

	versionSQL, err := quoteMutationIdentifier(exec, records[0].versionField.column)
	if err != nil {
		return "", nil, err
	}

	clauses := make([]string, 0, len(records))

	args := make([]any, 0, len(records)*2)
	for _, record := range records {
		clauses = append(
			clauses,
			"("+pkSQL+" = "+nextBindVar(exec, argIndex)+" AND "+versionSQL+" = "+nextBindVar(exec, argIndex)+")",
		)
		args = append(args, record.pkField.value.Interface(), record.versionField.value.Interface())
	}

	if len(clauses) == 1 {
		return clauses[0], args, nil
	}

	return "(" + strings.Join(clauses, " OR ") + ")", args, nil
}

func incrementMutationVersions(records []mutationRecord) {
	for _, record := range records {
		if !hasOptimisticMutation(record) || !record.versionField.value.IsValid() {
			continue
		}

		switch record.versionField.value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			record.versionField.value.SetInt(record.versionField.value.Int() + 1)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			record.versionField.value.SetUint(record.versionField.value.Uint() + 1)
		}
	}
}

func primaryMutationField(pkColumns []string, fields []mutationField) (mutationField, error) {
	if len(pkColumns) != 1 {
		return mutationField{}, errors.New("mutation item must define exactly one primary key column")
	}

	for _, field := range fields {
		if field.column == pkColumns[0] {
			return field, nil
		}
	}

	return mutationField{}, fmt.Errorf("mutation item is missing primary key column %s", pkColumns[0])
}

func mutationFieldPointer(col SQLColumn, holder Table) (any, error) {
	pointerFunc := col.scanPointer()
	if pointerFunc == nil {
		return nil, fmt.Errorf("column %s field pointer is nil", col.Name())
	}

	ptr := pointerFunc(holder)
	if ptr == nil {
		return nil, fmt.Errorf("column %s field pointer returned nil", col.Name())
	}

	value := reflect.ValueOf(ptr)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, fmt.Errorf("column %s field pointer must return a non-nil pointer", col.Name())
	}

	return ptr, nil
}
