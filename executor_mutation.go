package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
)

type mutationField struct {
	column string
	value  reflect.Value
}

type mutationRecord struct {
	tableName    string
	fields       []mutationField
	pkField      mutationField
	versionField mutationField
	autoIncr     bool
}

func insertTables(ctx context.Context, exec SQLExecutor, dst ...Table) error {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return err
	}

	for _, group := range groupInsertRecords(records) {
		if err := insertBatch(ctx, exec, group); err != nil {
			return err
		}
	}

	return nil
}

func updateTables(ctx context.Context, exec SQLExecutor, dst ...Table) (int64, error) {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return 0, err
	}

	var total int64

	for _, group := range groupUpdateRecords(records) {
		affected, err := updateBatch(ctx, exec, group)
		if err != nil {
			return total, err
		}

		total += affected
	}

	return total, nil
}

func deleteTables(ctx context.Context, exec SQLExecutor, dst ...Table) (int64, error) {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return 0, err
	}

	var total int64

	for _, group := range groupDeleteRecords(records) {
		affected, err := deleteBatch(ctx, exec, group)
		if err != nil {
			return total, err
		}

		total += affected
	}

	return total, nil
}

func collectMutationRecords(dst []Table) ([]mutationRecord, error) {
	records := make([]mutationRecord, 0, len(dst))

	for _, item := range dst {
		record, err := mutationMetadata(item)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

func groupInsertRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		fields := insertFieldsForRecord(record)
		return record.tableName + "|" + record.pkField.column + "|" + strings.Join(mutationFieldColumns(fields), ",")
	})
}

func groupUpdateRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		return record.tableName + "|" + record.pkField.column + "|" +
			strings.Join(mutationFieldColumns(updateFieldsForRecord(record)), ",")
	})
}

func groupDeleteRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		return record.tableName + "|" + record.pkField.column
	})
}

func groupMutationRecords(records []mutationRecord, keyFn func(mutationRecord) string) [][]mutationRecord {
	if len(records) == 0 {
		return nil
	}

	groups := make([][]mutationRecord, 0)
	indexByKey := make(map[string]int)

	for _, record := range records {
		key := keyFn(record)
		if idx, ok := indexByKey[key]; ok {
			groups[idx] = append(groups[idx], record)
			continue
		}

		indexByKey[key] = len(groups)
		groups = append(groups, []mutationRecord{record})
	}

	return groups
}

func insertBatch(ctx context.Context, exec SQLExecutor, records []mutationRecord) error {
	if len(records) == 0 {
		return nil
	}

	insertFields := insertFieldsForRecord(records[0])
	if len(insertFields) == 0 {
		return errInsertRequiresColumn
	}

	for _, record := range records[1:] {
		if !mutationFieldColumnsEqual(insertFields, insertFieldsForRecord(record)) {
			return errInsertLayoutMismatch
		}
	}

	tableSQL, err := quoteMutationIdentifier(exec, records[0].tableName)
	if err != nil {
		return err
	}

	quotedCols := make([]string, 0, len(insertFields))

	for _, field := range insertFields {
		col, err := quoteMutationIdentifier(exec, field.column)
		if err != nil {
			return err
		}

		quotedCols = append(quotedCols, col)
	}

	var (
		argIndex     int
		args         = make([]any, 0, len(insertFields)*len(records))
		valueClauses = make([]string, 0, len(records))
	)

	for _, record := range records {
		recordFields := insertFieldsForRecord(record)
		placeholders := make([]string, 0, len(recordFields))

		for _, field := range recordFields {
			placeholders = append(placeholders, nextBindVar(exec, &argIndex))
			args = append(args, field.value.Interface())
		}

		valueClauses = append(valueClauses, "("+strings.Join(placeholders, ", ")+")")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableSQL,
		strings.Join(quotedCols, ", "),
		strings.Join(valueClauses, ", "),
	)

	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	assignBatchInsertIDs(exec, records, result, len(insertFields) != len(records[0].fields))

	return nil
}

func updateBatch(ctx context.Context, exec SQLExecutor, records []mutationRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	updateFields := updateFieldsForRecord(records[0])
	if len(updateFields) == 0 {
		return 0, errUpdateRequiresMutableColumn
	}

	for _, record := range records {
		if isZeroMutationValue(record.pkField.value) {
			return 0, errUpdateRequiresPrimaryKey
		}

		if !mutationFieldColumnsEqual(updateFields, updateFieldsForRecord(record)) {
			return 0, errUpdateLayoutMismatch
		}
	}

	tableSQL, err := quoteMutationIdentifier(exec, records[0].tableName)
	if err != nil {
		return 0, err
	}

	pkSQL, err := quoteMutationIdentifier(exec, records[0].pkField.column)
	if err != nil {
		return 0, err
	}

	hasOptimisticLock := hasOptimisticMutation(records[0])

	versionSQL := ""
	if hasOptimisticLock {
		versionSQL, err = quoteMutationIdentifier(exec, records[0].versionField.column)
		if err != nil {
			return 0, err
		}
	}

	var (
		argIndex   int
		args       []any
		setClauses = make([]string, 0, len(updateFields))
	)

	for _, field := range updateFields {
		colSQL, err := quoteMutationIdentifier(exec, field.column)
		if err != nil {
			return 0, err
		}

		var clause strings.Builder
		clause.WriteString(colSQL)
		clause.WriteString(" = CASE ")
		clause.WriteString(pkSQL)

		for _, record := range records {
			recordField := mutationFieldByColumn(record.fields, field.column)

			clause.WriteString(" WHEN ")
			clause.WriteString(nextBindVar(exec, &argIndex))
			clause.WriteString(" THEN ")
			clause.WriteString(nextBindVar(exec, &argIndex))

			args = append(args, record.pkField.value.Interface(), recordField.value.Interface())
		}

		clause.WriteString(" ELSE ")
		clause.WriteString(colSQL)
		clause.WriteString(" END")
		setClauses = append(setClauses, clause.String())
	}

	if hasOptimisticLock {
		setClauses = append(setClauses, versionSQL+" = "+versionSQL+" + 1")
	}

	whereSQL, whereArgs, err := buildMutationWhereClause(exec, records, &argIndex)
	if err != nil {
		return 0, err
	}

	args = append(args, whereArgs...)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		tableSQL,
		strings.Join(setClauses, ", "),
		whereSQL,
	)

	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if hasOptimisticLock && rowsAffected != int64(len(records)) {
		return rowsAffected, &ErrOptimisticLockConflict{
			table:    records[0].tableName,
			expected: len(records),
			actual:   rowsAffected,
		}
	}

	if hasOptimisticLock {
		incrementMutationVersions(records)
	}

	return rowsAffected, nil
}

func deleteBatch(ctx context.Context, exec SQLExecutor, records []mutationRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	for _, record := range records {
		if isZeroMutationValue(record.pkField.value) {
			return 0, errDeleteRequiresPrimaryKey
		}
	}

	tableSQL, err := quoteMutationIdentifier(exec, records[0].tableName)
	if err != nil {
		return 0, err
	}

	var argIndex int

	whereSQL, args, err := buildMutationWhereClause(exec, records, &argIndex)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		tableSQL,
		whereSQL,
	)

	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if hasOptimisticMutation(records[0]) && rowsAffected != int64(len(records)) {
		return rowsAffected, &ErrOptimisticLockConflict{
			table:    records[0].tableName,
			expected: len(records),
			actual:   rowsAffected,
		}
	}

	return rowsAffected, nil
}

func quoteMutationIdentifier(exec SQLExecutor, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("identifier cannot be empty")
	}

	if !builtInIdentifierPattern.MatchString(name) {
		return "", fmt.Errorf("invalid identifier: %s", name)
	}

	if dialect := dialectForExecutor(exec); dialect != nil {
		return dialect.QuoteField(name), nil
	}

	return name, nil
}

func bindVar(exec SQLExecutor, index int) string {
	if dialect := dialectForExecutor(exec); dialect != nil {
		return dialect.BindVar(index)
	}

	return "?"
}

func isZeroMutationValue(value reflect.Value) bool {
	return value.IsZero()
}

func nextBindVar(exec SQLExecutor, index *int) string {
	placeholder := bindVar(exec, *index)
	*index++

	return placeholder
}

func assignMutationID(field reflect.Value, id int64) {
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		field.SetInt(id)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		field.SetUint(uint64(id))
	}
}

func assignBatchInsertIDs(exec SQLExecutor, records []mutationRecord, result sql.Result, omittedPrimaryKey bool) {
	if !omittedPrimaryKey || len(records) == 0 {
		return
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return
	}

	if len(records) == 1 {
		assignMutationID(records[0].pkField.value, lastID)
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil || rowsAffected != int64(len(records)) {
		slog.Warn("batch insert ID assignment skipped: rows affected mismatch",
			"expected", len(records),
			"actual", rowsAffected,
			"error", err,
		)

		return
	}

	dialect := dialectForExecutor(exec)
	if dialect == nil {
		return
	}

	startID, ok := dialect.BatchInsertStartID(lastID, rowsAffected)
	if !ok {
		return
	}

	for i, record := range records {
		assignMutationID(record.pkField.value, startID+int64(i))
	}
}
