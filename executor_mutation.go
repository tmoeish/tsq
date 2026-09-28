package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
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

	omittedPrimaryKey := len(insertFields) != len(records[0].fields)

	// Dialects without LastInsertId (PostgreSQL) hand the generated keys back
	// through a RETURNING clause instead; the suffix is empty everywhere else.
	if omittedPrimaryKey {
		if suffix := insertReturningSuffix(exec, records[0]); suffix != "" {
			return insertBatchReturning(ctx, exec, query+suffix, args, records)
		}
	}

	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	if err := assignBatchInsertIDs(ctx, exec, records, result, omittedPrimaryKey); err != nil {
		return err
	}

	return nil
}

func insertReturningSuffix(exec SQLExecutor, record mutationRecord) string {
	dialect := dialectForExecutor(exec)
	if dialect == nil || record.pkField.column == "" {
		return ""
	}

	return dialect.LastInsertIdReturningSuffix(record.tableName, record.pkField.column)
}

// insertBatchReturning runs a multi-row INSERT ... RETURNING <pk> and assigns the
// returned keys to the records in insertion order.
func insertBatchReturning(ctx context.Context, exec SQLExecutor, query string, args []any, records []mutationRecord) error {
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	defer func() { _ = rows.Close() }()

	assigned := 0

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan returned primary key: %w", err)
		}

		if assigned < len(records) {
			assignMutationID(records[assigned].pkField.value, id)
		}

		assigned++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if assigned != len(records) {
		logForExecutor(ctx, exec, slog.LevelWarn, "batch insert returned an unexpected number of primary keys",
			"expected", len(records),
			"actual", assigned,
		)
	}

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

	if err := refuseDuplicateMutationKeys(records); err != nil {
		return 0, err
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

	if rowsAffected != int64(len(records)) {
		return rowsAffected, explainUpdateShortfall(ctx, exec, records, updateFields, rowsAffected)
	}

	if hasOptimisticLock {
		incrementMutationVersions(records)
	}

	return rowsAffected, nil
}

// explainUpdateShortfall handles an update that matched fewer rows than it was
// given. A batch is not a transaction, so the database may hold the update for
// some rows: they are read back, and a row counts as written when it holds the
// next version and the values the statement wrote. Those rows get their new
// version in memory, as the rows of a fully successful update do, and the error
// names the others; the rows used to keep their old version, so retrying them
// could never succeed.
//
// Without a version column a shortfall is not a conflict: MySQL counts only the
// rows an UPDATE changed, so writing a row's current values reports none. Only
// rows that are gone are an error then, which wraps sql.ErrNoRows; it used to be
// reported as success.
func explainUpdateShortfall(ctx context.Context, exec SQLExecutor, records []mutationRecord, updateFields []mutationField, affected int64) error {
	versioned := hasOptimisticMutation(records[0])

	var compared []mutationField
	if versioned {
		compared = append(compared, records[0].versionField)

		for _, field := range updateFields {
			// A database may store a time at a coarser precision than was bound.
			if !isTimeMutationValue(field.value) {
				compared = append(compared, field)
			}
		}
	}

	stored, err := readBackMutationRecords(ctx, exec, records, compared)
	if err != nil {
		if versioned {
			return errors.Join(&ErrOptimisticLockConflict{table: records[0].tableName, expected: len(records), actual: affected},
				fmt.Errorf("read back the rows: %w", err))
		}

		return fmt.Errorf("update %s: read back the rows: %w", records[0].tableName, err)
	}

	var missing []any

	for _, record := range records {
		values, found := stored[mutationValueText(record.pkField.value.Interface())]
		written := found

		for i, field := range compared {
			want := mutationFieldByColumn(record.fields, field.column).value
			if field.column == record.versionField.column {
				want = nextMutationVersion(record.versionField.value)
			}

			if written && values[i] != mutationValueText(want.Interface()) {
				written = false
			}
		}

		if !written {
			missing = append(missing, record.pkField.value.Interface())
			continue
		}

		if versioned {
			incrementMutationVersions([]mutationRecord{record})
		}
	}

	switch {
	case versioned:
		return &ErrOptimisticLockConflict{table: records[0].tableName, expected: len(records), actual: affected, keys: missing}
	case len(missing) > 0:
		return fmt.Errorf("update %s: no row with primary key %v: %w", records[0].tableName, missing, sql.ErrNoRows)
	}

	return nil
}

// readBackMutationRecords returns the columns of the rows among records, keyed and
// rendered by mutationValueText.
func readBackMutationRecords(ctx context.Context, exec SQLExecutor, records []mutationRecord, columns []mutationField) (map[string][]string, error) {
	tableSQL, err := quoteMutationIdentifier(exec, records[0].tableName)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(columns)+1)
	for _, field := range append([]mutationField{records[0].pkField}, columns...) {
		quoted, err := quoteMutationIdentifier(exec, field.column)
		if err != nil {
			return nil, err
		}

		names = append(names, quoted)
	}

	var argIndex int

	placeholders := make([]string, 0, len(records))
	args := make([]any, 0, len(records))

	for _, record := range records {
		placeholders = append(placeholders, nextBindVar(exec, &argIndex))
		args = append(args, record.pkField.value.Interface())
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)",
		strings.Join(names, ", "), tableSQL, names[0], strings.Join(placeholders, ", "))

	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	stored := make(map[string][]string, len(records))

	for rows.Next() {
		key := reflect.New(records[0].pkField.value.Type())
		dest := []any{key.Interface()}

		held := make([]reflect.Value, 0, len(columns))
		for _, field := range columns {
			v := reflect.New(field.value.Type())
			held = append(held, v)
			dest = append(dest, v.Interface())
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		values := make([]string, 0, len(held))
		for _, v := range held {
			values = append(values, mutationValueText(v.Elem().Interface()))
		}

		stored[mutationValueText(key.Elem().Interface())] = values
	}

	return stored, rows.Err()
}

// mutationValueText renders a value the way the driver would bind it, so a value
// read back into the field's own type compares with the one written.
func mutationValueText(v any) string {
	if converted, err := driver.DefaultParameterConverter.ConvertValue(v); err == nil {
		v = converted
	}

	return fmt.Sprintf("%#v", v)
}

// isTimeMutationValue reports a field the driver binds as a time: time.Time,
// *time.Time, sql.NullTime and the other nullable time wrappers.
func isTimeMutationValue(v reflect.Value) bool {
	switch v.Interface().(type) {
	case time.Time, *time.Time:
		return true
	}

	if converted, err := driver.DefaultParameterConverter.ConvertValue(v.Interface()); err == nil {
		_, isTime := converted.(time.Time)
		return isTime
	}

	return false
}

// nextMutationVersion is v plus one, in v's own type.
func nextMutationVersion(v reflect.Value) reflect.Value {
	next := reflect.New(v.Type()).Elem()
	next.Set(v)

	switch next.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		next.SetInt(next.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		next.SetUint(next.Uint() + 1)
	}

	return next
}

// refuseDuplicateMutationKeys refuses two records with one primary key: the
// statement writes a row once, so one of them was dropped without a word (or,
// with a version column, reported as a conflict after the other was written).
func refuseDuplicateMutationKeys(records []mutationRecord) error {
	seen := make(map[string]int, len(records))

	for i, record := range records {
		key := mutationValueText(record.pkField.value.Interface())
		if first, dup := seen[key]; dup {
			return fmt.Errorf("rows %d and %d have the same primary key %v", first, i, record.pkField.value.Interface())
		}

		seen[key] = i
	}

	return nil
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
		// LastInsertId is int64 and a driver reporting a negative id would wrap into a
		// huge unsigned key here. Generated keys are positive, so a negative one means
		// the driver could not report an id; leaving the field at its zero value says
		// "unknown", which is what the caller can actually check for.
		if id < 0 {
			return
		}

		field.SetUint(uint64(id))
	}
}

// assignBatchInsertIDs writes the generated keys of an INSERT into records. A
// driver that cannot report them is an error: the rows would otherwise keep a
// zero key that every later write of them refuses, with nothing saying why.
func assignBatchInsertIDs(ctx context.Context, exec SQLExecutor, records []mutationRecord, result sql.Result, omittedPrimaryKey bool) error {
	if !omittedPrimaryKey || len(records) == 0 {
		return nil
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("read the generated key: %w", err)
	}

	if len(records) == 1 {
		assignMutationID(records[0].pkField.value, lastID)
		return nil
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil || rowsAffected != int64(len(records)) {
		logForExecutor(ctx, exec, slog.LevelWarn, "batch insert ID assignment skipped: rows affected mismatch",
			"expected", len(records),
			"actual", rowsAffected,
			"error", err,
		)

		return nil
	}

	dialect := dialectForExecutor(exec)
	if dialect == nil {
		return nil
	}

	startID, ok := dialect.BatchInsertStartID(lastID, rowsAffected)
	if !ok {
		return nil
	}

	// MySQL spaces the keys of one INSERT by auto_increment_increment, which a
	// multi-primary setup raises above 1; they used to be assumed consecutive.
	step := int64(1)

	if dialect.Name() == tsqdialect.MySQL {
		if err := exec.QueryRowContext(ctx, "SELECT @@auto_increment_increment").Scan(&step); err != nil {
			return fmt.Errorf("read the auto-increment step: %w", err)
		}

		step = max(step, 1)
	}

	for i, record := range records {
		assignMutationID(record.pkField.value, startID+int64(i)*step)
	}

	return nil
}
