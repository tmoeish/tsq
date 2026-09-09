package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// markDeleted stamps the soft-delete tombstone on item and refreshes its
// updated_at column, leaving the row ready for the ordinary update path. The
// update path is reused on purpose: a soft delete is an update, so it keeps the
// optimistic-lock check and the version increment that a physical delete has.
func markDeleted(item Table, at time.Time) error {
	record, err := mutationMetadata(item)
	if err != nil {
		return err
	}

	deletedAt := mutationFieldByColumn(record.fields, record.managed.DeletedAt)
	if !deletedAt.value.IsValid() {
		return fmt.Errorf(
			"table %s declares soft-delete column %s but the struct has no field for it",
			record.tableName, record.managed.DeletedAt,
		)
	}

	if err := applyTombstone(deletedAt.value, at); err != nil {
		return fmt.Errorf("table %s: %w", record.tableName, err)
	}

	if record.managed.UpdatedAt == "" {
		return nil
	}

	updatedAt := mutationFieldByColumn(record.fields, record.managed.UpdatedAt)
	if !updatedAt.value.IsValid() {
		return nil
	}

	return applyTimestamp(updatedAt.value, at)
}

// applyTombstone marks field as deleted at ts.
//
// TSQ cannot name the concrete type: deleted_at is the caller's field and four
// shapes are documented for it. Integer columns carry a Unix-nano tombstone,
// time columns carry the instant, and the nullable wrappers are driven through
// sql.Scanner so that neither sql.NullTime nor null.Time has to be imported
// here to be supported.
func applyTombstone(field reflect.Value, ts time.Time) error {
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !field.CanSet() {
			return errSoftDeleteFieldReadOnly
		}

		field.SetInt(ts.UnixNano())

		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if !field.CanSet() {
			return errSoftDeleteFieldReadOnly
		}

		field.SetUint(uint64(ts.UnixNano()))

		return nil
	}

	return applyTimestamp(field, ts)
}

// applyTimestamp writes ts into a time-shaped field.
func applyTimestamp(field reflect.Value, ts time.Time) error {
	if !field.CanSet() {
		return errSoftDeleteFieldReadOnly
	}

	switch field.Type() {
	case reflect.TypeFor[time.Time]():
		field.Set(reflect.ValueOf(ts))

		return nil
	case reflect.TypeFor[*time.Time]():
		field.Set(reflect.ValueOf(&ts))

		return nil
	}

	// sql.NullTime and null.Time both take a time.Time through Scan and set
	// their valid flag, so one path covers every nullable wrapper.
	if scanner, ok := reflect.TypeAssert[sql.Scanner](field.Addr()); ok {
		if err := scanner.Scan(ts); err != nil {
			return fmt.Errorf("set managed time column: %w", err)
		}

		return nil
	}

	return fmt.Errorf("unsupported managed time column type %s", field.Type())
}

// softDeleteItems stamps every item and routes them through the update path.
func softDeleteItems[T Table](ctx context.Context, tx SQLExecutor, items []T) error {
	at := time.Now()

	for _, item := range items {
		if err := markDeleted(item, at); err != nil {
			return err
		}
	}

	_, err := updateTables(ctx, tx, tablesOf(items)...)

	return err
}

// tablesOf widens a typed slice to the Table interface.
func tablesOf[T Table](items []T) []Table {
	widened := make([]Table, 0, len(items))
	for _, item := range items {
		widened = append(widened, item)
	}

	return widened
}

// softDeleteAssignments returns the SET columns and bound values a soft delete
// writes for T, plus the optimistic-lock column to increment.
//
// The caller may hold no row at all (deleting by primary key), so the field
// types come from a zero value of T rather than from an instance.
func softDeleteAssignments[T Table](at time.Time) (columns []string, values []any, version string, err error) {
	holder, ok := reflect.TypeAssert[Table](reflect.New(reflect.TypeFor[T]()))
	if !ok {
		// Generated tables declare their methods on the value receiver, so the
		// pointer always carries them too.
		return nil, nil, "", fmt.Errorf("%T does not implement tsq.Table through a pointer", holder)
	}

	if err := markDeleted(holder, at); err != nil {
		return nil, nil, "", err
	}

	record, err := mutationMetadata(holder)
	if err != nil {
		return nil, nil, "", err
	}

	for _, column := range []string{record.managed.DeletedAt, record.managed.UpdatedAt} {
		if column == "" {
			continue
		}

		field := mutationFieldByColumn(record.fields, column)
		if !field.value.IsValid() {
			continue
		}

		columns = append(columns, column)
		values = append(values, field.value.Interface())
	}

	return columns, values, record.managed.Version, nil
}

// buildSoftDeleteByPKsSQL renders an UPDATE that tombstones every row whose
// primary key is in the chunk.
func buildSoftDeleteByPKsSQL(
	tableName string,
	setColumns []string,
	versionColumn string,
	pkColumn string,
	placeholderCount int,
) (string, error) {
	if placeholderCount <= 0 {
		return "", errors.New("placeholder count must be greater than 0")
	}

	if len(setColumns) == 0 {
		return "", errors.New("soft delete requires at least one managed column")
	}

	quotedTable, err := quoteBuiltInIdentifier(tableName)
	if err != nil {
		return "", err
	}

	assignments := make([]string, 0, len(setColumns)+1)

	for _, column := range setColumns {
		quoted, err := quoteBuiltInIdentifier(column)
		if err != nil {
			return "", err
		}

		assignments = append(assignments, quoted+" = ?")
	}

	if versionColumn != "" {
		quotedVersion, err := quoteBuiltInIdentifier(versionColumn)
		if err != nil {
			return "", err
		}

		assignments = append(assignments, quotedVersion+" = "+quotedVersion+" + 1")
	}

	quotedPK, err := quoteBuiltInIdentifier(pkColumn)
	if err != nil {
		return "", err
	}

	placeholders := make([]string, placeholderCount)
	for i := range placeholders {
		placeholders[i] = "?"
	}

	return fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s IN (%s)",
		quotedTable,
		strings.Join(assignments, ", "),
		quotedPK,
		strings.Join(placeholders, ","),
	), nil
}

// softDeleteByPKsChunk tombstones one chunk of primary-key values.
func softDeleteByPKsChunk(
	ctx context.Context,
	tx SQLExecutor,
	tableName string,
	setColumns []string,
	setValues []any,
	versionColumn string,
	pkColumn string,
	ids []any,
) error {
	if len(ids) == 0 {
		return nil
	}

	sqlStr, err := buildSoftDeleteByPKsSQL(tableName, setColumns, versionColumn, pkColumn, len(ids))
	if err != nil {
		return err
	}

	if err := validateOperationalExecutorForSQL(tx, sqlStr); err != nil {
		return err
	}

	sqlText := renderSQLForExecutor(tx, sqlStr)

	args := make([]any, 0, len(setValues)+len(ids))
	args = append(args, setValues...)
	args = append(args, ids...)

	if _, err := tx.ExecContext(ctx, sqlText, args...); err != nil {
		return fmt.Errorf("chunked soft delete by primary keys failed: %s: %w", sqlText, err)
	}

	return nil
}

// chunkedSoftDeleteByPKsFn tombstones rows by primary-key value in chunks.
func chunkedSoftDeleteByPKsFn[O Table, T any](
	ctx context.Context,
	tx SQLExecutor,
	pkField TypedColumn[O, T],
	ids []T,
	options ...*ChunkedOptions,
) error {
	if len(ids) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	tableName, pkColumn, err := resolveChunkedDeletePKField(pkField)
	if err != nil {
		return err
	}

	boxedIDs := boxSlice(ids)
	if err := validateIDValues(boxedIDs); err != nil {
		return err
	}

	setColumns, setValues, versionColumn, err := softDeleteAssignments[O](time.Now())
	if err != nil {
		return err
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return err
	}

	// One placeholder per id, plus the fixed SET values shared by every chunk.
	chunkSize := chunkSizeForExecutor(tx, opts.ChunkSize, 1)
	if chunkSize > len(setValues) {
		chunkSize -= len(setValues)
	}

	for i := 0; i < len(boxedIDs); i += chunkSize {
		end := min(i+chunkSize, len(boxedIDs))

		batch := boxedIDs[i:end]
		if err := softDeleteByPKsChunk(
			ctx, tx, tableName, setColumns, setValues, versionColumn, pkColumn, batch,
		); err != nil {
			return fmt.Errorf("chunked soft delete by primary keys failed at index %d: %w", i, err)
		}
	}

	return nil
}
