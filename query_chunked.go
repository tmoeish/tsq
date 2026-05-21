package tsq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// ChunkedOptions 分块执行通用配置选项。
type ChunkedOptions struct {
	ChunkSize int // 每块处理的数量，默认 1000
}

// DefaultChunkedOptions 返回默认的分块执行配置。
func DefaultChunkedOptions() *ChunkedOptions {
	return &ChunkedOptions{
		ChunkSize: 1000,
	}
}

// ChunkedInsertOptions 分块插入配置选项。
type ChunkedInsertOptions struct {
	ChunkSize    int  // 每块处理的数量，默认 1000
	IgnoreErrors bool // 是否忽略重复键插入错误并继续处理后续数据
}

// DefaultChunkedInsertOptions 返回默认的分块插入配置。
func DefaultChunkedInsertOptions() *ChunkedInsertOptions {
	return &ChunkedInsertOptions{
		ChunkSize:    DefaultChunkedOptions().ChunkSize,
		IgnoreErrors: false,
	}
}

// ChunkedInsert inserts items in chunks using the provided executor.
//
// Transaction boundaries are intentionally caller-controlled. Passing a plain
// *sql.DB or non-transactional executor allows partial progress across chunks;
// passing a *sql.Tx makes the whole chunked operation participate in that
// transaction. TSQ does not open an implicit outer transaction for this helper.
func ChunkedInsert[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedInsertOptions,
) error {
	return trace(ctx, func(ctx context.Context) error {
		return chunkedInsertFn(ctx, tx, items, options...)
	})
}

func chunkedInsertFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	opts, err := normalizeChunkedInsertOptions(options...)
	if err != nil {
		return err
	}

	for i := 0; i < len(items); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(items))

		batch := items[i:end]
		if err := chunkedInsertChunk(ctx, tx, batch, opts); err != nil {
			return fmt.Errorf("chunked insert failed at index %d"+": %w", i, err)
		}
	}

	return nil
}

func chunkedInsertChunk[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	opts *ChunkedInsertOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	batch := make([]Table, 0, len(items))
	for itemIdx, item := range items {
		if isNilValue(item) {
			return fmt.Errorf("item at index %d is nil", itemIdx)
		}

		batch = append(batch, item)
	}

	if opts.IgnoreErrors {
		for itemIdx, item := range batch {
			if err := insertTables(ctx, tx, item); err != nil {
				if isDuplicateKeyError(err) {
					slog.Debug("Ignored duplicate key error in batch insert", "error", err)
					continue
				}

				return fmt.Errorf("chunked insert failed at item %d"+": %w", itemIdx, err)
			}
		}

		return nil
	}

	if err := insertTables(ctx, tx, batch...); err != nil {
		return fmt.Errorf("%s: %w", "chunked insert batch failed", err)
	}

	return nil
}

// ChunkedUpdate updates items in chunks using the provided executor.
//
// Transaction boundaries are intentionally caller-controlled. Passing a plain
// *sql.DB or non-transactional executor allows partial progress across chunks;
// passing a *sql.Tx makes the whole chunked operation participate in that
// transaction. TSQ does not open an implicit outer transaction for this helper.
func ChunkedUpdate[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedOptions,
) error {
	return trace(ctx, func(ctx context.Context) error {
		return chunkedUpdateFn(ctx, tx, items, options...)
	})
}

func chunkedUpdateFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return err
	}

	for i := 0; i < len(items); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(items))

		batch := items[i:end]
		if err := chunkedUpdateChunk(ctx, tx, batch); err != nil {
			return fmt.Errorf("chunked update failed at index %d"+": %w", i, err)
		}
	}

	return nil
}

func chunkedUpdateChunk[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
) error {
	batch := make([]Table, 0, len(items))
	for itemIdx, item := range items {
		if isNilValue(item) {
			return fmt.Errorf("item at index %d is nil", itemIdx)
		}

		batch = append(batch, item)
	}

	if len(batch) == 0 {
		return nil
	}

	if _, err := updateTables(ctx, tx, batch...); err != nil {
		return fmt.Errorf("%s: %w", "chunked update batch failed", err)
	}

	return nil
}

// ChunkedDelete deletes items in chunks using the provided executor.
//
// Transaction boundaries are intentionally caller-controlled. Passing a plain
// *sql.DB or non-transactional executor allows partial progress across chunks;
// passing a *sql.Tx makes the whole chunked operation participate in that
// transaction. TSQ does not open an implicit outer transaction for this helper.
func ChunkedDelete[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedOptions,
) error {
	return trace(ctx, func(ctx context.Context) error {
		return chunkedDeleteFn(ctx, tx, items, options...)
	})
}

func chunkedDeleteFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
	options ...*ChunkedOptions,
) error {
	if len(items) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return err
	}

	for i := 0; i < len(items); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(items))

		batch := items[i:end]
		if err := chunkedDeleteChunk(ctx, tx, batch); err != nil {
			return fmt.Errorf("chunked delete failed at index %d"+": %w", i, err)
		}
	}

	return nil
}

func chunkedDeleteChunk[T Table](
	ctx context.Context,
	tx SQLExecutor,
	items []T,
) error {
	batch := make([]Table, 0, len(items))
	for itemIdx, item := range items {
		if isNilValue(item) {
			return fmt.Errorf("item at index %d is nil", itemIdx)
		}

		batch = append(batch, item)
	}

	if len(batch) == 0 {
		return nil
	}

	if _, err := deleteTables(ctx, tx, batch...); err != nil {
		return fmt.Errorf("%s: %w", "chunked delete batch failed", err)
	}

	return nil
}

// ChunkedDeleteByIDs deletes rows by primary-key values in chunks.
//
// Transaction boundaries are intentionally caller-controlled. Passing a plain
// *sql.DB or non-transactional executor allows partial progress across chunks;
// passing a *sql.Tx makes the whole chunked operation participate in that
// transaction. TSQ does not open an implicit outer transaction for this helper.
func ChunkedDeleteByIDs(
	ctx context.Context,
	tx SQLExecutor,
	tableName string,
	idColumn string,
	ids []any,
	options ...*ChunkedOptions,
) error {
	return trace(ctx, func(ctx context.Context) error {
		return chunkedDeleteByIDsFn(ctx, tx, tableName, idColumn, ids, options...)
	})
}

func chunkedDeleteByIDsFn(
	ctx context.Context,
	tx SQLExecutor,
	tableName string,
	idColumn string,
	ids []any,
	options ...*ChunkedOptions,
) error {
	if len(ids) == 0 {
		return nil
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	if err := validateIDValues(ids); err != nil {
		return err
	}

	opts, err := normalizeChunkedOptions(options...)
	if err != nil {
		return err
	}

	for i := 0; i < len(ids); i += opts.ChunkSize {
		end := min(i+opts.ChunkSize, len(ids))

		batch := ids[i:end]
		if err := chunkedDeleteByIDsChunk(ctx, tx, tableName, idColumn, batch); err != nil {
			return fmt.Errorf("chunked delete by IDs failed at index %d"+": %w", i, err)
		}
	}

	return nil
}

func chunkedDeleteByIDsChunk(
	ctx context.Context,
	tx SQLExecutor,
	tableName string,
	idColumn string,
	ids []any,
) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	sqlStr, err := buildDeleteByIDsSQL(tableName, idColumn, len(placeholders))
	if err != nil {
		return err
	}

	sqlText := renderSQLForExecutor(tx, sqlStr)

	if err := validateOperationalExecutorForSQL(tx, sqlStr); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, sqlText, ids...)
	if err != nil {
		return fmt.Errorf("chunked delete by IDs failed: %s"+": %w", sqlText, err)
	}

	return nil
}

func buildDeleteByIDsSQL(tableName, idColumn string, placeholderCount int) (string, error) {
	if placeholderCount <= 0 {
		return "", errors.New("placeholder count must be greater than 0")
	}

	quotedTable, err := quoteBuiltInIdentifier(tableName)
	if err != nil {
		return "", err
	}

	quotedColumn, err := quoteBuiltInIdentifier(idColumn)
	if err != nil {
		return "", err
	}

	placeholders := make([]string, placeholderCount)
	for i := range placeholders {
		placeholders[i] = "?"
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		quotedTable,
		quotedColumn,
		strings.Join(placeholders, ","),
	), nil
}

// Insert inserts item using the table metadata on T.
func Insert[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	return trace(ctx, func(ctx context.Context) error {
		return insertFn(ctx, tx, item)
	})
}

func insertFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	if err := validateMutationItem(item); err != nil {
		return err
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	return insertTables(ctx, tx, item)
}

// Update updates item using the table metadata on T.
func Update[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	return trace(ctx, func(ctx context.Context) error {
		return updateFn(ctx, tx, item)
	})
}

func updateFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	if err := validateMutationItem(item); err != nil {
		return err
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	_, err := updateTables(ctx, tx, item)

	return err
}

// Delete deletes item using the table metadata on T.
func Delete[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	return trace(ctx, func(ctx context.Context) error {
		return deleteFn(ctx, tx, item)
	})
}

func deleteFn[T Table](
	ctx context.Context,
	tx SQLExecutor,
	item T,
) error {
	if err := validateMutationItem(item); err != nil {
		return err
	}

	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	_, err := deleteTables(ctx, tx, item)

	return err
}
