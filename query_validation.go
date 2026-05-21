package tsq

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgconn"
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return mysqlErr.Number == 1062
	}

	if sqliteErr, ok := errors.AsType[sqlite3.Error](err); ok {
		return errors.Is(sqliteErr.ExtendedCode, sqlite3.ErrConstraintUnique) ||
			errors.Is(sqliteErr.ExtendedCode, sqlite3.ErrConstraintPrimaryKey)
	}

	if pqErr, ok := errors.AsType[*pq.Error](err); ok {
		return string(pqErr.Code) == "23505"
	}

	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "23505"
	}

	return false
}

func normalizePageReq(page *PageRequest) *PageRequest {
	if page == nil {
		page = &PageRequest{}
	}

	normalized := *page
	if normalized.Page <= 0 {
		normalized.Page = 1
	}

	if normalized.Size <= 0 {
		normalized.Size = defaultPageSize
	}

	if normalized.Size > maxPageSize {
		normalized.Size = maxPageSize
	}

	return &normalized
}

func normalizeChunkedInsertOptions(options ...*ChunkedInsertOptions) (*ChunkedInsertOptions, error) {
	if len(options) > 1 {
		return nil, errors.New("expected at most one chunked insert options value")
	}

	opts := DefaultChunkedInsertOptions()

	if len(options) > 0 && options[0] != nil {
		opts = new(*options[0])
	}

	if err := validateChunkSize(opts.ChunkSize); err != nil {
		return nil, err
	}

	return opts, nil
}

func normalizeChunkedOptions(options ...*ChunkedOptions) (*ChunkedOptions, error) {
	if len(options) > 1 {
		return nil, errors.New("expected at most one chunked options value")
	}

	opts := DefaultChunkedOptions()

	if len(options) > 0 && options[0] != nil {
		opts = new(*options[0])
	}

	if err := validateChunkSize(opts.ChunkSize); err != nil {
		return nil, err
	}

	return opts, nil
}

func validateChunkSize(chunkSize int) error {
	if chunkSize <= 0 {
		return fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	return nil
}

func validateIDValues(ids []any) error {
	for i, id := range ids {
		if isNilValue(id) {
			return fmt.Errorf("id at index %d cannot be nil", i)
		}
	}

	return nil
}

func quoteBuiltInIdentifier(name string) (string, error) {
	if !builtInIdentifierPattern.MatchString(name) {
		return "", fmt.Errorf("invalid SQL identifier: %s", name)
	}

	if len(name) > 50 {
		slog.Warn("identifier is unusually long", "identifier", name, "length", len(name))
	}

	return rawIdentifier(name), nil
}

func validateQuery[O Owner](q *Query[O]) error {
	if q == nil {
		return errors.New("query cannot be nil")
	}

	if strings.TrimSpace(q.listSQL) == "" || strings.TrimSpace(q.cntSQL) == "" {
		return errors.New("query is not built")
	}

	if len(q.kwCols) > 0 &&
		(strings.TrimSpace(q.kwListSQL) == "" || strings.TrimSpace(q.kwCntSQL) == "") {
		return errors.New("keyword query is not built")
	}

	return nil
}

func validateExecutor(tx SQLExecutor) error {
	if tx == nil {
		return errSQLExecutorNil
	}

	value := reflect.ValueOf(tx)
	if value.IsValid() && value.Kind() == reflect.Pointer && value.IsNil() {
		return errSQLExecutorNil
	}

	if runtime, ok := tx.(*Runtime); ok {
		return validateTxRuntime(runtime)
	}

	return nil
}

func validateOperationalExecutor(tx SQLExecutor) error {
	return validateExecutor(tx)
}

func validateExecutorForSQL(tx SQLExecutor, rawSQLs ...string) error {
	if err := validateExecutor(tx); err != nil {
		return err
	}

	dialect := dialectForExecutor(tx)
	if dialect != nil {
		for _, rawSQL := range rawSQLs {
			for _, capability := range detectSQLCapabilities(rawSQL) {
				if err := tsqdialect.ValidateCapability(dialect, capability); err != nil {
					return err
				}
			}
		}

		return nil
	}

	for _, rawSQL := range rawSQLs {
		if containsIdentifierMarkersNeedingRender(rawSQL) || containsBindVarsNeedingDialect(rawSQL) {
			return errors.New("sql executor dialect cannot be determined")
		}
	}

	return nil
}

func detectSQLCapabilities(rawSQL string) []tsqdialect.Capability {
	upperSQL := strings.ToUpper(strings.TrimSpace(rawSQL))
	capabilities := make([]tsqdialect.Capability, 0, 8)

	if strings.HasPrefix(upperSQL, "WITH ") {
		capabilities = append(capabilities, tsqdialect.CapabilityCTE)
	}

	if strings.Contains(upperSQL, " FULL JOIN ") {
		capabilities = append(capabilities, tsqdialect.CapabilityFullOuterJoin)
	}

	if strings.Contains(upperSQL, " INTERSECT ") {
		capabilities = append(capabilities, tsqdialect.CapabilityIntersect)
	}

	if strings.Contains(upperSQL, " EXCEPT ") || strings.Contains(upperSQL, " MINUS ") {
		capabilities = append(capabilities, tsqdialect.CapabilityExcept)
	}

	if strings.Contains(upperSQL, " FOR UPDATE") {
		capabilities = append(capabilities, tsqdialect.CapabilitySelectForUpdate)
	}

	if strings.Contains(upperSQL, " FOR SHARE") {
		capabilities = append(capabilities, tsqdialect.CapabilitySelectForShare)
	}

	if strings.Contains(upperSQL, " NOWAIT") {
		capabilities = append(capabilities, tsqdialect.CapabilitySelectForNoWait)
	}

	if strings.Contains(upperSQL, " SKIP LOCKED") {
		capabilities = append(capabilities, tsqdialect.CapabilitySelectForSkipLocked)
	}

	return capabilities
}

func splitTrailingQueryLockClause(sql string) (string, string) {
	for _, clause := range []string{
		" FOR UPDATE SKIP LOCKED",
		" FOR UPDATE NOWAIT",
		" FOR SHARE SKIP LOCKED",
		" FOR SHARE NOWAIT",
		" FOR UPDATE",
		" FOR SHARE",
	} {
		if before, ok := strings.CutSuffix(sql, clause); ok {
			return before, strings.TrimSpace(clause)
		}
	}

	return sql, ""
}

func validateOperationalExecutorForSQL(tx SQLExecutor, rawSQLs ...string) error {
	if err := validateOperationalExecutor(tx); err != nil {
		return err
	}

	return validateExecutorForSQL(tx, rawSQLs...)
}

func validateMutationItem(item Table) error {
	if isNilValue(item) {
		return errors.New("mutation item cannot be nil")
	}

	return nil
}

func validateScanHolder(holder any) error {
	if isNilValue(holder) {
		return errors.New("scan holder cannot be nil")
	}

	if reflect.ValueOf(holder).Kind() != reflect.Pointer {
		return errors.New("scan holder must be a pointer")
	}

	return nil
}
