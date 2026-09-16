package tsq

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/go-sql-driver/mysql"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return mysqlErr.Number == 1062
	}

	if isSQLiteDuplicateKeyError(err) {
		return true
	}

	return isPostgresDuplicateKeyError(err)
}

func normalizePageReq(page *PageRequest) *PageRequest {
	return normalizePageReqWithLimit(page, DefaultMaxPageSize)
}

func normalizePageReqWithLimit(page *PageRequest, maxSize int) *PageRequest {
	if page == nil {
		page = &PageRequest{}
	}

	if maxSize <= 0 {
		maxSize = DefaultMaxPageSize
	}

	normalized := *page
	_ = normalized.NormalizeWithLimit(maxSize)

	return &normalized
}

func pageSizeLimitForExecutor(exec SQLExecutor) int {
	return runtimeForExecutor(exec).MaxPageSize()
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
		// Identifiers are quoted while Build() renders SQL, which happens before any
		// executor or runtime is in play, so there is no RuntimeOptions.Logger to route
		// this to. Dialect-specific length limits are enforced later, at execution time.
		slog.Default().Warn("identifier is unusually long", "identifier", name, "length", len(name))
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

	// A correlated query references tables its own FROM clause does not introduce,
	// so on its own it is not a runnable statement. Refusing here turns what would
	// be an obscure database error into one that names the cause.
	if q.correlated {
		return errors.New(
			"query declares correlated outer tables with Correlate and can only be used as a subquery, not executed on its own",
		)
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

// detectSQLCapabilities reports the dialect features a rendered statement needs.
//
// It reads the statement rather than the builder because the structure is no
// longer available at this point: a subquery reaches the outer query as SQL text
// inside a condition, so deriving capabilities from the builder alone would miss
// a FULL JOIN or a CTE that lives in one. Under-reporting is the worse failure:
// the query would run and fail on the server instead of being refused here with
// the capability and dialect named.
//
// Matching skips string literals and comments. A literal containing the words
// " FOR UPDATE " used to make a perfectly ordinary query unrunnable on SQLite.
func detectSQLCapabilities(rawSQL string) []tsqdialect.Capability {
	trimmed := strings.TrimSpace(rawSQL)
	capabilities := make([]tsqdialect.Capability, 0, 8)

	if hasPrefixFold(trimmed, "WITH ") {
		capabilities = append(capabilities, tsqdialect.CapabilityCTE)
	}

	for _, match := range []struct {
		keyword    string
		capability tsqdialect.Capability
	}{
		{" FULL JOIN ", tsqdialect.CapabilityFullOuterJoin},
		{" INTERSECT ", tsqdialect.CapabilityIntersect},
		{" EXCEPT ", tsqdialect.CapabilityExcept},
		{" MINUS ", tsqdialect.CapabilityExcept},
		{" FOR UPDATE", tsqdialect.CapabilitySelectForUpdate},
		{" FOR SHARE", tsqdialect.CapabilitySelectForShare},
		{" NOWAIT", tsqdialect.CapabilitySelectForNoWait},
		{" SKIP LOCKED", tsqdialect.CapabilitySelectForSkipLocked},
	} {
		if sqlContainsOutsideLiterals(trimmed, match.keyword) {
			capabilities = append(capabilities, match.capability)
		}
	}

	return capabilities
}

// sqlContainsOutsideLiterals reports whether needle appears in raw as SQL rather
// than inside a string literal or a comment.
func sqlContainsOutsideLiterals(raw, needle string) bool {
	return walkSQL(raw, nil, func(source string, i int, _ *strings.Builder) (int, bool, bool) {
		match := hasPrefixFold(source[i:], needle)

		return len(needle), match, match
	})
}

// hasPrefixFold is strings.HasPrefix for the ASCII keywords TSQ matches on.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
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
