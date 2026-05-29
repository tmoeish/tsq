package tsq

import "testing"

type fakeSQLiteCodeError struct {
	code int
}

func (e *fakeSQLiteCodeError) Error() string {
	return "sqlite"
}

func (e *fakeSQLiteCodeError) Code() int {
	return e.code
}

func TestSQLiteDuplicateKeyErrorRecognizesExtendedConstraintCodes(t *testing.T) {
	if !isSQLiteDuplicateKeyError(&fakeSQLiteCodeError{code: sqliteConstraintUniqueCode}) {
		t.Fatal("expected SQLITE_CONSTRAINT_UNIQUE to be treated as duplicate key")
	}

	if !isSQLiteDuplicateKeyError(&fakeSQLiteCodeError{code: sqliteConstraintPrimaryKeyCode}) {
		t.Fatal("expected SQLITE_CONSTRAINT_PRIMARYKEY to be treated as duplicate key")
	}

	if isSQLiteDuplicateKeyError(&fakeSQLiteCodeError{code: 19}) {
		t.Fatal("expected base SQLITE_CONSTRAINT to stay non-duplicate without extended code")
	}
}

func TestSQLiteRetryableTransactionConflictUsesPrimaryResultCode(t *testing.T) {
	if !isSQLiteRetryableTransactionConflict(&fakeSQLiteCodeError{code: 261}) {
		t.Fatal("expected extended SQLITE_BUSY code to be retryable")
	}

	if !isSQLiteRetryableTransactionConflict(&fakeSQLiteCodeError{code: sqliteResultLocked}) {
		t.Fatal("expected SQLITE_LOCKED to be retryable")
	}

	if isSQLiteRetryableTransactionConflict(&fakeSQLiteCodeError{code: sqliteConstraintUniqueCode}) {
		t.Fatal("expected unique constraint errors to stay non-retryable")
	}
}
