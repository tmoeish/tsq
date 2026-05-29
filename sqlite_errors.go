package tsq

import "errors"

const (
	sqliteResultBusy               = 5
	sqliteResultLocked             = 6
	sqliteConstraintPrimaryKeyCode = 1555
	sqliteConstraintUniqueCode     = 2067
)

type sqliteErrorCoder interface {
	Code() int
}

func isSQLiteDuplicateKeyError(err error) bool {
	code, ok := sqliteErrorCode(err)

	return ok && (code == sqliteConstraintPrimaryKeyCode || code == sqliteConstraintUniqueCode)
}

func isSQLiteRetryableTransactionConflict(err error) bool {
	code, ok := sqliteErrorCode(err)
	if !ok {
		return false
	}

	switch sqlitePrimaryResultCode(code) {
	case sqliteResultBusy, sqliteResultLocked:
		return true
	default:
		return false
	}
}

func sqliteErrorCode(err error) (int, bool) {
	var sqliteErr sqliteErrorCoder
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code(), true
	}

	return 0, false
}

func sqlitePrimaryResultCode(code int) int {
	return code & 0xff
}
