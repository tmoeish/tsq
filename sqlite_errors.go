package tsq

import (
	"errors"
	"reflect"
)

const (
	sqliteResultBusy               = 5
	sqliteResultLocked             = 6
	sqliteConstraintPrimaryKeyCode = 1555
	sqliteConstraintUniqueCode     = 2067
)

type sqliteErrorCoder interface {
	error
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
	if sqliteErr, ok := errors.AsType[sqliteErrorCoder](err); ok {
		return sqliteErr.Code(), true
	}

	return mattnSQLiteErrorCode(err)
}

// mattnSQLiteErrorCode reads the result code of a github.com/mattn/go-sqlite3
// Error. That driver reports the codes as struct fields rather than methods, so
// there is no interface to match: without this, every classification below would
// silently answer "not a SQLite error" on the CGO driver, and duplicate keys and
// busy databases would stop being recognized.
func mattnSQLiteErrorCode(err error) (int, bool) {
	for _, candidate := range errorChain(err) {
		v := reflect.ValueOf(candidate)
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				continue
			}

			v = v.Elem()
		}

		if v.Kind() != reflect.Struct {
			continue
		}

		t := v.Type()
		if t.Name() != "Error" || t.PkgPath() != "github.com/mattn/go-sqlite3" {
			continue
		}

		// The extended code carries the constraint kind (2067 unique, 1555 primary
		// key); the plain code is the fallback for errors without one.
		for _, field := range []string{"ExtendedCode", "Code"} {
			f := v.FieldByName(field)
			if f.IsValid() && f.CanInt() && f.Int() != 0 {
				return int(f.Int()), true
			}
		}
	}

	return 0, false
}

func sqlitePrimaryResultCode(code int) int {
	return code & 0xff
}
