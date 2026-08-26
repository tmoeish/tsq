package tsq

import "errors"

const (
	postgresUniqueViolation      = "23505"
	postgresSerializationFailure = "40001"
	postgresDeadlockDetected     = "40P01"
	postgresLockNotAvailable     = "55P03"
)

// sqlStateError is the common shape of PostgreSQL driver errors. lib/pq,
// jackc/pgconn (pgx v4) and jackc/pgx/v5/pgconn all expose SQLState() on their
// error types, so matching this interface covers every driver without importing
// any of them. Matching a concrete type from one driver silently misses the
// others: the same SQLSTATE arrives as a different Go type.
type sqlStateError interface {
	error
	SQLState() string
}

func postgresSQLState(err error) (string, bool) {
	if stateErr, ok := errors.AsType[sqlStateError](err); ok {
		return stateErr.SQLState(), true
	}

	return "", false
}

func isPostgresDuplicateKeyError(err error) bool {
	state, ok := postgresSQLState(err)

	return ok && state == postgresUniqueViolation
}

func isPostgresRetryableTransactionConflict(err error) bool {
	state, ok := postgresSQLState(err)
	if !ok {
		return false
	}

	switch state {
	case postgresSerializationFailure, postgresDeadlockDetected, postgresLockNotAvailable:
		return true
	default:
		return false
	}
}
