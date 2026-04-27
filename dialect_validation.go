package tsq

import (
	"fmt"
	"strings"
)

// DialectName represents a SQL dialect type
type DialectName string

const (
	DialectMySQL      DialectName = "mysql"
	DialectPostgres   DialectName = "postgres"
	DialectSQLite     DialectName = "sqlite3"
	DialectSQLServer  DialectName = "sqlserver"
	DialectOracle     DialectName = "oracle"
	DialectUnknown    DialectName = "unknown"
)

// detectDialectName attempts to identify the dialect from its string representation
func detectDialectName(d Dialect) DialectName {
	if d == nil {
		return DialectUnknown
	}

	dialStr := fmt.Sprintf("%T", d)
	dialStr = strings.ToLower(dialStr)

	switch {
	case strings.Contains(dialStr, "mysql"):
		return DialectMySQL
	case strings.Contains(dialStr, "postgres"):
		return DialectPostgres
	case strings.Contains(dialStr, "sqlite"):
		return DialectSQLite
	case strings.Contains(dialStr, "sqlserver"):
		return DialectSQLServer
	case strings.Contains(dialStr, "oracle"):
		return DialectOracle
	default:
		return DialectUnknown
	}
}

// SupportsFullOuterJoin returns whether the dialect supports FULL OUTER JOIN
func SupportsFullOuterJoin(d Dialect) bool {
	dialect := detectDialectName(d)
	switch dialect {
	case DialectPostgres, DialectOracle:
		return true
	case DialectMySQL, DialectSQLite, DialectSQLServer:
		return false
	default:
		return false
	}
}

// ErrUnsupportedOperation represents an operation unsupported by the dialect
type ErrUnsupportedOperation struct {
	operation string
	dialect   DialectName
	reason    string
}

func NewErrUnsupportedOperation(operation string, dialect DialectName, reason string) *ErrUnsupportedOperation {
	return &ErrUnsupportedOperation{
		operation: operation,
		dialect:   dialect,
		reason:    reason,
	}
}

func (e *ErrUnsupportedOperation) Error() string {
	if e.reason != "" {
		return fmt.Sprintf("operation %q not supported by %q: %s", e.operation, e.dialect, e.reason)
	}
	return fmt.Sprintf("operation %q not supported by %q", e.operation, e.dialect)
}

// ValidateOperationForDialect checks if an operation is supported by the dialect
func ValidateOperationForDialect(operation string, d Dialect) error {
	if d == nil {
		// No dialect validation when dialect is unknown
		return nil
	}

	dialect := detectDialectName(d)

	switch operation {
	case "FULL OUTER JOIN":
		if !SupportsFullOuterJoin(d) {
			return NewErrUnsupportedOperation(
				"FULL OUTER JOIN",
				dialect,
				"Use LEFT/RIGHT JOIN with UNION instead",
			)
		}
	}

	return nil
}
