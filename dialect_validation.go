package tsq

import (
	"errors"
	"fmt"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func validateOperationForDialect(operation string, d tsqdialect.Dialect) error {
	if d == nil {
		return nil
	}

	return tsqdialect.ValidateCapability(d, canonicalDialectCapability(operation))
}

func validateIdentifierForDialect(identifier string, sqlDialect tsqdialect.Dialect) error {
	if identifier == "" {
		return errors.New("identifier cannot be empty")
	}

	if !builtInIdentifierPattern.MatchString(identifier) {
		return fmt.Errorf("invalid SQL identifier: %s (must match pattern [A-Za-z_][A-Za-z0-9_]*)", identifier)
	}

	return validateIdentifierLength(identifier, sqlDialect)
}

func validateIdentifierLength(identifier string, sqlDialect tsqdialect.Dialect) error {
	return tsqdialect.ValidateIdentifierLength(identifier, sqlDialect)
}

func canonicalDialectCapability(operation string) tsqdialect.DialectCapability {
	value := strings.ToUpper(strings.TrimSpace(operation))

	switch value {
	case "FULL JOIN", "FULL OUTER JOIN":
		return tsqdialect.DialectCapabilityFullOuterJoin
	case "CTE":
		return tsqdialect.DialectCapabilityCTE
	case "INTERSECT":
		return tsqdialect.DialectCapabilityIntersect
	case "EXCEPT", "MINUS":
		return tsqdialect.DialectCapabilityExcept
	case "FOR UPDATE":
		return tsqdialect.DialectCapabilitySelectForUpdate
	case "FOR SHARE":
		return tsqdialect.DialectCapabilitySelectForShare
	case "NOWAIT":
		return tsqdialect.DialectCapabilitySelectForNoWait
	case "SKIP LOCKED":
		return tsqdialect.DialectCapabilitySelectForSkipLocked
	default:
		return tsqdialect.DialectCapability(value)
	}
}
