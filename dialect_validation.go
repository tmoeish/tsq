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

func canonicalDialectCapability(operation string) tsqdialect.Capability {
	value := strings.ToUpper(strings.TrimSpace(operation))

	switch value {
	case "FULL JOIN", "FULL OUTER JOIN":
		return tsqdialect.CapabilityFullOuterJoin
	case "CTE":
		return tsqdialect.CapabilityCTE
	case "INTERSECT":
		return tsqdialect.CapabilityIntersect
	case "EXCEPT", "MINUS":
		return tsqdialect.CapabilityExcept
	case "FOR UPDATE":
		return tsqdialect.CapabilitySelectForUpdate
	case "FOR SHARE":
		return tsqdialect.CapabilitySelectForShare
	case "NOWAIT":
		return tsqdialect.CapabilitySelectForNoWait
	case "SKIP LOCKED":
		return tsqdialect.CapabilitySelectForSkipLocked
	default:
		return tsqdialect.Capability(value)
	}
}
