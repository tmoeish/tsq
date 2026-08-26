package tsq

import (
	"errors"
	"fmt"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

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
