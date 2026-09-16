package parser

import (
	"errors"
	"fmt"
)

// Parser failures a caller may want to tell apart, matched with errors.Is.
var (
	ErrPackageImport    = errors.New("cannot import package")
	ErrDuplicateField   = errors.New("duplicate field")
	ErrEmbeddedCycle    = errors.New("cyclic embedded struct")
	ErrUnsupportedField = errors.New("unsupported field type")
	ErrInvalidDirective = errors.New("invalid //tsq: directive")
)

func packageImportError(importPath string, cause error) error {
	return fmt.Errorf("%w %s: %w", ErrPackageImport, importPath, cause)
}

func duplicateFieldError(fieldName string) error {
	return fmt.Errorf("%w %q", ErrDuplicateField, fieldName)
}

func duplicateEmbeddedError(typeName string) error {
	return fmt.Errorf("%w: embedded type %q appears twice", ErrDuplicateField, typeName)
}

func embeddedCycleError(structName string) error {
	return fmt.Errorf("%w: %s", ErrEmbeddedCycle, structName)
}

func unsupportedFieldError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupportedField, fmt.Sprintf(format, args...))
}
