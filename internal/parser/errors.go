package parser

import (
	"errors"
	"fmt"
	"strings"
)

// ErrorType classifies parser errors.
type ErrorType int

const (
	// Package errors
	ErrorTypePackageImport ErrorType = iota

	// Struct errors
	ErrorTypeDuplicateField
	ErrorTypeDuplicateEmbedded
	ErrorTypeEmbeddedCycle

	// DSL errors
	ErrorTypeDSLTokenize
	ErrorTypeDSLUnexpectedToken
	ErrorTypeDSLUnexpectedValue
	ErrorTypeDSLUnclosedString
	ErrorTypeDSLInvalidNumber
	ErrorTypeDSLMissingBracket
	ErrorTypeDSLMissingBrace
	ErrorTypeDSLDuplicateKey

	// Field errors
	ErrorTypeFieldUnsupportedType
	ErrorTypeFieldInvalidSelector

	// DSL field and index validation
	ErrorTypeDSLFieldNotFound
	ErrorTypeDSLIndexFieldDuplicate
	ErrorTypeDSLIndexDuplicate
)

// ParserError is the error type returned by the parser.
type ParserError struct {
	Type    ErrorType
	Message string
	Context map[string]any
}

// Error implements error.
func (e *ParserError) Error() string {
	if prefix := parserErrorLocationPrefix(e.Context); prefix != "" && !strings.HasPrefix(e.Message, prefix+": ") {
		return prefix + ": " + e.Message
	}

	return e.Message
}

// GetType returns the error classification.
func (e *ParserError) GetType() ErrorType {
	return e.Type
}

// GetContext returns the error context.
func (e *ParserError) GetContext() map[string]any {
	return e.Context
}

// newParserError builds a ParserError.
func newParserError(errorType ErrorType, message string, context map[string]any) *ParserError {
	return &ParserError{
		Type:    errorType,
		Message: message,
		Context: context,
	}
}

func parserErrorLocationPrefix(context map[string]any) string {
	if context == nil {
		return ""
	}

	filename, ok := context["filename"].(string)
	if !ok || filename == "" {
		return ""
	}

	line, _ := context["line"].(int)
	if line > 0 {
		return fmt.Sprintf("%s:%d", filename, line)
	}

	return filename
}

// ===== Error constructors =====

// NewPackageImportError reports a package that could not be imported.
func NewPackageImportError(packagePath string, cause error) error {
	msg := fmt.Sprintf("failed to import package: %s", packagePath)
	err := newParserError(ErrorTypePackageImport, msg, map[string]any{
		"package": packagePath,
	})

	return fmt.Errorf("%v"+": %w", cause, err)
}

// NewDuplicateFieldError reports a field declared twice.
func NewDuplicateFieldError(fieldName, structName string) error {
	msg := fmt.Sprintf("duplicate field '%s' in struct '%s'", fieldName, structName)
	err := newParserError(ErrorTypeDuplicateField, msg, map[string]any{
		"field":  fieldName,
		"struct": structName,
	})

	return err
}

// NewDuplicateEmbeddedError reports an embedded type declared twice.
func NewDuplicateEmbeddedError(typeName, structName string) error {
	msg := fmt.Sprintf("duplicate embedded type '%s' in struct '%s'", typeName, structName)
	err := newParserError(ErrorTypeDuplicateEmbedded, msg, map[string]any{
		"type":   typeName,
		"struct": structName,
	})

	return err
}

// NewEmbeddedCycleError reports a cycle in embedded structs.
func NewEmbeddedCycleError(structName string) error {
	msg := fmt.Sprintf("cyclic embedded struct reference: '%s'", structName)
	err := newParserError(ErrorTypeEmbeddedCycle, msg, map[string]any{
		"struct": structName,
	})

	return err
}

// ===== DSL errors =====

// NewDSLTokenizeError reports a DSL tokenization failure.
func NewDSLTokenizeError(input string, position int, char byte) error {
	highlightedSnippet := highlightDSLPosition(input, position)
	msg := fmt.Sprintf(
		"invalid character %q in DSL at position %d; expected a key, string, number, ',', '=', '[', ']', '{', or '}' near ...%s...",
		string(char), position, highlightedSnippet,
	)
	err := newParserError(ErrorTypeDSLTokenize, msg, map[string]any{
		"input":    input,
		"position": position,
		"char":     string(char),
		"snippet":  highlightedSnippet,
	})

	return err
}

// NewDSLUnexpectedTokenError reports an unexpected DSL token.
func NewDSLUnexpectedTokenError(expected, actual string, position int) error {
	msg := fmt.Sprintf(
		"malformed DSL at position %d: expected %s, got %q",
		position, expected, actual,
	)
	err := newParserError(ErrorTypeDSLUnexpectedToken, msg, map[string]any{
		"expected": expected,
		"actual":   actual,
		"position": position,
	})

	return err
}

func NewDSLUnexpectedTopLevelTokenError(actual string, position int) error {
	msg := fmt.Sprintf(
		"unexpected token in @TABLE/@RESULT body at position %d: got %q; expected a DSL key like name=..., pk=..., created_at, ux=[...], idx=[...], or search=[...]",
		position,
		actual,
	)
	err := newParserError(ErrorTypeDSLUnexpectedToken, msg, map[string]any{
		"expected": "top-level DSL key",
		"actual":   actual,
		"position": position,
	})

	return err
}

func NewDSLUnexpectedObjectTokenError(actual string, position int) error {
	msg := fmt.Sprintf(
		"unexpected token in DSL object at position %d: got %q; expected object field like name=... or fields=[...], or closing '}'",
		position,
		actual,
	)
	if actual == "]" {
		msg += " (did you forget a closing '}' before ']'?)"
	}

	err := newParserError(ErrorTypeDSLUnexpectedToken, msg, map[string]any{
		"expected": "object field or closing brace",
		"actual":   actual,
		"position": position,
	})

	return err
}

func NewDSLUnexpectedValueTokenError(actual string, position int) error {
	msg := fmt.Sprintf(
		"invalid DSL value at position %d: got %q; expected a string, boolean, number, array [...], or object {...}",
		position,
		actual,
	)
	err := newParserError(ErrorTypeDSLUnexpectedValue, msg, map[string]any{
		"token":    actual,
		"position": position,
	})

	return err
}

func NewDSLUnknownTableKeyError(actual string) error {
	return newDSLUnknownKeyError("table DSL", actual, []string{
		"name", "pk", "version", "created_at", "updated_at", "deleted_at", "ux", "idx", "search",
	})
}

func NewDSLUnknownIndexKeyError(actual string) error {
	return newDSLUnknownKeyError("index DSL", actual, []string{
		"name", "fields",
	})
}

func newDSLUnknownKeyError(scope, actual string, validKeys []string) error {
	msg := fmt.Sprintf(
		"unknown %s key %q; valid keys: %s",
		scope,
		actual,
		strings.Join(validKeys, ", "),
	)
	if suggestion := closestDSLKey(actual, validKeys); suggestion != "" && suggestion != actual {
		msg += fmt.Sprintf(" (did you mean %q?)", suggestion)
	}

	err := newParserError(ErrorTypeDSLUnexpectedToken, msg, map[string]any{
		"expected":  strings.Join(validKeys, ", "),
		"actual":    actual,
		"validKeys": append([]string(nil), validKeys...),
	})

	return err
}

func closestDSLKey(actual string, validKeys []string) string {
	if actual == "kw" {
		for _, key := range validKeys {
			if key == "search" {
				return key
			}
		}
	}

	bestKey := ""
	bestDistance := -1

	for _, key := range validKeys {
		distance := levenshteinDistance(actual, key)
		if bestDistance == -1 || distance < bestDistance {
			bestDistance = distance
			bestKey = key
		}
	}

	if bestDistance == -1 || bestDistance > 3 {
		return ""
	}

	return bestKey
}

func levenshteinDistance(a, b string) int {
	if a == b {
		return 0
	}

	if len(a) == 0 {
		return len(b)
	}

	if len(b) == 0 {
		return len(a)
	}

	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)

	for j := range len(b) + 1 {
		prev[j] = j
	}

	for i := 1; i <= len(a); i++ {
		curr[0] = i

		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}

			curr[j] = min(
				prev[j]+1,
				curr[j-1]+1,
				prev[j-1]+cost,
			)
		}

		prev, curr = curr, prev
	}

	return prev[len(b)]
}

func NewDSLValueTypeError(key, expected string, actual any) error {
	msg := fmt.Sprintf(
		"invalid value for DSL key %q: expected %s, got %s",
		key,
		expected,
		describeDSLValue(actual),
	)
	err := newParserError(ErrorTypeDSLUnexpectedValue, msg, map[string]any{
		"key":      key,
		"expected": expected,
		"actual":   describeDSLValue(actual),
	})

	return err
}

func NewDSLArrayEntryTypeError(key, expected string, actual any) error {
	msg := fmt.Sprintf(
		"invalid entry in DSL array %q: expected %s, got %s",
		key,
		expected,
		describeDSLValue(actual),
	)
	err := newParserError(ErrorTypeDSLUnexpectedValue, msg, map[string]any{
		"key":      key,
		"expected": expected,
		"actual":   describeDSLValue(actual),
	})

	return err
}

func NewDSLEmptyArrayError(key string) error {
	msg := fmt.Sprintf("DSL key %q must not be an empty array", key)
	err := newParserError(ErrorTypeDSLUnexpectedValue, msg, map[string]any{
		"key": key,
	})

	return err
}

// NewDSLUnclosedStringError reports an unterminated DSL string.
func NewDSLUnclosedStringError(input string, position int) error {
	highlightedSnippet := highlightDSLPosition(input, position)
	msg := fmt.Sprintf(
		"unclosed string literal in DSL at position %d; add the missing closing quote near ...%s...",
		position, highlightedSnippet,
	)
	err := newParserError(ErrorTypeDSLUnclosedString, msg, map[string]any{
		"input":    input,
		"position": position,
		"snippet":  highlightedSnippet,
	})

	return err
}

// NewDSLInvalidNumberError reports a malformed DSL number.
func NewDSLInvalidNumberError(numberStr string, position int) error {
	msg := fmt.Sprintf(
		"invalid number %q in DSL at position %d; only digits are supported here",
		numberStr, position,
	)
	err := newParserError(ErrorTypeDSLInvalidNumber, msg, map[string]any{
		"number":   numberStr,
		"position": position,
	})

	return err
}

// NewDSLDuplicateKeyError reports a DSL key given twice.
func NewDSLDuplicateKeyError(key string, position int) error {
	msg := fmt.Sprintf("duplicate DSL key %q at position %d; each key can only appear once in the same object", key, position)
	err := newParserError(ErrorTypeDSLDuplicateKey, msg, map[string]any{
		"key":      key,
		"position": position,
	})

	return err
}

func NewDSLAnnotationMissingOpeningParenError(keyword, input string, position int) error {
	msg := fmt.Sprintf(
		"%s must be followed by '('; use %s(...) near ...%s...",
		keyword,
		keyword,
		highlightDSLPosition(input, position),
	)
	err := newParserError(ErrorTypeDSLMissingBracket, msg, map[string]any{
		"input":    input,
		"keyword":  keyword,
		"position": position,
	})

	return err
}

func NewDSLAnnotationMissingClosingParenError(keyword, input string, position int) error {
	msg := fmt.Sprintf(
		"%s is missing a closing ')' near ...%s...",
		keyword,
		highlightDSLPosition(input, position),
	)
	err := newParserError(ErrorTypeDSLMissingBracket, msg, map[string]any{
		"input":    input,
		"keyword":  keyword,
		"position": position,
	})

	return err
}

func NewDSLMissingBraceError(position int) error {
	msg := fmt.Sprintf("DSL object is missing a closing '}' at position %d", position)
	err := newParserError(ErrorTypeDSLMissingBrace, msg, map[string]any{
		"position": position,
	})

	return err
}

func NewDSLArrayMissingClosingBracketError(position int) error {
	msg := fmt.Sprintf("DSL array is missing a closing ']' at position %d", position)
	err := newParserError(ErrorTypeDSLMissingBracket, msg, map[string]any{
		"position": position,
	})

	return err
}

func NewDSLInvalidPrimaryKeyError(value, reason string) error {
	msg := fmt.Sprintf(
		"invalid pk value %q: %s; expected %q or %q",
		value,
		reason,
		"ID",
		"ID,true",
	)
	err := newParserError(ErrorTypeDSLUnexpectedValue, msg, map[string]any{
		"key":    "pk",
		"value":  value,
		"reason": reason,
	})

	return err
}

// ===== Field errors =====

// NewFieldUnsupportedTypeError reports a field type the generator cannot handle.
func NewFieldUnsupportedTypeError(typeExpr any) error {
	typeStr := fmt.Sprintf("%T", typeExpr)
	msg := fmt.Sprintf("unsupported field type: %s", typeStr)
	err := newParserError(ErrorTypeFieldUnsupportedType, msg, map[string]any{
		"type": typeStr,
	})

	return err
}

func NewFieldUnsupportedCompositionError(description string) error {
	msg := fmt.Sprintf("unsupported field type: %s", description)
	err := newParserError(ErrorTypeFieldUnsupportedType, msg, map[string]any{
		"type": description,
	})

	return err
}

// NewFieldInvalidSelectorError reports an unsupported selector expression.
func NewFieldInvalidSelectorError(selectorExpr any) error {
	selStr := fmt.Sprintf("%T", selectorExpr)
	msg := fmt.Sprintf("invalid selector expression: %s", selStr)
	err := newParserError(ErrorTypeFieldInvalidSelector, msg, map[string]any{
		"selector": selStr,
	})

	return err
}

// ===== DSL field and index validation =====

// NewDSLFieldNotFoundError reports a DSL reference to a field the struct lacks.
func NewDSLFieldNotFoundError(field, structName string) error {
	msg := fmt.Sprintf(
		"DSL references unknown Go field '%s' in struct '%s' (use struct field names, not db column names)",
		field,
		structName,
	)
	err := newParserError(ErrorTypeDSLFieldNotFound,
		msg,
		map[string]any{"field": field, "struct": structName},
	)

	return err
}

// NewDSLIndexFieldDuplicateError reports a field listed twice in one index.
func NewDSLIndexFieldDuplicateError(indexName, field string) error {
	msg := fmt.Sprintf("index %q lists Go field %q more than once", indexName, field)
	err := newParserError(ErrorTypeDSLIndexFieldDuplicate,
		msg,
		map[string]any{"index": indexName, "field": field},
	)

	return err
}

// NewDSLIndexDuplicateError reports two indexes with the same definition.
func NewDSLIndexDuplicateError(indexName, fields string) error {
	msg := fmt.Sprintf("index %q duplicates another index definition with the same fields [%s]", indexName, fields)
	err := newParserError(ErrorTypeDSLIndexDuplicate,
		msg,
		map[string]any{"index": indexName, "fields": fields},
	)

	return err
}

// ===== Error inspection helpers =====

// GetParserError extracts the ParserError from err, if any.
func GetParserError(err error) *ParserError {
	if parserErr, ok := errors.AsType[*ParserError](err); ok {
		return parserErr
	}

	return nil
}

// IsErrorType reports whether err is a ParserError of the given type.
func IsErrorType(err error, errorType ErrorType) bool {
	if parserErr := GetParserError(err); parserErr != nil {
		return parserErr.Type == errorType
	}

	return false
}

func attachParserErrorLocation(err error, filename string, line int) error {
	if err == nil || filename == "" || line <= 0 {
		return err
	}

	parserErr := GetParserError(err)
	if parserErr == nil {
		return err
	}

	if parserErr.Context == nil {
		parserErr.Context = make(map[string]any)
	}

	if _, exists := parserErr.Context["filename"]; exists {
		return err
	}

	parserErr.Context["filename"] = filename
	parserErr.Context["line"] = line

	return err
}

func highlightDSLPosition(input string, position int) string {
	contextLen := 20
	start := max(position-contextLen, 0)
	end := min(position+contextLen, len(input))
	snippet := input[start:end]

	highlightIdx := position - start
	if highlightIdx >= 0 && highlightIdx < len(snippet) {
		return snippet[:highlightIdx] + ">" + string(snippet[highlightIdx]) + "<" + snippet[highlightIdx+1:]
	}

	return snippet
}

func describeDSLValue(value any) string {
	switch v := value.(type) {
	case DSLString:
		return "string"
	case DSLBool:
		return "boolean"
	case DSLNumber:
		return "number"
	case DSLArray:
		return "array"
	case DSLObject:
		return "object"
	case nil:
		return "empty value"
	case string:
		return fmt.Sprintf("token %q", v)
	default:
		return fmt.Sprintf("%T", value)
	}
}
