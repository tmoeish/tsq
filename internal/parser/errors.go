package parser

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
)

// ErrorType 表示错误类型
type ErrorType int

const (
	// Package 相关错误
	ErrorTypePackageImport ErrorType = iota
	ErrorTypeFileParseError

	// Struct 相关错误
	ErrorTypeDuplicateField
	ErrorTypeDuplicateEmbedded
	ErrorTypeEmbeddedCycle
	ErrorTypeEmbeddedNotFound
	ErrorTypeUnsupportedType

	// DSL 相关错误
	ErrorTypeDSLTokenize
	ErrorTypeDSLUnexpectedToken
	ErrorTypeDSLUnexpectedValue
	ErrorTypeDSLUnclosedString
	ErrorTypeDSLInvalidNumber
	ErrorTypeDSLMissingBracket
	ErrorTypeDSLMissingBrace
	ErrorTypeDSLDuplicateKey

	// Field 相关错误
	ErrorTypeFieldUnsupportedType
	ErrorTypeFieldInvalidSelector

	// DSL 字段和索引校验
	ErrorTypeDSLFieldNotFound
	ErrorTypeDSLIndexFieldDuplicate
	ErrorTypeDSLIndexDuplicate
)

// ParserError 表示解析器错误
type ParserError struct {
	Type    ErrorType
	Message string
	Context map[string]any
}

// Error 实现 error 接口
func (e *ParserError) Error() string {
	if prefix := parserErrorLocationPrefix(e.Context); prefix != "" && !strings.HasPrefix(e.Message, prefix+": ") {
		return prefix + ": " + e.Message
	}

	return e.Message
}

// GetType 返回错误类型
func (e *ParserError) GetType() ErrorType {
	return e.Type
}

// GetContext 返回错误上下文
func (e *ParserError) GetContext() map[string]any {
	return e.Context
}

// newParserError 创建解析器错误
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

// ErrorMessages 错误消息模板
var ErrorMessages = map[ErrorType]string{
	// Package 相关错误
	ErrorTypePackageImport:  "failed to import package",
	ErrorTypeFileParseError: "failed to parse file",

	// Struct 相关错误
	ErrorTypeDuplicateField:    "duplicate field in struct",
	ErrorTypeDuplicateEmbedded: "duplicate embedded type in struct",
	ErrorTypeEmbeddedCycle:     "cyclic embedded type in struct",
	ErrorTypeEmbeddedNotFound:  "embedded struct not found",
	ErrorTypeUnsupportedType:   "unsupported type expression",

	// DSL 相关错误
	ErrorTypeDSLTokenize:        "failed to tokenize DSL",
	ErrorTypeDSLUnexpectedToken: "unexpected token in DSL",
	ErrorTypeDSLUnexpectedValue: "unexpected value token in DSL",
	ErrorTypeDSLUnclosedString:  "unclosed string literal in DSL",
	ErrorTypeDSLInvalidNumber:   "invalid number format in DSL",
	ErrorTypeDSLMissingBracket:  "missing bracket in DSL",
	ErrorTypeDSLMissingBrace:    "missing brace in DSL",
	ErrorTypeDSLDuplicateKey:    "duplicate key in DSL",

	// Field 相关错误
	ErrorTypeFieldUnsupportedType: "unsupported field type",
	ErrorTypeFieldInvalidSelector: "invalid selector expression",

	// DSL 字段和索引校验
	ErrorTypeDSLFieldNotFound:       "field '%s' not found in struct '%s'",
	ErrorTypeDSLIndexFieldDuplicate: "duplicate field '%s' in index '%s'",
	ErrorTypeDSLIndexDuplicate:      "duplicate index definition: fields '%s' in index '%s'",
}

// ===== 错误创建辅助函数 =====

// NewPackageImportError 创建包导入错误
func NewPackageImportError(packagePath string, cause error) error {
	msg := fmt.Sprintf("failed to import package: %s", packagePath)
	err := newParserError(ErrorTypePackageImport, msg, map[string]any{
		"package": packagePath,
	})

	return errors.Trace(errors.Annotatef(err, "%v", cause))
}

// NewFileParseError 创建文件解析错误
func NewFileParseError(filename string, cause error) error {
	msg := fmt.Sprintf("failed to parse file: %s", filename)
	err := newParserError(ErrorTypeFileParseError, msg, map[string]any{
		"filename": filename,
	})

	return errors.Trace(errors.Annotatef(err, "%v", cause))
}

// NewDuplicateFieldError 创建重复字段错误
func NewDuplicateFieldError(fieldName, structName string) error {
	msg := fmt.Sprintf("duplicate field '%s' in struct '%s'", fieldName, structName)
	err := newParserError(ErrorTypeDuplicateField, msg, map[string]any{
		"field":  fieldName,
		"struct": structName,
	})

	return errors.Trace(err)
}

// NewDuplicateEmbeddedError 创建重复嵌入类型错误
func NewDuplicateEmbeddedError(typeName, structName string) error {
	msg := fmt.Sprintf("duplicate embedded type '%s' in struct '%s'", typeName, structName)
	err := newParserError(ErrorTypeDuplicateEmbedded, msg, map[string]any{
		"type":   typeName,
		"struct": structName,
	})

	return errors.Trace(err)
}

// NewEmbeddedNotFoundError 创建嵌入结构体未找到错误
func NewEmbeddedNotFoundError(structName string) error {
	msg := fmt.Sprintf("embedded struct not found: '%s'", structName)
	err := newParserError(ErrorTypeEmbeddedNotFound, msg, map[string]any{
		"struct": structName,
	})

	return errors.Trace(err)
}

// NewEmbeddedCycleError 创建嵌入结构体循环引用错误
func NewEmbeddedCycleError(structName string) error {
	msg := fmt.Sprintf("cyclic embedded struct reference: '%s'", structName)
	err := newParserError(ErrorTypeEmbeddedCycle, msg, map[string]any{
		"struct": structName,
	})

	return errors.Trace(err)
}

// NewUnsupportedTypeError 创建不支持类型错误
func NewUnsupportedTypeError(typeExpr any) error {
	typeStr := fmt.Sprintf("%T", typeExpr)
	msg := fmt.Sprintf("unsupported type expression: %s", typeStr)
	err := newParserError(ErrorTypeUnsupportedType, msg, map[string]any{
		"type": typeStr,
	})

	return errors.Trace(err)
}

// ===== DSL 相关错误 =====

// NewDSLTokenizeError 创建 DSL 词法分析错误
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

	return errors.Trace(err)
}

// NewDSLUnexpectedTokenError 创建 DSL 意外 token 错误
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

	return errors.Trace(err)
}

func NewDSLUnexpectedTopLevelTokenError(actual string, position int) error {
	msg := fmt.Sprintf(
		"unexpected token in @TABLE/@RESULT body at position %d: got %q; expected a DSL key like name=..., pk=..., created_at, ux=[...], idx=[...], or kw=[...]",
		position,
		actual,
	)
	err := newParserError(ErrorTypeDSLUnexpectedToken, msg, map[string]any{
		"expected": "top-level DSL key",
		"actual":   actual,
		"position": position,
	})

	return errors.Trace(err)
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

	return errors.Trace(err)
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

	return errors.Trace(err)
}

func NewDSLUnknownTableKeyError(actual string) error {
	return newDSLUnknownKeyError("table DSL", actual, []string{
		"name", "pk", "version", "created_at", "updated_at", "deleted_at", "ux", "idx", "kw",
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

	return errors.Trace(err)
}

func closestDSLKey(actual string, validKeys []string) string {
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

// NewDSLUnexpectedValueError 创建 DSL 意外值错误
func NewDSLUnexpectedValueError(tokenValue string, position int) error {
	msg := fmt.Sprintf(
		"invalid DSL value for %q at position %d",
		tokenValue, position,
	)
	err := newParserError(ErrorTypeDSLUnexpectedValue, msg, map[string]any{
		"token":    tokenValue,
		"position": position,
	})

	return errors.Trace(err)
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

	return errors.Trace(err)
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

	return errors.Trace(err)
}

func NewDSLEmptyArrayError(key string) error {
	msg := fmt.Sprintf("DSL key %q must not be an empty array", key)
	err := newParserError(ErrorTypeDSLUnexpectedValue, msg, map[string]any{
		"key": key,
	})

	return errors.Trace(err)
}

// NewDSLUnclosedStringError 创建 DSL 未闭合字符串错误
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

	return errors.Trace(err)
}

// NewDSLInvalidNumberError 创建 DSL 无效数字错误
func NewDSLInvalidNumberError(numberStr string, position int) error {
	msg := fmt.Sprintf(
		"invalid number %q in DSL at position %d; only digits are supported here",
		numberStr, position,
	)
	err := newParserError(ErrorTypeDSLInvalidNumber, msg, map[string]any{
		"number":   numberStr,
		"position": position,
	})

	return errors.Trace(err)
}

// NewDSLDuplicateKeyError 创建 DSL 重复 key 错误
func NewDSLDuplicateKeyError(key string, position int) error {
	msg := fmt.Sprintf("duplicate DSL key %q at position %d; each key can only appear once in the same object", key, position)
	err := newParserError(ErrorTypeDSLDuplicateKey, msg, map[string]any{
		"key":      key,
		"position": position,
	})

	return errors.Trace(err)
}

// NewDSLMissingBracketError 创建 DSL 缺失括号错误
func NewDSLMissingBracketError(input string, position int) error {
	msg := fmt.Sprintf("missing bracket in DSL at position %d near ...%s...", position, highlightDSLPosition(input, position))
	err := newParserError(ErrorTypeDSLMissingBracket, msg, map[string]any{
		"input":    input,
		"position": position,
	})

	return errors.Trace(err)
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

	return errors.Trace(err)
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

	return errors.Trace(err)
}

func NewDSLMissingBraceError(position int) error {
	msg := fmt.Sprintf("DSL object is missing a closing '}' at position %d", position)
	err := newParserError(ErrorTypeDSLMissingBrace, msg, map[string]any{
		"position": position,
	})

	return errors.Trace(err)
}

func NewDSLArrayMissingClosingBracketError(position int) error {
	msg := fmt.Sprintf("DSL array is missing a closing ']' at position %d", position)
	err := newParserError(ErrorTypeDSLMissingBracket, msg, map[string]any{
		"position": position,
	})

	return errors.Trace(err)
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

	return errors.Trace(err)
}

// ===== Field 相关错误 =====

// NewFieldUnsupportedTypeError 创建字段不支持类型错误
func NewFieldUnsupportedTypeError(typeExpr any) error {
	typeStr := fmt.Sprintf("%T", typeExpr)
	msg := fmt.Sprintf("unsupported field type: %s", typeStr)
	err := newParserError(ErrorTypeFieldUnsupportedType, msg, map[string]any{
		"type": typeStr,
	})

	return errors.Trace(err)
}

func NewFieldUnsupportedCompositionError(description string) error {
	msg := fmt.Sprintf("unsupported field type: %s", description)
	err := newParserError(ErrorTypeFieldUnsupportedType, msg, map[string]any{
		"type": description,
	})

	return errors.Trace(err)
}

// NewFieldInvalidSelectorError 创建字段无效选择器错误
func NewFieldInvalidSelectorError(selectorExpr any) error {
	selStr := fmt.Sprintf("%T", selectorExpr)
	msg := fmt.Sprintf("invalid selector expression: %s", selStr)
	err := newParserError(ErrorTypeFieldInvalidSelector, msg, map[string]any{
		"selector": selStr,
	})

	return errors.Trace(err)
}

// ===== DSL 字段和索引校验 =====

// NewDSLFieldNotFoundError 创建 DSL 字段不存在错误
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

	return errors.Trace(err)
}

// NewDSLIndexFieldDuplicateError 创建索引字段重复错误
func NewDSLIndexFieldDuplicateError(indexName, field string) error {
	msg := fmt.Sprintf("index %q lists Go field %q more than once", indexName, field)
	err := newParserError(ErrorTypeDSLIndexFieldDuplicate,
		msg,
		map[string]any{"index": indexName, "field": field},
	)

	return errors.Trace(err)
}

// NewDSLIndexDuplicateError 创建索引定义重复错误
func NewDSLIndexDuplicateError(indexName, fields string) error {
	msg := fmt.Sprintf("index %q duplicates another index definition with the same fields [%s]", indexName, fields)
	err := newParserError(ErrorTypeDSLIndexDuplicate,
		msg,
		map[string]any{"index": indexName, "fields": fields},
	)

	return errors.Trace(err)
}

// ===== 错误类型检查辅助函数 =====

// IsParserError 检查是否为解析器错误
func IsParserError(err error) bool {
	var parserError *ParserError
	ok := errors.As(err, &parserError)

	return ok
}

// GetParserError 获取解析器错误
func GetParserError(err error) *ParserError {
	var parserErr *ParserError
	if errors.As(err, &parserErr) {
		return parserErr
	}

	return nil
}

// IsErrorType 检查错误是否为指定类型
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
