package parser

import (
	"fmt"

	"github.com/juju/errors"
)

const (
	colorRed   = "\033[31m"
	colorReset = "\033[0m"
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

// ErrorMessages 错误消息模板
var ErrorMessages = map[ErrorType]string{
	// Package 相关错误
	ErrorTypePackageImport:  "failed to import package",
	ErrorTypeFileParseError: "failed to parse file",

	// Struct 相关错误
	ErrorTypeDuplicateField:    "duplicate field in struct",
	ErrorTypeDuplicateEmbedded: "duplicate embedded type in struct",
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

	return errors.Trace(errors.Wrap(err, cause))
}

// NewFileParseError 创建文件解析错误
func NewFileParseError(filename string, cause error) error {
	msg := fmt.Sprintf("failed to parse file: %s", filename)
	err := newParserError(ErrorTypeFileParseError, msg, map[string]any{
		"filename": filename,
	})

	return errors.Trace(errors.Wrap(err, cause))
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
	contextLen := 20

	start := position - contextLen
	if start < 0 {
		start = 0
	}

	end := position + contextLen
	if end > len(input) {
		end = len(input)
	}

	snippet := input[start:end]
	highlightIdx := position - start

	var highlightedSnippet string

	if highlightIdx >= 0 && highlightIdx < len(snippet) {
		highlightedSnippet = snippet[:highlightIdx] + colorRed + string(snippet[highlightIdx]) + colorReset + snippet[highlightIdx+1:]
	} else {
		highlightedSnippet = snippet
	}

	msg := fmt.Sprintf(
		"failed to tokenize DSL at position %d, char: '%s', context: ...%s...",
		position, string(char), highlightedSnippet,
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
		"unexpected token in DSL at %sposition %d: expected '%s', got '%s'%s",
		colorRed, position, expected, actual, colorReset,
	)
	err := newParserError(ErrorTypeDSLUnexpectedToken, msg, map[string]any{
		"expected": expected,
		"actual":   actual,
		"position": position,
	})

	return errors.Trace(err)
}

// NewDSLUnexpectedValueError 创建 DSL 意外值错误
func NewDSLUnexpectedValueError(tokenValue string, position int) error {
	msg := fmt.Sprintf(
		"unexpected value token in DSL at %sposition %d: '%s'%s",
		colorRed, position, tokenValue, colorReset,
	)
	err := newParserError(ErrorTypeDSLUnexpectedValue, msg, map[string]any{
		"token":    tokenValue,
		"position": position,
	})

	return errors.Trace(err)
}

// NewDSLUnclosedStringError 创建 DSL 未闭合字符串错误
func NewDSLUnclosedStringError(input string, position int) error {
	contextLen := 20

	start := position - contextLen
	if start < 0 {
		start = 0
	}

	end := position + contextLen
	if end > len(input) {
		end = len(input)
	}

	snippet := input[start:end]
	highlightIdx := position - start

	var highlightedSnippet string

	if highlightIdx >= 0 && highlightIdx < len(snippet) {
		highlightedSnippet = snippet[:highlightIdx] + colorRed + string(snippet[highlightIdx]) + colorReset + snippet[highlightIdx+1:]
	} else {
		highlightedSnippet = snippet
	}

	msg := fmt.Sprintf(
		"unclosed string literal in DSL at position %d, context: ...%s...",
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
		"invalid number format in DSL at %sposition %d: '%s'%s",
		colorRed, position, numberStr, colorReset,
	)
	err := newParserError(ErrorTypeDSLInvalidNumber, msg, map[string]any{
		"number":   numberStr,
		"position": position,
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
	msg := fmt.Sprintf("field '%s' not found in struct '%s'", field, structName)
	err := newParserError(ErrorTypeDSLFieldNotFound,
		msg,
		map[string]any{"field": field, "struct": structName},
	)

	return errors.Trace(err)
}

// NewDSLIndexFieldDuplicateError 创建索引字段重复错误
func NewDSLIndexFieldDuplicateError(indexName, field string) error {
	msg := fmt.Sprintf("duplicate field '%s' in index '%s'", field, indexName)
	err := newParserError(ErrorTypeDSLIndexFieldDuplicate,
		msg,
		map[string]any{"index": indexName, "field": field},
	)

	return errors.Trace(err)
}

// NewDSLIndexDuplicateError 创建索引定义重复错误
func NewDSLIndexDuplicateError(indexName, fields string) error {
	msg := fmt.Sprintf("duplicate index definition: fields '%s' in index '%s'", fields, indexName)
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
	ok := errors.As(errors.Cause(err), &parserError)

	return ok
}

// GetParserError 获取解析器错误
func GetParserError(err error) *ParserError {
	var parserErr *ParserError
	if errors.As(errors.Cause(err), &parserErr) {
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
