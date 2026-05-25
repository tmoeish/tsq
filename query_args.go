package tsq

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// escapeKeywordSearch escapes special characters in keyword search strings for use with LIKE clauses.
// This prevents SQL injection via LIKE wildcard characters (% and _).
//
// Example:
//
//	keyword := "100% cotton"
//	escaped := EscapeKeywordSearch(keyword)  // "100\\% cotton"
//
// Note: When using this function, your SQL dialect may require you to specify the escape character
// in the LIKE clause. For example:
//
//	SELECT * FROM table WHERE column LIKE ? ESCAPE '\'
//
// Currently, TSQ keyword search does not apply escaping automatically. Users MUST call this function
// if their keywords contain % or _ characters to prevent unintended pattern matching or SQL injection.
func escapeKeywordSearch(keyword string) string {
	s := strings.ReplaceAll(keyword, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")

	return s
}

func resolveQueryArgs(base, extra []any, keyword string) ([]any, error) {
	_, args, err := resolveQueryWithState("", base, extra, keyword, scanQueryArgState(base))
	return args, err
}

func resolveQuery(baseSQL string, base, extra []any, keyword string) (string, []any, error) {
	return resolveQueryWithState(baseSQL, base, extra, keyword, scanQueryArgState(base))
}

func resolveQueryWithState(
	baseSQL string,
	base,
	extra []any,
	keyword string,
	state queryArgState,
) (string, []any, error) {
	if !state.initialized {
		state = scanQueryArgState(base)
	}

	if !state.hasDeferredArgs() {
		if len(extra) == 0 {
			return baseSQL, base, nil
		}

		result := make([]any, 0, len(base)+len(extra))
		result = append(result, base...)
		result = append(result, extra...)

		return baseSQL, result, nil
	}

	if !state.hasExternalSliceArg {
		args, err := resolveQueryArgsOnly(base, extra, keyword)
		return baseSQL, args, err
	}

	result := make([]any, 0, len(base)+len(extra))
	extraIndex := 0
	like := ""
	cursor := 0

	var sqlBuilder strings.Builder
	hasSQL := baseSQL != ""

	for _, arg := range base {
		if hasSQL {
			next := strings.Index(baseSQL[cursor:], "?")
			if next < 0 {
				sqlBuilder.WriteString(baseSQL[cursor:])
				hasSQL = false
			} else {
				sqlBuilder.WriteString(baseSQL[cursor : cursor+next])
				cursor += next + 1
			}
		}

		switch arg {
		case externalArgMarker:
			if extraIndex >= len(extra) {
				return "", nil, errors.New("missing external query argument")
			}

			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, extra[extraIndex])
			extraIndex++
		case externalStartsWithMarker:
			if extraIndex >= len(extra) {
				return "", nil, errors.New("missing external query argument")
			}

			value, err := buildPatternQueryArg(extra[extraIndex], "", "%")
			if err != nil {
				return "", nil, err
			}

			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, value)
			extraIndex++
		case externalEndsWithMarker:
			if extraIndex >= len(extra) {
				return "", nil, errors.New("missing external query argument")
			}

			value, err := buildPatternQueryArg(extra[extraIndex], "%", "")
			if err != nil {
				return "", nil, err
			}

			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, value)
			extraIndex++
		case externalContainsMarker:
			if extraIndex >= len(extra) {
				return "", nil, errors.New("missing external query argument")
			}

			value, err := buildPatternQueryArg(extra[extraIndex], "%", "%")
			if err != nil {
				return "", nil, err
			}

			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, value)
			extraIndex++
		case externalSliceArgMarker{}:
			if extraIndex >= len(extra) {
				return "", nil, errors.New("missing external query argument")
			}

			values, err := flattenExternalSliceArg(extra[extraIndex])
			if err != nil {
				return "", nil, err
			}

			if hasSQL {
				sqlBuilder.WriteString(expandSlicePlaceholders(len(values)))
			}

			result = append(result, values...)
			extraIndex++
		case externalNotInSliceArgMarker{}:
			if extraIndex >= len(extra) {
				return "", nil, errors.New("missing external query argument")
			}

			values, err := flattenExternalSliceArg(extra[extraIndex])
			if err != nil {
				return "", nil, err
			}

			if hasSQL {
				sqlBuilder.WriteString(expandNotInSlicePlaceholders(len(values)))
			}

			result = append(result, values...)
			extraIndex++
		case keywordArgMarker:
			if keyword == "" {
				return "", nil, errors.New("missing keyword query argument")
			}

			if like == "" {
				like = "%" + keyword + "%"
			}

			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, like)
		default:
			if hasSQL {
				sqlBuilder.WriteString("?")
			}

			result = append(result, arg)
		}
	}

	result = append(result, extra[extraIndex:]...)

	if hasSQL {
		sqlBuilder.WriteString(baseSQL[cursor:])
		return sqlBuilder.String(), result, nil
	}

	return baseSQL, result, nil
}

func resolveQueryArgsOnly(base, extra []any, keyword string) ([]any, error) {
	result := make([]any, 0, len(base)+len(extra))
	extraIndex := 0
	like := ""

	for _, arg := range base {
		switch arg {
		case externalArgMarker:
			if extraIndex >= len(extra) {
				return nil, errors.New("missing external query argument")
			}

			result = append(result, extra[extraIndex])
			extraIndex++
		case externalStartsWithMarker:
			if extraIndex >= len(extra) {
				return nil, errors.New("missing external query argument")
			}

			value, err := buildPatternQueryArg(extra[extraIndex], "", "%")
			if err != nil {
				return nil, err
			}

			result = append(result, value)
			extraIndex++
		case externalEndsWithMarker:
			if extraIndex >= len(extra) {
				return nil, errors.New("missing external query argument")
			}

			value, err := buildPatternQueryArg(extra[extraIndex], "%", "")
			if err != nil {
				return nil, err
			}

			result = append(result, value)
			extraIndex++
		case externalContainsMarker:
			if extraIndex >= len(extra) {
				return nil, errors.New("missing external query argument")
			}

			value, err := buildPatternQueryArg(extra[extraIndex], "%", "%")
			if err != nil {
				return nil, err
			}

			result = append(result, value)
			extraIndex++
		case keywordArgMarker:
			if keyword == "" {
				return nil, errors.New("missing keyword query argument")
			}

			if like == "" {
				like = "%" + keyword + "%"
			}

			result = append(result, like)
		case externalNotInSliceArgMarker{}:
			if extraIndex >= len(extra) {
				return nil, errors.New("missing external query argument")
			}

			values, err := flattenExternalSliceArg(extra[extraIndex])
			if err != nil {
				return nil, err
			}

			result = append(result, values...)
			extraIndex++
		default:
			result = append(result, arg)
		}
	}

	result = append(result, extra[extraIndex:]...)

	return result, nil
}

func flattenExternalSliceArg(arg any) ([]any, error) {
	if isNilValue(arg) {
		return nil, nil
	}

	switch v := arg.(type) {
	case []any:
		return validateAnySlice(v)
	case []int:
		return boxSlice(v), nil
	case []int64:
		return boxSlice(v), nil
	case []string:
		return boxSlice(v), nil
	case []bool:
		return boxSlice(v), nil
	case []float64:
		return boxSlice(v), nil
	case []float32:
		return boxSlice(v), nil
	case *[]any:
		if v == nil {
			return nil, nil
		}

		return validateAnySlice(*v)
	case *[]int:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]int64:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]string:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]bool:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]float64:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	case *[]float32:
		if v == nil {
			return nil, nil
		}

		return boxSlice(*v), nil
	}

	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, nil
		}

		v = v.Elem()
	}

	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("external IN query argument must be a slice or array, got %T", arg)
	}

	values := make([]any, 0, v.Len())
	for i := range v.Len() {
		value := v.Index(i).Interface()
		if err := validatePredicateValue(value); err != nil {
			return nil, err
		}

		values = append(values, value)
	}

	return values, nil
}

func buildPatternQueryArg(arg any, prefix, suffix string) (string, error) {
	if isNilValue(arg) {
		return "", errors.New("pattern query argument cannot be nil")
	}

	value := reflect.ValueOf(arg)
	if value.Kind() != reflect.String {
		return "", fmt.Errorf("pattern query argument must be a string, got %T", arg)
	}

	return prefix + value.String() + suffix, nil
}

func expandSlicePlaceholders(size int) string {
	if size <= slicePlaceholderCacheMax {
		return slicePlaceholderCache[size]
	}

	var builder strings.Builder
	builder.Grow(size*3 - 2)

	for i := range size {
		if i > 0 {
			builder.WriteString(", ")
		}

		builder.WriteByte('?')
	}

	return builder.String()
}

func expandNotInSlicePlaceholders(size int) string {
	if size == 0 {
		return "SELECT 1 WHERE 1 = 0"
	}

	return expandSlicePlaceholders(size)
}

func scanQueryArgState(args []any) queryArgState {
	state := queryArgState{initialized: true}

	for _, arg := range args {
		switch arg {
		case externalArgMarker:
			state.hasExternalArg = true
		case externalStartsWithMarker, externalEndsWithMarker, externalContainsMarker:
			state.hasExternalArg = true
		case externalSliceArgMarker{}:
			state.hasExternalSliceArg = true
		case externalNotInSliceArgMarker{}:
			state.hasExternalSliceArg = true
		case keywordArgMarker:
			state.hasKeywordArg = true
		}
	}

	return state
}

func buildSlicePlaceholderCache(max int) []string {
	cache := make([]string, max+1)
	cache[0] = "NULL"
	cache[1] = "?"

	for size := 2; size <= max; size++ {
		var builder strings.Builder
		builder.Grow(size*3 - 2)

		for i := 0; i < size; i++ {
			if i > 0 {
				builder.WriteString(", ")
			}

			builder.WriteByte('?')
		}

		cache[size] = builder.String()
	}

	return cache
}

func validateAnySlice(values []any) ([]any, error) {
	if len(values) == 0 {
		return nil, nil
	}

	result := make([]any, 0, len(values))
	for _, value := range values {
		if err := validatePredicateValue(value); err != nil {
			return nil, err
		}

		result = append(result, value)
	}

	return result, nil
}

func boxSlice[T any](values []T) []any {
	if len(values) == 0 {
		return nil
	}

	result := make([]any, len(values))
	for i, value := range values {
		result[i] = value
	}

	return result
}
