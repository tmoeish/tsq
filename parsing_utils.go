package tsq

import (
	"strings"

	"github.com/juju/errors"
)

// ParsingUtils provides shared utilities for SQL and query parsing
// This consolidates common parsing patterns used throughout the package

// ParseIntValue safely parses a string to int with bounds checking
// Returns 0 if parsing fails
func ParseIntValue(s string, minVal, maxVal int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	var result int64

	_, err := readInt(s, &result)
	if err != nil {
		return 0
	}

	if result < int64(minVal) {
		return minVal
	}

	if result > int64(maxVal) {
		return maxVal
	}

	return int(result)
}

// readInt is a helper for parsing integers from strings
func readInt(s string, result *int64) (string, error) {
	var sign bool
	if len(s) > 0 && (s[0] == '-' || s[0] == '+') {
		sign = s[0] == '-'
		s = s[1:]
	}

	if len(s) == 0 {
		return "", errors.New("no digits found")
	}

	x := int64(0)

	for i := 0; i < len(s); i++ {
		d := s[i]
		if d < '0' || d > '9' {
			if i == 0 {
				return "", errors.Errorf("invalid digit: %c", d)
			}
			s = s[i:]

			break
		}
		x = x*10 + int64(d-'0')
	}

	if sign {
		x = -x
	}

	*result = x

	return s, nil
}

// SplitAndTrim splits a string by separator and trims whitespace from each part
// Filters out empty strings after trimming
func SplitAndTrim(s, sep string) []string {
	if s == "" {
		return []string{}
	}

	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}

	return result
}

// TrimLower returns a trimmed lowercase version of a string
func TrimLower(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// IsValidIdentifier checks if a string is a valid SQL identifier
// Valid identifiers contain alphanumeric, underscore, and don't start with digit
func IsValidIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	if s[0] >= '0' && s[0] <= '9' {
		return false
	}

	for _, ch := range s {
		isLetter := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
		isDigit := ch >= '0' && ch <= '9'
		isUnderscore := ch == '_'

		if !isLetter && !isDigit && !isUnderscore {
			return false
		}
	}

	return true
}

// ParseKeyValue parses a key=value string into components
// Returns (key, value, error)
func ParseKeyValue(s string) (string, string, error) {
	s = strings.TrimSpace(s)

	before, after, ok := strings.Cut(s, "=")
	if !ok {
		return "", "", errors.Errorf("no '=' found in %q", s)
	}

	key := strings.TrimSpace(before)
	value := strings.TrimSpace(after)

	if key == "" {
		return "", "", errors.New("empty key")
	}

	return key, value, nil
}

// ContainsSQL checks if a string might contain SQL-like constructs
func ContainsSQL(s string) bool {
	keywords := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE"}

	upper := strings.ToUpper(s)
	for _, kw := range keywords {
		if strings.Contains(upper, kw) {
			return true
		}
	}

	return false
}
