package sqldialect

import (
	"math/big"
	"strings"
)

// SameDefault compares two spellings of a column default. Two quoted literals
// compare exactly, so 'Active' and 'active' differ; anything else compares without
// case, since keywords (CURRENT_TIMESTAMP) are spelled either way and MySQL reads a
// string default back without its quotes.
//
// Numbers compare by value and booleans as 1 and 0: MySQL reads a declared true
// back as 1 and a decimal 0 as 0.00, which used to ask for the same ALTER on every
// boot.
//
// Pass the declared side as the DDL spells it (DefaultSQL): an expression default
// compares without its parentheses, and two current-time defaults are the same
// only if both are UTC or neither is. MySQL reports CURRENT_TIMESTAMP(6) for a
// DATETIME(6) column and PostgreSQL timezone('UTC'::text, CURRENT_TIMESTAMP) for
// the UTC expression; a plain CURRENT_TIMESTAMP there stores the session's local
// time, which is what a table created before TSQ wrote UTC defaults still has.
func SameDefault(left, right string) bool {
	left, right = unparenthesize(left), unparenthesize(right)

	if IsCurrentTime(left) && IsCurrentTime(right) {
		return isUTC(left) == isUTC(right)
	}

	a, aQuoted := normalizeDefaultLiteral(left)
	b, bQuoted := normalizeDefaultLiteral(right)

	if aQuoted && bQuoted {
		return a == b
	}

	if x, ok := defaultNumber(a); ok {
		if y, ok := defaultNumber(b); ok {
			return x.Cmp(y) == 0
		}
	}

	return strings.EqualFold(a, b)
}

// defaultNumber reads a default as an exact number, true and false included.
func defaultNumber(value string) (*big.Rat, bool) {
	switch strings.ToLower(value) {
	case "true":
		value = "1"
	case "false":
		value = "0"
	}

	return new(big.Rat).SetString(value)
}

// normalizeDefaultLiteral makes two spellings of the same default comparable: a
// declared 'USD' reads back as USD on MySQL and as 'USD'::character varying on
// PostgreSQL, and comparing those verbatim asks to set the default on every boot.
// It drops a cast outside the literal (a '::' inside one is text), unquotes a
// literal and reports whether it was one.
func normalizeDefaultLiteral(value string) (string, bool) {
	value = strings.TrimSpace(value)

	quoted := false

	for i := 0; i < len(value); i++ {
		switch {
		case value[i] == '\'':
			quoted = !quoted
		case !quoted && strings.HasPrefix(value[i:], "::"):
			value = strings.TrimSpace(value[:i])
			i = len(value)
		}
	}

	if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return strings.ReplaceAll(value[1:len(value)-1], "''", "'"), true
	}

	return value, false
}

// unparenthesize drops parentheses that wrap the whole default.
func unparenthesize(value string) string {
	value = strings.TrimSpace(value)

	for len(value) >= 2 && value[0] == '(' && value[len(value)-1] == ')' {
		depth := 0
		wraps := true

		for i := 0; i < len(value)-1; i++ {
			switch value[i] {
			case '(':
				depth++
			case ')':
				depth--
			}

			if depth == 0 {
				wraps = false

				break
			}
		}

		if !wraps {
			break
		}

		value = strings.TrimSpace(value[1 : len(value)-1])
	}

	return value
}

func isUTC(value string) bool {
	return strings.Contains(strings.ToLower(value), "utc")
}

// NullFill is what a column that becomes NOT NULL writes into the rows holding
// NULL first, so the change cannot fail on them: its default, or its type's zero
// value. It is empty when neither is known (an explicit type: without a default).
func NullFill(d Dialect, before Column, after ColumnSpec) string {
	if !before.Type.Nullable || after.Type.Nullable || before.PrimaryKey {
		return ""
	}

	if after.Default != "" {
		return DefaultSQL(d, after)
	}

	zero, _ := ZeroLiteral(d, after.Type)

	return zero
}

// ZeroLiteral is the Go zero value of a column type as a SQL literal of the
// dialect, which a migration writes into the rows a new NOT NULL column is added to
// and into the NULLs of a column that becomes NOT NULL. A column of an explicit
// type: has none TSQ knows.
func ZeroLiteral(d Dialect, t ColumnType) (string, bool) {
	if t.RawType != "" {
		return "", false
	}

	postgres := d.Name() == Postgres

	switch t.Kind {
	case KindString:
		return "''", true
	case KindInt, KindFloat:
		return "0", true
	case KindBool:
		if postgres {
			return "FALSE", true
		}

		return "0", true
	case KindBytes:
		if postgres {
			return "''", true
		}

		return "X''", true
	case KindTime:
		if postgres {
			return "'0001-01-01 00:00:00'", true
		}

		return "'0001-01-01 00:00:00+00:00'", true
	}

	return "", false
}
