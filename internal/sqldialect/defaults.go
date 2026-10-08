package sqldialect

import (
	"fmt"
	"math/big"
	"strings"
	"time"
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

	// No default and the empty string '' are two things: every engine reports
	// them apart (NULL against ''), and compared as one, a column that dropped its
	// '' kept it, and a later change of its type met it ("default for column
	// cannot be cast automatically").
	if (left == "") != (right == "") {
		return false
	}

	if IsCurrentTime(left) && IsCurrentTime(right) {
		return isUTC(left) == isUTC(right)
	}

	a, aQuoted := normalizeDefaultLiteral(left)
	b, bQuoted := normalizeDefaultLiteral(right)

	if aQuoted && bQuoted {
		return a == b || sameTimeLiteral(a, b)
	}

	if x, ok := defaultNumber(a); ok {
		if y, ok := defaultNumber(b); ok {
			return x.Cmp(y) == 0
		}
	}

	return strings.EqualFold(a, b)
}

// sameTimeLiteral reports two literals that are one time: MySQL reads the default
// '2020-01-02 03:04:05' of a DATETIME(6) back with its six zeros.
func sameTimeLiteral(a, b string) bool {
	x, okX := parseTimeLiteral(a)
	y, okY := parseTimeLiteral(b)

	return okX && okY && x.Equal(y)
}

func parseTimeLiteral(value string) (time.Time, bool) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02"} {
		if t, err := time.Parse(layout, value); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
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
		// SQLite keeps a time as text, in the spelling the drivers write: with the
		// zone. MySQL takes a zone in a literal only within the range of TIMESTAMP:
		// year 1 with an offset is an error under an explicit session time zone and
		// is stored as 0000-00-00, without a word, under the default one, after
		// which every ALTER that copies the table fails on the row.
		if d.Name() == SQLite {
			return "'0001-01-01 00:00:00+00:00'", true
		}

		return "'0001-01-01 00:00:00'", true
	}

	return "", false
}

// AddNeedsRebuild reports a column SQLite cannot ADD to a table with rows, so the
// table is rebuilt with it instead: a default that is not a constant
// (CURRENT_TIMESTAMP, an expression), NOT NULL without a default, which the
// other dialects add with the zero value as a default they then drop and SQLite
// cannot (it has no DROP DEFAULT), or a stored generated column. The generator
// and the runtime share it.
func AddNeedsRebuild(d Dialect, column ColumnSpec) bool {
	if d.AlterMode() != AlterRebuild || column.PrimaryKey || column.AutoIncrement {
		return false
	}

	// ALTER TABLE ADD COLUMN takes a generated column only as VIRTUAL, and the one
	// form every dialect has, which is what TSQ declares, is STORED.
	if column.Fill == FillGenerated {
		return true
	}

	nonConstant := IsCurrentTime(column.Default) || strings.HasPrefix(strings.TrimSpace(column.Default), "(")

	return nonConstant || (!column.Type.Nullable && column.Default == "")
}

// AddColumnSQL adds column to a table that may hold rows, on a dialect that
// alters in place. A NOT NULL column without a default is refused there
// (PostgreSQL for every type, MySQL for a time), so the rows present get the
// type's zero value, the value the Go field of a row that never set it holds: the
// column is added with that default, which is then dropped, leaving the column as
// declared. A column of an explicit type: has no zero value TSQ knows and is added
// as it is.
func AddColumnSQL(d Dialect, table string, column ColumnSpec) ([]string, error) {
	quotedTable := d.QuoteIdent(table)

	filled := column
	if zero, ok := ZeroLiteral(d, column.Type); ok && fillsNewColumn(column) {
		filled.Default = zero
	}

	definition, err := ColumnDefinitionSQL(d, filled)
	if err != nil {
		return nil, err
	}

	statements := []string{fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", quotedTable, definition)}
	if filled.Default != column.Default {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, d.QuoteIdent(column.Name)))
	}

	return statements, nil
}

// fillsNewColumn reports a column whose existing rows need a value when it is
// added: NOT NULL, with nothing that gives it one.
func fillsNewColumn(column ColumnSpec) bool {
	return !column.Type.Nullable && column.Default == "" && column.Generated == "" &&
		column.Fill != FillGenerated && !column.PrimaryKey && !column.AutoIncrement
}

// NewColumnFill is what the rows present get in a new NOT NULL column without a
// default, for the note a migration writes; it is empty when nothing is filled.
func NewColumnFill(d Dialect, column ColumnSpec) string {
	if !fillsNewColumn(column) {
		return ""
	}

	zero, _ := ZeroLiteral(d, column.Type)

	return zero
}

// Retype is how a SQLite rebuild carries the values of a column into another type.
// SQLite stores what it is given under any declared type, so a rebuild that copied
// a column as it was "succeeded" over values no field of the new type reads: 1.5 in
// an integer column, 2 in a boolean one, 'Hello' in either. MySQL and PostgreSQL
// convert such a value where their casts do and refuse the change where they do
// not, and a rebuild does the same.
type Retype uint8

const (
	// RetypeAsIs is a change every stored value survives as it is.
	RetypeAsIs Retype = iota
	// RetypeConvert is a change every value converts under, by SQLiteRetypeSource.
	RetypeConvert
	// RetypeMayFail is a change some values may not convert under: text into a
	// number, a boolean or a time.
	RetypeMayFail
)

// SQLiteRetype classifies the change of a column from before to after. A raw
// type: on either side is the declaration's own business and is carried as it is.
func SQLiteRetype(before, after ColumnType) Retype {
	if before.RawType != "" || after.RawType != "" || before.Kind == after.Kind {
		return RetypeAsIs
	}

	numeric := before.Kind == KindInt || before.Kind == KindFloat || before.Kind == KindBool

	switch after.Kind {
	case KindInt:
		switch before.Kind {
		case KindBool:
			return RetypeAsIs
		case KindFloat:
			return RetypeConvert
		}

		return RetypeMayFail
	case KindFloat:
		if numeric {
			return RetypeAsIs
		}

		return RetypeMayFail
	case KindBool:
		if numeric {
			return RetypeConvert
		}

		return RetypeMayFail
	case KindTime:
		return RetypeMayFail
	default:
		// Text and bytes hold anything.
		return RetypeAsIs
	}
}

// SQLiteRetypeSource is what a rebuild copies from source into a column that
// becomes after: a fraction is rounded into an integer as MySQL rounds it, and any
// number but zero is true, as PostgreSQL's USING c <> 0 has it. It goes by the
// value's own storage class, so it is right whatever the old column was declared.
func SQLiteRetypeSource(source string, after ColumnType) string {
	if after.RawType != "" {
		return source
	}

	switch after.Kind {
	case KindInt:
		return fmt.Sprintf("CASE typeof(%s) WHEN 'real' THEN CAST(ROUND(%s) AS INTEGER) ELSE %s END", source, source, source)
	case KindBool:
		return fmt.Sprintf("CASE WHEN typeof(%s) IN ('integer', 'real') THEN %s <> 0 ELSE %s END", source, source, source)
	default:
		return source
	}
}

// SQLiteMisfit is a condition true of a value stored in column that a field of type
// t does not read, and what such a value is not, for the error; both are empty for
// a type that reads anything. It is asked of the rebuilt table, after SQLite's
// affinity has converted what it converts ('12' into an integer column is 12).
func SQLiteMisfit(column string, t ColumnType) (condition, kind string) {
	if t.RawType != "" {
		return "", ""
	}

	switch t.Kind {
	case KindInt:
		return fmt.Sprintf("typeof(%s) NOT IN ('integer', 'null')", column), "an integer"
	case KindFloat:
		return fmt.Sprintf("typeof(%s) NOT IN ('real', 'integer', 'null')", column), "a number"
	case KindBool:
		return fmt.Sprintf("typeof(%s) <> 'null' AND (typeof(%s) <> 'integer' OR %s NOT IN (0, 1))", column, column, column), "a boolean"
	case KindTime:
		return fmt.Sprintf("typeof(%s) <> 'null' AND (typeof(%s) <> 'text' OR %s NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*')",
			column, column, column), "a time"
	default:
		return "", ""
	}
}
