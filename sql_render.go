package tsq

import (
	"encoding/base64"
	"strings"

	"gopkg.in/gorp.v2"
)

const (
	identifierMarkerPrefix         = "__tsq_ident__("
	identifierMarkerSuffix         = ")"
	identifierMarkerEncodingPrefix = "b64:"
)

type rawQualifiedNamer interface {
	rawQualifiedName() string
}

type dialectProvider interface {
	TSQDialect() gorp.Dialect
}

type schemaTabler interface {
	Schema() string
}

func rawIdentifier(name string) string {
	return identifierMarkerPrefix +
		identifierMarkerEncodingPrefix +
		base64.RawURLEncoding.EncodeToString([]byte(name)) +
		identifierMarkerSuffix
}

func rawQualifiedIdentifier(table, column string) string {
	return rawIdentifier(table) + "." + rawIdentifier(column)
}

func rawTableSourceIdentifier(table Table) string {
	if table == nil {
		return ""
	}

	tableName := physicalTableName(table)
	if schemaTable, ok := table.(schemaTabler); ok && strings.TrimSpace(schemaTable.Schema()) != "" {
		return rawIdentifier(schemaTable.Schema()) + "." + rawIdentifier(tableName)
	}

	return rawIdentifier(tableName)
}

func rawTableQualifierIdentifier(table Table) string {
	if table == nil {
		return ""
	}

	if alias := tableAliasName(table); alias != "" {
		return rawIdentifier(alias)
	}

	return rawTableSourceIdentifier(table)
}

func rawTableIdentifier(table Table) string {
	if table == nil {
		return ""
	}

	source := rawTableSourceIdentifier(table)
	if alias := tableAliasName(table); alias != "" {
		return source + " AS " + rawIdentifier(alias)
	}

	return source
}

func rawQualifiedIdentifierForTable(table Table, column string) string {
	return rawTableQualifierIdentifier(table) + "." + rawIdentifier(column)
}

func rawColumnQualifiedName(col Column) string {
	if raw, ok := col.(rawQualifiedNamer); ok {
		return raw.rawQualifiedName()
	}

	return col.QualifiedName()
}

func renderCanonicalSQL(raw string) string {
	return renderSQLWithIdentifierQuoter(raw, canonicalQuoteIdentifier)
}

func renderSQLForExecutor(exec gorp.SqlExecutor, raw string) string {
	return renderSQLForDialect(raw, dialectForExecutor(exec))
}

func renderSQLForDialect(raw string, dialect gorp.Dialect) string {
	rendered := renderSQLWithIdentifierQuoter(raw, func(name string) string {
		if dialect == nil {
			return canonicalQuoteIdentifier(name)
		}

		return dialect.QuoteField(name)
	})

	if dialect == nil {
		return rendered
	}

	return rewriteBindVars(rendered, dialect)
}

func containsIdentifierMarkersNeedingRender(raw string) bool {
	if !strings.Contains(raw, identifierMarkerPrefix) {
		return false
	}

	return walkSQL(raw, nil, func(source string, i int, _ *strings.Builder) (int, bool, bool) {
		return len(identifierMarkerPrefix), strings.HasPrefix(source[i:], identifierMarkerPrefix), strings.HasPrefix(source[i:], identifierMarkerPrefix)
	})
}

func containsBindVarsNeedingDialect(raw string) bool {
	if !strings.Contains(raw, "?") {
		return false
	}

	return walkSQL(raw, nil, func(source string, i int, _ *strings.Builder) (int, bool, bool) {
		return 1, source[i] == '?', source[i] == '?'
	})
}

func renderSQLWithIdentifierQuoter(raw string, quoter func(string) string) string {
	if !strings.Contains(raw, identifierMarkerPrefix) {
		return raw
	}

	var builder strings.Builder
	builder.Grow(len(raw))
	walkSQL(raw, &builder, func(source string, i int, out *strings.Builder) (int, bool, bool) {
		if !strings.HasPrefix(source[i:], identifierMarkerPrefix) {
			return 0, false, false
		}

		start := i + len(identifierMarkerPrefix)
		end := strings.Index(source[start:], identifierMarkerSuffix)
		if end < 0 {
			out.WriteString(source[i:])
			return len(source) - i, true, true
		}

		out.WriteString(quoter(decodeIdentifierMarker(source[start : start+end])))
		return len(identifierMarkerPrefix) + end + len(identifierMarkerSuffix), true, false
	})

	return builder.String()
}

func walkSQL(
	raw string,
	builder *strings.Builder,
	handlePlain func(raw string, i int, builder *strings.Builder) (advance int, handled bool, stop bool),
) bool {
	var (
		inSingleStr    bool
		inDoubleStr    bool
		inLineComment  bool
		inBlockComment bool
		dollarQuoteTag string
	)

	writeByte := func(ch byte) {
		if builder != nil {
			builder.WriteByte(ch)
		}
	}

	writeString := func(value string) {
		if builder != nil {
			builder.WriteString(value)
		}
	}

	for i := 0; i < len(raw); {
		ch := raw[i]

		switch {
		case inLineComment:
			writeByte(ch)
			i++
			if ch == '\n' {
				inLineComment = false
			}
		case inBlockComment:
			writeByte(ch)
			i++
			if ch == '*' && i < len(raw) && raw[i] == '/' {
				writeByte(raw[i])
				i++
				inBlockComment = false
			}
		case inSingleStr:
			writeByte(ch)
			i++
			if ch == '\'' {
				if i < len(raw) && raw[i] == '\'' {
					writeByte(raw[i])
					i++
				} else {
					inSingleStr = false
				}
			}
		case inDoubleStr:
			writeByte(ch)
			i++
			if ch == '"' {
				if i < len(raw) && raw[i] == '"' {
					writeByte(raw[i])
					i++
				} else {
					inDoubleStr = false
				}
			}
		case dollarQuoteTag != "":
			if strings.HasPrefix(raw[i:], dollarQuoteTag) {
				writeString(dollarQuoteTag)
				i += len(dollarQuoteTag)
				dollarQuoteTag = ""
				continue
			}

			writeByte(ch)
			i++
		default:
			if tag, ok := matchDollarQuote(raw, i); ok {
				writeString(tag)
				dollarQuoteTag = tag
				i += len(tag)
				continue
			}

			if handlePlain != nil {
				advance, handled, stop := handlePlain(raw, i, builder)
				if handled {
					if stop {
						return true
					}
					if advance <= 0 {
						advance = 1
					}
					i += advance
					continue
				}
			}

			switch ch {
			case '\'':
				inSingleStr = true
			case '"':
				inDoubleStr = true
			case '-':
				if i+1 < len(raw) && raw[i+1] == '-' {
					inLineComment = true
				}
			case '/':
				if i+1 < len(raw) && raw[i+1] == '*' {
					inBlockComment = true
				}
			}

			writeByte(ch)
			i++
		}
	}

	return false
}

func decodeIdentifierMarker(payload string) string {
	encoded, ok := strings.CutPrefix(payload, identifierMarkerEncodingPrefix)
	if !ok {
		return payload
	}

	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return payload
	}

	return string(decoded)
}

func canonicalQuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func rewriteBindVars(sql string, dialect gorp.Dialect) string {
	if dialect == nil || !strings.Contains(sql, "?") {
		return sql
	}

	if dialect.BindVar(0) == "?" {
		return sql
	}

	var (
		builder        strings.Builder
		bindIndex      int
		inSingleStr    bool
		inDoubleStr    bool
		inLineComment  bool
		inBlockComment bool
		dollarQuoteTag string
	)

	builder.Grow(len(sql) + 8)

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch {
		case inLineComment:
			builder.WriteByte(ch)

			if ch == '\n' {
				inLineComment = false
			}
		case inBlockComment:
			builder.WriteByte(ch)

			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				builder.WriteByte(sql[i+1])

				i++
				inBlockComment = false
			}
		case inSingleStr:
			builder.WriteByte(ch)

			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					builder.WriteByte(sql[i+1])

					i++
				} else {
					inSingleStr = false
				}
			}
		case inDoubleStr:
			builder.WriteByte(ch)

			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					builder.WriteByte(sql[i+1])

					i++
				} else {
					inDoubleStr = false
				}
			}
		case dollarQuoteTag != "":
			if strings.HasPrefix(sql[i:], dollarQuoteTag) {
				builder.WriteString(dollarQuoteTag)
				i += len(dollarQuoteTag) - 1
				dollarQuoteTag = ""

				continue
			}

			builder.WriteByte(ch)
		default:
			if tag, ok := matchDollarQuote(sql, i); ok {
				builder.WriteString(tag)
				dollarQuoteTag = tag
				i += len(tag) - 1

				continue
			}

			switch ch {
			case '\'':
				inSingleStr = true

				builder.WriteByte(ch)
			case '"':
				inDoubleStr = true

				builder.WriteByte(ch)
			case '-':
				if i+1 < len(sql) && sql[i+1] == '-' {
					inLineComment = true
				}

				builder.WriteByte(ch)
			case '/':
				if i+1 < len(sql) && sql[i+1] == '*' {
					inBlockComment = true
				}

				builder.WriteByte(ch)
			case '?':
				builder.WriteString(dialect.BindVar(bindIndex))

				bindIndex++
			default:
				builder.WriteByte(ch)
			}
		}
	}

	return builder.String()
}

func matchDollarQuote(sql string, start int) (string, bool) {
	if start >= len(sql) || sql[start] != '$' {
		return "", false
	}

	if start+1 < len(sql) && sql[start+1] == '$' {
		tag := "$$"
		return tag, strings.Contains(sql[start+len(tag):], tag)
	}

	if start+1 >= len(sql) || !isDollarQuoteTagStart(sql[start+1]) {
		return "", false
	}

	end := start + 2
	for end < len(sql) && isDollarQuoteTagChar(sql[end]) {
		end++
	}

	if end >= len(sql) || sql[end] != '$' {
		return "", false
	}

	tag := sql[start : end+1]

	return tag, strings.Contains(sql[start+len(tag):], tag)
}

func isDollarQuoteTagStart(ch byte) bool {
	return ch == '_' || ('A' <= ch && ch <= 'Z') || ('a' <= ch && ch <= 'z')
}

func isDollarQuoteTagChar(ch byte) bool {
	return isDollarQuoteTagStart(ch) || ('0' <= ch && ch <= '9')
}

func dialectForExecutor(exec gorp.SqlExecutor) gorp.Dialect {
	switch tx := exec.(type) {
	case *gorp.DbMap:
		return tx.Dialect
	case dialectProvider:
		return tx.TSQDialect()
	}

	return nil
}
