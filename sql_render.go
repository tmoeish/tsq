package tsq

import (
	"reflect"
	"strings"
	"unsafe"

	"gopkg.in/gorp.v2"
)

const (
	identifierMarkerPrefix = "__tsq_ident__("
	identifierMarkerSuffix = ")"
)

type rawQualifiedNamer interface {
	rawQualifiedName() string
}

func rawIdentifier(name string) string {
	return identifierMarkerPrefix + name + identifierMarkerSuffix
}

func rawQualifiedIdentifier(table, column string) string {
	return rawIdentifier(table) + "." + rawIdentifier(column)
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

	var (
		inSingleStr    bool
		inDoubleStr    bool
		inLineComment  bool
		inBlockComment bool
		dollarQuoteTag string
	)

	for i := 0; i < len(raw); {
		ch := raw[i]

		switch {
		case inLineComment:
			i++
			if ch == '\n' {
				inLineComment = false
			}
		case inBlockComment:
			i++
			if ch == '*' && i < len(raw) && raw[i] == '/' {
				i++
				inBlockComment = false
			}
		case inSingleStr:
			i++
			if ch == '\'' {
				if i < len(raw) && raw[i] == '\'' {
					i++
				} else {
					inSingleStr = false
				}
			}
		case inDoubleStr:
			i++
			if ch == '"' {
				if i < len(raw) && raw[i] == '"' {
					i++
				} else {
					inDoubleStr = false
				}
			}
		case dollarQuoteTag != "":
			if strings.HasPrefix(raw[i:], dollarQuoteTag) {
				i += len(dollarQuoteTag)
				dollarQuoteTag = ""
				continue
			}
			i++
		case strings.HasPrefix(raw[i:], identifierMarkerPrefix):
			return true
		default:
			if tag, ok := matchDollarQuote(raw, i); ok {
				dollarQuoteTag = tag
				i += len(tag)
				continue
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
			i++
		}
	}

	return false
}

func containsBindVarsNeedingDialect(raw string) bool {
	if !strings.Contains(raw, "?") {
		return false
	}

	var (
		inSingleStr    bool
		inDoubleStr    bool
		inLineComment  bool
		inBlockComment bool
		dollarQuoteTag string
	)

	for i := 0; i < len(raw); {
		ch := raw[i]

		switch {
		case inLineComment:
			i++
			if ch == '\n' {
				inLineComment = false
			}
		case inBlockComment:
			i++
			if ch == '*' && i < len(raw) && raw[i] == '/' {
				i++
				inBlockComment = false
			}
		case inSingleStr:
			i++
			if ch == '\'' {
				if i < len(raw) && raw[i] == '\'' {
					i++
				} else {
					inSingleStr = false
				}
			}
		case inDoubleStr:
			i++
			if ch == '"' {
				if i < len(raw) && raw[i] == '"' {
					i++
				} else {
					inDoubleStr = false
				}
			}
		case dollarQuoteTag != "":
			if strings.HasPrefix(raw[i:], dollarQuoteTag) {
				i += len(dollarQuoteTag)
				dollarQuoteTag = ""
				continue
			}
			i++
		case ch == '?':
			return true
		default:
			if tag, ok := matchDollarQuote(raw, i); ok {
				dollarQuoteTag = tag
				i += len(tag)
				continue
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
			i++
		}
	}

	return false
}

func renderSQLWithIdentifierQuoter(raw string, quoter func(string) string) string {
	if !strings.Contains(raw, identifierMarkerPrefix) {
		return raw
	}

	var builder strings.Builder
	var (
		inSingleStr    bool
		inDoubleStr    bool
		inLineComment  bool
		inBlockComment bool
		dollarQuoteTag string
	)

	builder.Grow(len(raw))

	for i := 0; i < len(raw); {
		ch := raw[i]

		switch {
		case inLineComment:
			builder.WriteByte(ch)
			i++
			if ch == '\n' {
				inLineComment = false
			}
		case inBlockComment:
			builder.WriteByte(ch)
			i++
			if ch == '*' && i < len(raw) && raw[i] == '/' {
				builder.WriteByte(raw[i])
				i++
				inBlockComment = false
			}
		case inSingleStr:
			builder.WriteByte(ch)
			i++
			if ch == '\'' {
				if i < len(raw) && raw[i] == '\'' {
					builder.WriteByte(raw[i])
					i++
				} else {
					inSingleStr = false
				}
			}
		case inDoubleStr:
			builder.WriteByte(ch)
			i++
			if ch == '"' {
				if i < len(raw) && raw[i] == '"' {
					builder.WriteByte(raw[i])
					i++
				} else {
					inDoubleStr = false
				}
			}
		case dollarQuoteTag != "":
			if strings.HasPrefix(raw[i:], dollarQuoteTag) {
				builder.WriteString(dollarQuoteTag)
				i += len(dollarQuoteTag)
				dollarQuoteTag = ""
				continue
			}
			builder.WriteByte(ch)
			i++
		case strings.HasPrefix(raw[i:], identifierMarkerPrefix):
			start := i + len(identifierMarkerPrefix)
			end := strings.Index(raw[start:], identifierMarkerSuffix)
			if end < 0 {
				builder.WriteString(raw[i:])
				i = len(raw)
				continue
			}

			builder.WriteString(quoter(raw[start : start+end]))
			i = start + end + len(identifierMarkerSuffix)
		default:
			if tag, ok := matchDollarQuote(raw, i); ok {
				builder.WriteString(tag)
				dollarQuoteTag = tag
				i += len(tag)
				continue
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
			builder.WriteByte(ch)
			i++
		}
	}

	return builder.String()
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
	}

	value := reflect.ValueOf(exec)
	if !value.IsValid() || value.Kind() != reflect.Ptr || value.IsNil() {
		return nil
	}

	elem := value.Elem()
	if !elem.IsValid() {
		return nil
	}

	field := elem.FieldByName("dbmap")
	if !field.IsValid() || field.Kind() != reflect.Ptr || field.IsNil() {
		return nil
	}

	if field.Type() != reflect.TypeOf((*gorp.DbMap)(nil)) {
		return nil
	}

	dbMap := (*gorp.DbMap)(unsafe.Pointer(field.Pointer()))
	if dbMap == nil {
		return nil
	}

	return dbMap.Dialect
}
