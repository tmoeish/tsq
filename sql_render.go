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

func renderSQLWithIdentifierQuoter(raw string, quoter func(string) string) string {
	if !strings.Contains(raw, identifierMarkerPrefix) {
		return raw
	}

	var builder strings.Builder
	var (
		inSingleStr bool
		inDoubleStr bool
	)

	builder.Grow(len(raw))

	for i := 0; i < len(raw); {
		ch := raw[i]

		switch {
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
			switch ch {
			case '\'':
				inSingleStr = true
			case '"':
				inDoubleStr = true
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
		builder     strings.Builder
		bindIndex   int
		inSingleStr bool
		inDoubleStr bool
	)

	builder.Grow(len(sql) + 8)

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch {
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
		default:
			switch ch {
			case '\'':
				inSingleStr = true
				builder.WriteByte(ch)
			case '"':
				inDoubleStr = true
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
