package tsq

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// sqlExpr is TSQ's representation of a SQL fragment before a dialect is known.
//
// A fragment is a flat list of parts. Text is emitted verbatim, identifiers are
// quoted by the executing dialect, values and parameters become placeholders, and a
// nested query renders itself in place. Nothing is ever encoded into text and parsed
// back out: the dialect decides quoting and placeholder syntax while it walks the
// parts, and features such as FULL JOIN are reported by the part that renders them
// rather than found by scanning the finished SQL.
type sqlExpr struct {
	parts []exprPart
}

type exprPartKind uint8

const (
	partText exprPartKind = iota
	partIdent
	partValue
	partParam
	partQuery
	partByDialect
	partForDialect
)

type exprPart struct {
	kind  exprPartKind
	text  string
	value any
	param *paramSpec
	query queryRenderer
	// byDialect holds the spelling of a construct the dialects disagree on.
	byDialect map[tsqdialect.Name]sqlExpr
	// forDialect spells the construct once the dialect is known, for a spelling
	// that needs the dialect itself (to quote an identifier, say). false means the
	// dialect cannot express it.
	forDialect func(d sqld.Dialect) (sqlExpr, bool)
	feature    string
}

// queryRenderer is a nested SELECT: a subquery or a CTE body.
type queryRenderer interface {
	renderQuery(r *renderer)
	correlatedTables() map[string]Table
}

func sqlText(s string) sqlExpr { return sqlExpr{parts: []exprPart{{kind: partText, text: s}}} }

func sqlIdent(name string) sqlExpr {
	return sqlExpr{parts: []exprPart{{kind: partIdent, text: name}}}
}

func sqlValue(v any) sqlExpr { return sqlExpr{parts: []exprPart{{kind: partValue, value: v}}} }

func sqlParam(p *paramSpec) sqlExpr { return sqlExpr{parts: []exprPart{{kind: partParam, param: p}}} }

func sqlQuery(q queryRenderer) sqlExpr {
	return sqlExpr{parts: []exprPart{{kind: partQuery, query: q}}}
}

// sqlByDialect renders the fragment registered for the executing dialect; feature
// names the construct in the error a dialect without an entry produces.
func sqlByDialect(feature string, choices map[tsqdialect.Name]sqlExpr) sqlExpr {
	return sqlExpr{parts: []exprPart{{kind: partByDialect, byDialect: choices, feature: feature}}}
}

// sqlForDialect defers the spelling of a construct until the dialect is known, so
// that it is built with the dialect in use rather than with one the caller picked.
func sqlForDialect(feature string, spell func(d sqld.Dialect) (sqlExpr, bool)) sqlExpr {
	return sqlExpr{parts: []exprPart{{kind: partForDialect, forDialect: spell, feature: feature}}}
}

// sqlJoin concatenates fragments.
func sqlJoin(exprs ...sqlExpr) sqlExpr {
	n := 0
	for _, e := range exprs {
		n += len(e.parts)
	}

	parts := make([]exprPart, 0, n)
	for _, e := range exprs {
		parts = append(parts, e.parts...)
	}

	return sqlExpr{parts: parts}
}

// sqlList joins fragments with sep.
func sqlList(sep string, exprs []sqlExpr) sqlExpr {
	joined := make([]sqlExpr, 0, 2*len(exprs))
	for i, e := range exprs {
		if i > 0 {
			joined = append(joined, sqlText(sep))
		}

		joined = append(joined, e)
	}

	return sqlJoin(joined...)
}

// sqlFormat builds a fragment from a format string whose %s verbs take args in order
// and whose %% is a literal percent sign. Any other verb is an error.
func sqlFormat(format string, args ...sqlExpr) (sqlExpr, error) {
	var (
		parts   []sqlExpr
		literal strings.Builder
		used    int
	)

	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			literal.WriteByte(format[i])
			continue
		}

		if i+1 >= len(format) {
			return sqlExpr{}, errors.New("format ends with a lone %")
		}

		i++

		switch format[i] {
		case '%':
			literal.WriteByte('%')
		case 's':
			if used >= len(args) {
				return sqlExpr{}, fmt.Errorf("format %q has more %%s verbs than arguments (%d)", format, len(args))
			}

			parts = append(parts, sqlText(literal.String()), args[used])
			literal.Reset()

			used++
		default:
			return sqlExpr{}, fmt.Errorf("format %q uses %%%c; only %%s and %%%% are supported", format, format[i])
		}
	}

	if used != len(args) {
		return sqlExpr{}, fmt.Errorf("format %q has %d %%s verbs but %d arguments", format, used, len(args))
	}

	parts = append(parts, sqlText(literal.String()))

	return sqlJoin(parts...), nil
}

// correlated returns the outer tables that nested queries in e depend on.
func (e sqlExpr) correlated() map[string]Table {
	var tables map[string]Table

	for _, part := range e.parts {
		if part.kind != partQuery {
			continue
		}

		for name, table := range part.query.correlatedTables() {
			if tables == nil {
				tables = make(map[string]Table)
			}

			tables[name] = table
		}
	}

	return tables
}

// renderer walks fragments for one dialect and produces a statement template.
type renderer struct {
	dialect sqld.Dialect
	stmt    statement
	text    strings.Builder
	err     error
}

func newRenderer(d sqld.Dialect) *renderer {
	return &renderer{dialect: d}
}

func (r *renderer) fail(err error) {
	if r.err == nil && err != nil {
		r.err = err
	}
}

func (r *renderer) writeText(s string) { r.text.WriteString(s) }

func (r *renderer) writeIdent(name string) {
	if err := validateIdentifierForDialect(name, r.dialect); err != nil {
		r.fail(err)
		return
	}

	r.text.WriteString(r.dialect.QuoteIdent(name))
}

func (r *renderer) flush() {
	if r.text.Len() == 0 {
		return
	}

	r.stmt.chunks = append(r.stmt.chunks, chunk{text: r.text.String()})
	r.text.Reset()
}

func (r *renderer) writeValue(v any) {
	r.flush()
	r.stmt.chunks = append(r.stmt.chunks, chunk{value: v, hasValue: true})
}

func (r *renderer) writeParam(p *paramSpec) {
	r.flush()
	r.stmt.chunks = append(r.stmt.chunks, chunk{param: p})
}

// require records that the statement uses capability and fails the render when the
// dialect does not support it.
func (r *renderer) require(capability tsqdialect.Capability) {
	r.fail(sqld.ValidateCapability(r.dialect, capability))
}

func (r *renderer) write(e sqlExpr) {
	for _, part := range e.parts {
		switch part.kind {
		case partText:
			r.writeText(part.text)
		case partIdent:
			r.writeIdent(part.text)
		case partValue:
			r.writeValue(part.value)
		case partParam:
			r.writeParam(part.param)
		case partQuery:
			part.query.renderQuery(r)
		case partForDialect:
			choice, ok := part.forDialect(r.dialect)
			if !ok {
				r.fail(fmt.Errorf("%s is not supported on %s", part.feature, r.dialect.Name()))
				continue
			}

			r.write(choice)
		case partByDialect:
			choice, ok := part.byDialect[r.dialect.Name()]
			if !ok {
				r.fail(fmt.Errorf("%s is not supported on %s", part.feature, r.dialect.Name()))
				continue
			}

			r.write(choice)
		}
	}
}

func (r *renderer) finish() (*statement, error) {
	r.flush()

	if r.err != nil {
		return nil, r.err
	}

	stmt := r.stmt

	return &stmt, nil
}

// statement is a rendered template: dialect-specific text with the placeholders
// still open, because how many a list parameter needs is only known at execution.
type statement struct {
	chunks []chunk
}

type chunk struct {
	text     string
	value    any
	hasValue bool
	param    *paramSpec
}

// params returns the distinct parameters of the statement in first-use order.
func (s *statement) params() []*paramSpec {
	var result []*paramSpec

	for _, c := range s.chunks {
		if c.param != nil && !slices.Contains(result, c.param) {
			result = append(result, c.param)
		}
	}

	return result
}

// assemble produces executable SQL and its arguments from the template and the
// bound arguments.
func (s *statement) assemble(d sqld.Dialect, bound argSet) (string, []any, error) {
	var (
		sql  strings.Builder
		args []any
	)

	placeholder := func(v any) {
		args = append(args, bindValue(v))
		sql.WriteString(d.Placeholder(len(args) - 1))
	}

	for _, c := range s.chunks {
		switch {
		case c.hasValue:
			placeholder(c.value)
		case c.param != nil:
			value, err := bound.value(c.param)
			if err != nil {
				return "", nil, err
			}

			if err := c.param.write(value, placeholder, &sql); err != nil {
				return "", nil, err
			}
		default:
			sql.WriteString(c.text)
		}
	}

	return sql.String(), args, nil
}
