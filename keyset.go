package tsq

import (
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
)

// Keyset selects one page of a query by the position after the previous page
// rather than by an offset, so page 1000 costs what page 1 does and rows inserted
// meanwhile do not shift the pages. Pass it to Query.PageKeyset.
type Keyset struct {
	// Size is the page size; 0 means 20, and the runtime's WithMaxPageSize caps it.
	Size int
	// OrderBy orders the rows and defines the position. It is required, every column
	// must be selected by the query and never NULL, and the last must be a primary
	// key, which makes the position unique.
	OrderBy []OrderBy
	// After is KeysetPage.Next of the previous page; empty starts at the first row.
	After string

	// keyword is PageRequest.Keyword, which PageKeyset applies; see requestKeyword.
	keyword string
}

// KeysetPage is one page of a keyset-paged query.
type KeysetPage[T any] struct {
	Size int    `json:"size"` // Size is the page size served.
	Data []*T   `json:"data"` // Data holds the rows of the page, never nil.
	Next string `json:"next"` // Next is the Keyset.After of the following page; empty on the last.
}

// HasNext reports whether another page follows.
func (p *KeysetPage[T]) HasNext() bool { return p != nil && p.Next != "" }

// PageKeyset returns the page of rows after k.After in k.OrderBy order. The query
// must not set its own OrderBy, Limit or Offset, and must not group or combine
// rows. There is no total: counting is what keyset paging avoids; use Count when
// it is needed.
func (q *Query[O]) PageKeyset(ctx context.Context, db Executor, k Keyset, args ...Arg) (*KeysetPage[O], error) {
	return traceExecutor1(ctx, db, q.traceInfo(TraceOpPage), func(ctx context.Context) (*KeysetPage[O], error) {
		if q == nil {
			return nil, errors.New("query cannot be nil")
		}

		if q.err != nil {
			return nil, q.err
		}

		args, err := q.requestKeyword(k.keyword, args)
		if err != nil {
			return nil, err
		}

		size := Paging{Size: k.Size}.normalized(runtimeForExecutor(db).maxPage()).Size

		keys, err := q.keysetColumns(k.OrderBy)
		if err != nil {
			return nil, err
		}

		m := renderMode{paged: true, limit: size + 1}
		for _, key := range keys {
			m.order = append(m.order, key.term)
		}

		if k.After != "" {
			values, err := q.decodeCursor(keys, k.After)
			if err != nil {
				return nil, err
			}

			seek := seekCondition(keys, values)
			m.seek = &seek
		}

		_, stmts, err := q.prepare(db, args, nil, m)
		if err != nil {
			return nil, err
		}

		rows, err := q.query(ctx, db, "page", stmts[0])
		if err != nil {
			return nil, err
		}

		page := &KeysetPage[O]{Size: size, Data: rows}
		if page.Data == nil {
			page.Data = make([]*O, 0)
		}

		if len(rows) > size {
			page.Data = rows[:size]

			if page.Next, err = q.encodeCursor(keys, rows[size-1]); err != nil {
				return nil, err
			}
		}

		return page, nil
	})
}

// keysetColumn is one ordering column of a keyset and the selected column its
// value is read from.
type keysetColumn[O any] struct {
	term     orderTerm
	selected BoundColumn[O]
}

func (q *Query[O]) keysetColumns(orderBy []OrderBy) ([]keysetColumn[O], error) {
	s := &q.spec

	switch {
	case s.Limit != nil:
		return nil, errors.New("query sets Limit/Offset; PageKeyset controls paging, so drop them from the builder")
	case len(s.OrderBys) > 0:
		return nil, errors.New("query sets OrderBy; pass the order in Keyset.OrderBy instead")
	case s.grouped():
		return nil, errors.New("PageKeyset needs a query that returns table rows, without GROUP BY, aggregates, DISTINCT or set operations")
	case len(orderBy) == 0:
		return nil, errors.New("Keyset.OrderBy is required")
	}

	keys := make([]keysetColumn[O], 0, len(orderBy))

	for _, ob := range orderBy {
		if isNilValue(ob.column) {
			return nil, errors.New("Keyset.OrderBy has a nil column")
		}

		if ob.direction != orderAsc && ob.direction != orderDesc {
			return nil, fmt.Errorf("invalid order direction %q", ob.direction)
		}

		want := debugSQL(columnInfo(ob.column).sql)

		var selected BoundColumn[O]

		for _, col := range s.Selects {
			if debugSQL(columnInfo(col).sql) == want {
				selected = col
				break
			}
		}

		if selected == nil {
			return nil, fmt.Errorf("Keyset.OrderBy column %s must be selected by the query", ob.column.Name())
		}

		if null, why := s.canBeNull(columnInfo(ob.column).null); null {
			return nil, fmt.Errorf("Keyset.OrderBy column %s can be NULL (%s); a position needs values", ob.column.Name(), why)
		}

		keys = append(keys, keysetColumn[O]{term: orderTerm{expr: columnInfo(ob.column).sql, direction: ob.direction}, selected: selected})
	}

	last := orderBy[len(orderBy)-1].column.core()
	if last.err() != nil || isNilValue(last.table) || !last.plain || last.table.definition().primaryKey == nil ||
		last.table.definition().primaryKey.name != last.name {
		return nil, errors.New("the last Keyset.OrderBy column must be a primary key, so that every position is unique")
	}

	return keys, nil
}

// seekCondition matches the rows after values in the order of keys:
// k1 > v1 OR (k1 = v1 AND k2 > v2) OR ..., with < for descending terms.
func seekCondition[O any](keys []keysetColumn[O], values []any) sqlExpr {
	alternatives := make([]sqlExpr, 0, len(keys))

	for i, key := range keys {
		parts := make([]sqlExpr, 0, i+1)

		for j, prev := range keys[:i] {
			parts = append(parts, sqlJoin(prev.term.expr, sqlText(" = "), sqlValue(values[j])))
		}

		op := " > "
		if key.term.direction == orderDesc {
			op = " < "
		}

		parts = append(parts, sqlJoin(key.term.expr, sqlText(op), sqlValue(values[i])))
		alternatives = append(alternatives, sqlJoin(sqlText("("), sqlList(" AND ", parts), sqlText(")")))
	}

	return sqlJoin(sqlText("("), sqlList(" OR ", alternatives), sqlText(")"))
}

type cursor struct {
	Order  uint32            `json:"o"`
	Values []json.RawMessage `json:"v"`
}

// fingerprint identifies the ordering a cursor was made for, so a cursor from
// another order is refused instead of silently skipping rows.
func fingerprint[O any](keys []keysetColumn[O]) uint32 {
	h := fnv.New32a()

	for _, key := range keys {
		_, _ = h.Write([]byte(debugSQL(key.term.expr) + " " + string(key.term.direction) + ";"))
	}

	return h.Sum32()
}

func (q *Query[O]) encodeCursor(keys []keysetColumn[O], row *O) (string, error) {
	c := cursor{Order: fingerprint(keys)}

	for _, key := range keys {
		v := reflect.ValueOf(key.selected.core().scan(row)).Elem()
		if isNilValue(v.Interface()) {
			return "", fmt.Errorf("keyset column %s is NULL in the last row; order by columns that are never NULL", key.selected.Name())
		}

		if valuer, ok := reflect.TypeAssert[driver.Valuer](v); ok {
			value, err := valuer.Value()
			if err != nil {
				return "", fmt.Errorf("encode keyset column %s: %w", key.selected.Name(), err)
			}

			if value == nil {
				return "", fmt.Errorf("keyset column %s is NULL in the last row; order by columns that are never NULL", key.selected.Name())
			}
		}

		raw, err := json.Marshal(v.Interface())
		if err != nil {
			return "", fmt.Errorf("encode keyset column %s: %w", key.selected.Name(), err)
		}

		c.Values = append(c.Values, raw)
	}

	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (q *Query[O]) decodeCursor(keys []keysetColumn[O], after string) ([]any, error) {
	invalid := errors.New("invalid Keyset.After")

	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(after))
	if err != nil {
		return nil, invalid
	}

	var c cursor
	if err := json.Unmarshal(raw, &c); err != nil || len(c.Values) != len(keys) {
		return nil, invalid
	}

	if c.Order != fingerprint(keys) {
		return nil, errors.New("Keyset.After was made for a different OrderBy")
	}

	row := new(O)
	values := make([]any, 0, len(keys))

	for i, key := range keys {
		ptr := key.selected.core().scan(row)
		if err := json.Unmarshal(c.Values[i], ptr); err != nil {
			return nil, invalid
		}

		values = append(values, reflect.ValueOf(ptr).Elem().Interface())
	}

	return values, nil
}
