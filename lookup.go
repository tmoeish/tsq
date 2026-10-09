package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

// lazyQuery builds a query the first time it is needed and keeps it, so its
// per-dialect render cache survives between calls.
type lazyQuery[R any] struct {
	once sync.Once
	q    *Query[R]
	err  error
}

func (l *lazyQuery[R]) get(build func() (*Query[R], error)) (*Query[R], error) {
	l.once.Do(func() { l.q, l.err = build() })

	return l.q, l.err
}

// scope indexes the queries cached per soft-delete scope.
func (t *TableOf[R, K]) scope() int {
	if t.includeDeleted {
		return 1
	}

	return 0
}

// unaliased is t under its own name, which is how lookups read it.
func (t *TableOf[R, K]) unaliased() *TableOf[R, K] {
	if t.alias == "" {
		return t
	}

	return t.As("")
}

func (t *TableOf[R, K]) primaryKey() (Column[R, K], error) {
	if err := t.Err(); err != nil {
		return nil, err
	}

	if t.keys.pk == nil {
		return nil, fmt.Errorf("table %s has no primary key", t.def.name)
	}

	return t.keys.pk, nil
}

// Query returns the query that reads every row of the table: every column, and
// keyword search over the declared search columns, so tsq.Keyword works with it.
// On a table with deleted_at it leaves deleted rows out unless t is WithDeleted.
// Write queries with conditions or an order with the builder. An invalid table
// definition is reported when the query runs.
func (t *TableOf[R, K]) Query() *Query[R] {
	if err := t.Err(); err != nil {
		return &Query[R]{err: err}
	}

	t = t.unaliased()

	q, err := t.keys.all[t.scope()].get(func() (*Query[R], error) {
		if err := t.Err(); err != nil {
			return nil, err
		}

		stage := Select(t.Columns()...).From(t)
		if search := t.searchColumns(); len(search) > 0 {
			return stage.Search(search[0], search[1:]...).Build()
		}

		return stage.Build()
	})
	if err != nil {
		return &Query[R]{err: err}
	}

	return q
}

// Get reads the row whose primary key is key, and fails with an error wrapping
// sql.ErrNoRows when there is none. On a table with deleted_at a deleted row is
// not found unless t is WithDeleted.
func (t *TableOf[R, K]) Get(ctx context.Context, db Executor, key K) (*R, error) {
	q, pk, err := t.byKey()
	if err != nil {
		return nil, err
	}

	return q.Get(ctx, db, pk.Bind(key))
}

// Find is Get that returns nil, nil when there is no such row.
func (t *TableOf[R, K]) Find(ctx context.Context, db Executor, key K) (*R, error) {
	q, pk, err := t.byKey()
	if err != nil {
		return nil, err
	}

	return q.Find(ctx, db, pk.Bind(key))
}

func (t *TableOf[R, K]) byKey() (*Query[R], Column[R, K], error) {
	t = t.unaliased()

	pk, err := t.primaryKey()
	if err != nil {
		return nil, nil, err
	}

	q, err := t.keys.get[t.scope()].get(func() (*Query[R], error) {
		return Select(t.Columns()...).From(t).Where(pk.EQ(pk.Param())).Build()
	})

	return q, pk, err
}

// Fetch reads the rows whose primary keys are keys, in the order given, a row
// once per time its key is given. Any number of keys works: they are split to
// fit the dialect's bind parameter limit. It fails with an error wrapping
// sql.ErrNoRows, naming the missing keys, when any key has no row.
func (t *TableOf[R, K]) Fetch(ctx context.Context, db Executor, keys ...K) ([]*R, error) {
	t = t.unaliased()

	pk, err := t.primaryKey()
	if err != nil {
		return nil, err
	}

	list, err := t.keys.list[t.scope()].get(func() (*Query[R], error) {
		return Select(t.Columns()...).From(t).Where(pk.In(pk.ListParam())).Build()
	})
	if err != nil {
		return nil, err
	}

	return fetchInOrder(ctx, db, t, list, pk, keys, nil)
}

// FetchBy reads the rows whose col is one of values, in the order given, as
// Fetch does for the primary key. col should be unique, alone or together with
// the columns where fixes; generated GetByX and FetchByX methods call it for each
// unique index. where takes conditions with fixed values, such as
// t.OrgID.EQ(tsq.Val(org)).
//
// A value matches the row the database matches, under its collation: on a
// case-insensitive column, "ada" finds the row holding "Ada".
func (t *TableOf[R, K]) FetchBy[T comparable](ctx context.Context, db Executor, col Column[R, T], values []T, where ...Condition) ([]*R, error) {
	t = t.unaliased()

	col, err := ownColumn(t, col)
	if err != nil {
		return nil, err
	}

	list, err := t.byQuery(col, true, where, func() (*Query[R], error) {
		return Select(t.Columns()...).From(t).Where(col.In(col.ListParam()), where...).Build()
	})
	if err != nil {
		return nil, err
	}

	return fetchInOrder(ctx, db, t, list, col, values, where)
}

// GetBy reads the row whose col is value, and fails with an error wrapping
// sql.ErrNoRows when there is none. col should be unique, alone or together with
// the columns where fixes; the generated GetByX methods call it for each unique
// index. Without where the query is built once and reused.
func (t *TableOf[R, K]) GetBy[T comparable](ctx context.Context, db Executor, col Column[R, T], value T, where ...Condition) (*R, error) {
	q, col, err := oneBy(t, col, where)
	if err != nil {
		return nil, err
	}

	return q.Get(ctx, db, col.Bind(value))
}

// FindBy is GetBy that returns nil, nil when there is no such row; the generated
// FindByX methods call it.
func (t *TableOf[R, K]) FindBy[T comparable](ctx context.Context, db Executor, col Column[R, T], value T, where ...Condition) (*R, error) {
	q, col, err := oneBy(t, col, where)
	if err != nil {
		return nil, err
	}

	return q.Find(ctx, db, col.Bind(value))
}

// oneBy is the query of GetBy and FindBy.
func oneBy[R any, K, T comparable](t *TableOf[R, K], col Column[R, T], where []Condition) (*Query[R], Column[R, T], error) {
	t = t.unaliased()

	col, err := ownColumn(t, col)
	if err != nil {
		return nil, nil, err
	}

	q, err := t.byQuery(col, false, where, func() (*Query[R], error) {
		return Select(t.Columns()...).From(t).Where(col.EQ(col.Param()), where...).Build()
	})

	return q, col, err
}

// byQuery returns the GetBy / FetchBy query of col: built once per scope and
// cached when there are no extra conditions, whose values would differ per call.
func (t *TableOf[R, K]) byQuery(col SQLColumn, list bool, where []Condition, build func() (*Query[R], error)) (*Query[R], error) {
	if err := t.uniqueBy(col, where); err != nil {
		return nil, err
	}

	if len(where) > 0 {
		return build()
	}

	cached, _ := t.keys.by.LoadOrStore(byKey{column: col.Name(), scope: t.scope(), list: list}, &lazyQuery[R]{})

	return cached.(*lazyQuery[R]).get(build)
}

// ownColumn checks col and binds it to t, the unaliased table the lookups read:
// a column taken from an aliased table struct is bound to the alias.
func ownColumn[R any, K, T comparable](t *TableOf[R, K], col Column[R, T]) (Column[R, T], error) {
	if err := t.Err(); err != nil {
		return nil, err
	}

	if col == nil {
		return nil, errors.New("lookup column cannot be nil")
	}

	if col.core().table != nil && col.core().table.TableName() != t.TableName() {
		col = col.WithTable(t)
	}

	return col, nil
}

// fetchInOrder lists the rows of values through list and puts them in the order
// of values.
//
// The database compares strings under its collation, which can equate two values
// Go does not (case, trailing spaces), so a string value with no row equal to it
// in Go is asked for on its own before it counts as missing. That stops at the
// first value the database does not find either: the fetch fails there, so a long
// list of missing strings costs one extra query, not one per value.
func fetchInOrder[R any, K, T comparable](
	ctx context.Context,
	db Executor,
	t *TableOf[R, K],
	list *Query[R],
	col Column[R, T],
	values []T,
	where []Condition,
) ([]*R, error) {
	if col.core().nullable {
		return nil, fmt.Errorf("fetch %s by %s: the column can be NULL; fetch by a NOT NULL column", t.def.name, col.Name())
	}

	if len(values) == 0 {
		return []*R{}, nil
	}

	rows, err := list.ListIn(ctx, db, col.ListParam(), values)
	if err != nil {
		return nil, err
	}

	get := col.core().get
	byValue := make(map[any]*R, len(rows))

	for _, row := range rows {
		if v, ok := get(row).(T); ok {
			byValue[fetchKey(v)] = row
		}
	}

	// The database decides what matches: a collation for text, and for a time the
	// instant, whatever the zone of the value passed; Go's == on those would call a
	// stored row missing. A value not found by key is asked for on its own.
	kind := reflect.TypeFor[T]().Kind()
	collated := kind == reflect.String || kind == reflect.Struct

	var (
		one     *Query[R]
		missing []T
		seen    = map[T]bool{}
		ordered = make([]*R, 0, len(values))
	)

	for _, v := range values {
		row, ok := byValue[fetchKey(v)]
		if ok {
			ordered = append(ordered, row)
			continue
		}

		if seen[v] {
			continue
		}

		seen[v] = true

		if !collated {
			missing = append(missing, v)
			continue
		}

		if one == nil {
			if one, err = Select(t.Columns()...).From(t).Where(col.EQ(col.Param()), where...).Build(); err != nil {
				return nil, err
			}
		}

		if row, err = one.Find(ctx, db, col.Bind(v)); err != nil {
			return nil, err
		}

		if row == nil {
			missing = append(missing, v)
			break
		}

		byValue[fetchKey(v)] = row
		ordered = append(ordered, row)
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("fetch %s by %s %v: %w", t.def.name, col.Name(), missing, sql.ErrNoRows)
	}

	return ordered, nil
}

// uniqueBy refuses a lookup by a column that does not identify one row: col,
// with the columns where fixes by equality, has to cover the primary key or a
// unique index. A lookup by anything else returned one arbitrary row of several,
// or reported rows as missing, without an error.
func (t *TableOf[R, K]) uniqueBy(col SQLColumn, where []Condition) error {
	def := t.def
	fixed := map[string]bool{col.core().name: true}

	for _, cond := range where {
		if pin := conditionInfo(cond).pins; pin != nil && pin.table == t.TableName() {
			fixed[pin.column] = true
		}
	}

	if def.primaryKey != nil && fixed[def.primaryKey.name] {
		return nil
	}

	for _, index := range def.indexes {
		if !index.Unique {
			continue
		}

		// The live-row scope fixes an integer tombstone for every lookup.
		covered := true

		for _, name := range index.Columns {
			if !fixed[name] && (name != def.managed.DeletedAt || !def.tombstoneIsZero || t.includeDeleted) {
				covered = false
				break
			}
		}

		if covered {
			return nil
		}
	}

	names := slices.Sorted(maps.Keys(fixed))

	return fmt.Errorf("%s: a lookup by %s is not unique; it needs the primary key or a unique index, with the other columns fixed by EQ in where. Query anything else with Select",
		def.name, strings.Join(names, ", "))
}

// fetchKey is the key fetchInOrder matches a value by: a time is the instant, in
// UTC at the microsecond the database keeps.
func fetchKey(v any) any {
	if t, ok := v.(time.Time); ok {
		return t.UTC().Truncate(time.Microsecond)
	}

	return v
}
