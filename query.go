package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

var builtInIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Query is a built SELECT. It is immutable and safe for concurrent use; build it
// once and reuse it. It is rendered for a dialect when it first runs on one, and the
// rendering is cached.
type Query[O any] struct {
	spec querySpec[O]
	// scanErr is why the rows cannot be read into O: a value that can be NULL
	// mapped into a field that cannot hold it. It does not fail Build, because a
	// query used as a subquery or CTE is never read.
	scanErr error
	cache   sync.Map // renderKey -> *statement
}

type renderKey struct {
	dialect tsqdialect.Name
	count   bool
	keyword bool
	single  bool
}

// statement returns the template for mode on d.
func (q *Query[O]) statement(d sqld.Dialect, m renderMode) (*statement, error) {
	key := renderKey{dialect: d.Name(), count: m.count, keyword: m.keyword, single: m.single}
	if !m.paged {
		if cached, ok := q.cache.Load(key); ok {
			return cached.(*statement), nil
		}
	}

	r := newRenderer(d)
	q.spec.render(r, m)

	stmt, err := r.finish()
	if err != nil {
		return nil, err
	}

	if !m.paged {
		q.cache.Store(key, stmt)
	}

	return stmt, nil
}

// prepared is one statement ready to run.
type prepared struct {
	sql  string
	args []any
}

// prepare renders every mode and binds args across all of them: a parameter is
// unused only if none of the statements uses it.
func (q *Query[O]) prepare(exec Executor, args []Arg, builtin map[*paramSpec]any, modes ...renderMode) (execScope, []prepared, error) {
	if q == nil {
		return execScope{}, nil, errors.New("query cannot be nil")
	}

	scope, err := executorScope(exec)
	if err != nil {
		return execScope{}, nil, err
	}

	if len(q.spec.Correlated) > 0 {
		return execScope{}, nil, errors.New("a query with Correlate(...) can only run as a subquery of a query that provides those tables")
	}

	keyword, args, err := q.keywordArgs(args)
	if err != nil {
		return execScope{}, nil, err
	}

	for i := range modes {
		modes[i].keyword = keyword
	}

	stmts := make([]*statement, 0, len(modes))

	var used []*paramSpec

	for _, m := range modes {
		stmt, err := q.statement(scope.dialect, m)
		if err != nil {
			return execScope{}, nil, err
		}

		stmts = append(stmts, stmt)
		used = append(used, stmt.params()...)
	}

	bound, err := bindArgs(used, args, builtin)
	if err != nil {
		return execScope{}, nil, err
	}

	result := make([]prepared, 0, len(stmts))

	for _, stmt := range stmts {
		sqlText, sqlArgs, err := stmt.assemble(scope.dialect, bound)
		if err != nil {
			return execScope{}, nil, err
		}

		result = append(result, prepared{sql: sqlText, args: sqlArgs})
	}

	return scope, result, nil
}

// SQL renders the query for dialect with args bound, as it would run.
func (q *Query[O]) SQL(engine tsqdialect.Name, args ...Arg) (string, []any, error) {
	exec, err := wrapExecutor(noopExecutor{}, engine)
	if err != nil {
		return "", nil, err
	}

	_, stmts, err := q.prepare(exec, args, nil, renderMode{})
	if err != nil {
		return "", nil, err
	}

	return stmts[0].sql, stmts[0].args, nil
}

// String renders the query for debugging, in SQLite syntax, with parameters shown
// by name.
func (q *Query[O]) String() string {
	r := newRenderer(sqld.SQLiteDialect{})
	q.spec.render(r, renderMode{})

	return debugStatement(r)
}

func (q *Query[O]) scan(rows interface{ Scan(...any) error }) (*O, error) {
	row := new(O)

	dest := make([]any, len(q.spec.Selects))
	for i, col := range q.spec.Selects {
		dest[i] = col.core().scan(row)
	}

	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}

	return row, nil
}

// List returns every matching row.
func (q *Query[O]) List(ctx context.Context, db Executor, args ...Arg) ([]*O, error) {
	return traceExecutor1(ctx, db, TraceOpList, func(ctx context.Context) ([]*O, error) {
		_, stmts, err := q.prepare(db, args, nil, renderMode{})
		if err != nil {
			return nil, err
		}

		return q.query(ctx, db, "list", stmts[0])
	})
}

func (q *Query[O]) query(ctx context.Context, db Executor, op string, stmt prepared) ([]*O, error) {
	var list []*O

	err := q.each(ctx, db, op, stmt, func(row *O) bool {
		list = append(list, row)
		return true
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}

// each scans the rows of stmt one at a time until fn returns false.
func (q *Query[O]) each(ctx context.Context, db Executor, op string, stmt prepared, fn func(*O) bool) error {
	if q.scanErr != nil {
		return q.scanErr
	}

	logSQLForExecutor(ctx, db, op, stmt.sql, stmt.args)

	rows, err := db.QueryContext(ctx, stmt.sql, stmt.args...)
	if err != nil {
		return fmt.Errorf("%s query: %w", op, err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logForExecutor(ctx, db, slog.LevelWarn, "failed to close rows", "error", closeErr)
		}
	}()

	for rows.Next() {
		row, err := q.scan(rows)
		if err != nil {
			return fmt.Errorf("%s query: %w", op, err)
		}

		if !fn(row) {
			return nil
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s query: %w", op, err)
	}

	return nil
}

// keywordArgs reports whether args search, dropping an empty Keyword: it renders
// no predicate, so its value would otherwise be reported as unused.
func (q *Query[O]) keywordArgs(args []Arg) (bool, []Arg, error) {
	idx := slices.IndexFunc(args, func(a Arg) bool { return a.spec == keywordParam })
	if idx < 0 {
		return false, args, nil
	}

	if term, _ := args[idx].value.(string); term == "" {
		return false, slices.Delete(slices.Clone(args), idx, idx+1), nil
	}

	if len(q.spec.KeywordSearch) == 0 {
		return false, nil, errors.New("Keyword needs a query built with Search")
	}

	return true, args, nil
}

// ListIn is List for a list parameter that may hold more values than one statement
// can bind, such as a lookup by thousands of keys. values are deduplicated, split
// into statements that fit the dialect's bind parameter limit, and read in one
// snapshot; the rows are concatenated in no particular order. args bind the other
// parameters.
//
// Splitting only preserves the result of a query that filters row by row, so the
// query must use param exactly once, as col.In(param) passed directly to Where,
// and have no GROUP BY, aggregate, DISTINCT, set operation, ORDER BY or LIMIT.
func (q *Query[O]) ListIn[T comparable](ctx context.Context, db Executor, param ListParam[T], values []T, args ...Arg) ([]*O, error) {
	return traceExecutor1(ctx, db, TraceOpList, func(ctx context.Context) ([]*O, error) {
		if err := q.checkSplittable(param.spec); err != nil {
			return nil, err
		}

		scope, empty, err := q.prepare(db, append(slices.Clone(args), param.Bind()), nil, renderMode{})
		if err != nil {
			return nil, err
		}

		stmt, err := q.statement(scope.dialect, renderMode{})
		if err != nil {
			return nil, err
		}

		uses := 0

		for _, c := range stmt.chunks {
			if c.param != nil && c.param.root() == param.spec {
				uses++
			}
		}

		if uses != 1 {
			return nil, fmt.Errorf("list in: %s is used %d times; it must be used once", param.spec.label(), uses)
		}

		// An empty list renders without placeholders, so this is what the rest of
		// the statement binds.
		room := sqld.MaxBindParams(scope.dialect) - len(empty[0].args)
		if room < 1 {
			return nil, errors.New("list in: the other arguments already fill the bind parameter limit")
		}

		unique := make([]T, 0, len(values))
		seen := make(map[T]bool, len(values))

		for _, v := range values {
			if !seen[v] {
				seen[v] = true
				unique = append(unique, v)
			}
		}

		if len(unique) == 0 {
			return q.query(ctx, db, "list", empty[0])
		}

		parts := chunks(unique, room)

		// One statement needs no snapshot: there is nothing to be consistent with.
		if len(parts) == 1 {
			_, stmts, err := q.prepare(db, append(slices.Clone(args), param.Bind(parts[0]...)), nil, renderMode{})
			if err != nil {
				return nil, err
			}

			return q.query(ctx, db, "list", stmts[0])
		}

		return snapshotRead(ctx, db, func(ctx context.Context, db Executor) ([]*O, error) {
			var rows []*O

			for _, part := range parts {
				_, stmts, err := q.prepare(db, append(slices.Clone(args), param.Bind(part...)), nil, renderMode{})
				if err != nil {
					return nil, err
				}

				list, err := q.query(ctx, db, "list", stmts[0])
				if err != nil {
					return nil, err
				}

				rows = append(rows, list...)
			}

			return rows, nil
		})
	})
}

func (q *Query[O]) checkSplittable(spec *paramSpec) error {
	if q == nil {
		return errors.New("query cannot be nil")
	}

	if spec == nil {
		return errors.New("list parameter is not initialized; use tsq.NewListParam")
	}

	s := &q.spec
	if s.grouped() || len(s.OrderBys) > 0 || s.Limit != nil {
		return errors.New("list in: the query must filter row by row, without GROUP BY, aggregates, DISTINCT, set operations, ORDER BY or LIMIT")
	}

	top := 0

	for _, c := range s.Filters {
		if conditionInfo(c).inList == spec {
			top++
		}
	}

	if top != 1 {
		return fmt.Errorf("list in: %s must be used as col.In(%s) passed directly to Where", spec.label(), spec.label())
	}

	return nil
}

// Iter streams the matching rows, scanning one at a time, so a large result never
// sits in memory. Breaking out of the loop stops the query. A failure is yielded
// once, with a nil row, and ends the sequence.
//
// The rows hold a connection until the loop ends, so do not run other statements
// on a single-connection executor, such as a transaction, from inside the loop.
func (q *Query[O]) Iter(ctx context.Context, db Executor, args ...Arg) iter.Seq2[*O, error] {
	return func(yield func(*O, error) bool) {
		stopped := false

		err := traceExecutor(ctx, db, TraceOpIter, func(ctx context.Context) error {
			if q == nil {
				return errors.New("query cannot be nil")
			}

			_, stmts, err := q.prepare(db, args, nil, renderMode{})
			if err != nil {
				return err
			}

			return q.each(ctx, db, "iter", stmts[0], func(row *O) bool {
				stopped = !yield(row, nil)
				return !stopped
			})
		})
		if err != nil && !stopped {
			yield(nil, err)
		}
	}
}

// Get returns the first matching row, or an error wrapping sql.ErrNoRows.
func (q *Query[O]) Get(ctx context.Context, db Executor, args ...Arg) (*O, error) {
	return traceExecutor1(ctx, db, TraceOpGet, func(ctx context.Context) (*O, error) {
		return q.get(ctx, db, args)
	})
}

func (q *Query[O]) get(ctx context.Context, db Executor, args []Arg) (*O, error) {
	if q.scanErr != nil {
		return nil, q.scanErr
	}

	_, stmts, err := q.prepare(db, args, nil, renderMode{single: true})
	if err != nil {
		return nil, err
	}

	stmt := stmts[0]
	logSQLForExecutor(ctx, db, "get", stmt.sql, stmt.args)

	row, err := q.scan(db.QueryRowContext(ctx, stmt.sql, stmt.args...))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}

		return nil, fmt.Errorf("get query: %w", err)
	}

	return row, nil
}

// Find returns the first matching row, or nil when there is none.
func (q *Query[O]) Find(ctx context.Context, db Executor, args ...Arg) (*O, error) {
	return traceExecutor1(ctx, db, TraceOpGet, func(ctx context.Context) (*O, error) {
		row, err := q.get(ctx, db, args)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return row, err
	})
}

// Exists reports whether any row matches. It reads at most one row rather than
// counting them all.
func (q *Query[O]) Exists(ctx context.Context, db Executor, args ...Arg) (bool, error) {
	return traceExecutor1(ctx, db, TraceOpGet, func(ctx context.Context) (bool, error) {
		row, err := q.get(ctx, db, args)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return row != nil, err
	})
}

// Count returns the number of matching rows.
func (q *Query[O]) Count(ctx context.Context, db Executor, args ...Arg) (int64, error) {
	return traceExecutor1(ctx, db, TraceOpCount, func(ctx context.Context) (int64, error) {
		_, stmts, err := q.prepare(db, args, nil, renderMode{count: true})
		if err != nil {
			return 0, err
		}

		return queryCount(ctx, db, stmts[0])
	})
}

func queryCount(ctx context.Context, db Executor, stmt prepared) (int64, error) {
	logSQLForExecutor(ctx, db, "count", stmt.sql, stmt.args)

	var n int64
	if err := db.QueryRowContext(ctx, stmt.sql, stmt.args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count query: %w", err)
	}

	return n, nil
}

func (q *Query[O]) checkSingleSelect(selected SQLColumn) error {
	if q == nil {
		return errors.New("query cannot be nil")
	}

	if isNilValue(selected) {
		return errors.New("selected column cannot be nil")
	}

	if len(q.spec.Selects) != 1 {
		return fmt.Errorf("query must select exactly one column, got %d", len(q.spec.Selects))
	}

	want := debugSQL(columnInfo(selected).sql)
	if got := debugSQL(columnInfo(q.spec.Selects[0]).sql); got != want {
		return fmt.Errorf("query selects %s, not %s", got, want)
	}

	return nil
}

// Page runs the query for one page, plus a count of all matching rows. The query
// must not set Limit or Offset, and it must not order itself when p.OrderBy is set:
// Page owns those clauses.
func (q *Query[O]) Page(ctx context.Context, db Executor, p Paging, args ...Arg) (*PageResponse[O], error) {
	return traceExecutor1(ctx, db, TraceOpPage, func(ctx context.Context) (*PageResponse[O], error) {
		if q == nil {
			return nil, errors.New("query cannot be nil")
		}

		p = p.normalized(runtimeForExecutor(db).MaxPageSize())

		if q.spec.Limit != nil {
			return nil, errors.New("query sets Limit/Offset; Page controls paging, so drop them from the builder")
		}

		if len(p.OrderBy) > 0 && len(q.spec.OrderBys) > 0 {
			return nil, errors.New("query already sets OrderBy; drop it from the builder or leave Paging.OrderBy empty")
		}

		order := make([]orderTerm, 0, len(p.OrderBy))
		for _, ob := range p.OrderBy {
			if ob.direction != ASC && ob.direction != DESC {
				return nil, fmt.Errorf("invalid order direction %q", ob.direction)
			}

			order = append(order, q.spec.orderTerm(ob))
		}

		_, stmts, err := q.prepare(db, args, nil,
			renderMode{count: true},
			renderMode{paged: true, order: order, limit: p.Size, offset: p.Size * (p.Page - 1)},
		)
		if err != nil {
			return nil, err
		}

		// The count and the rows are read from one snapshot; otherwise a write
		// between them makes Total disagree with Data.
		return snapshotRead(ctx, db, func(ctx context.Context, db Executor) (*PageResponse[O], error) {
			total, err := queryCount(ctx, db, stmts[0])
			if err != nil {
				return nil, err
			}

			rows, err := q.query(ctx, db, "page", stmts[1])
			if err != nil {
				return nil, err
			}

			return newPageResponse(p, total, rows), nil
		})
	})
}

// snapshotRead runs fn in one read-only transaction, so the statements it runs see
// the same data. An executor that is already a transaction is used as it is, with
// whatever isolation its caller chose.
func snapshotRead[T any](ctx context.Context, db Executor, fn func(context.Context, Executor) (T, error)) (T, error) {
	var zero T

	s, err := executorScope(db)
	if err != nil {
		return zero, err
	}

	if s.tx {
		return fn(ctx, db)
	}

	// READ COMMITTED, PostgreSQL's default, takes a snapshot per statement. SQLite
	// transactions are serializable already and some drivers reject a level.
	opts := &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead}
	if s.dialect.Name() == tsqdialect.SQLite {
		opts.Isolation = sql.LevelDefault
	}

	if s.runtime != nil {
		return s.runtime.withTxResult(ctx, &TxOptions{SQL: opts}, fn)
	}

	bound, ok := db.(boundExecutor)
	if !ok {
		return fn(ctx, db)
	}

	beginner, ok := bound.DBTX.(interface {
		BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	})
	if !ok {
		return fn(ctx, db)
	}

	tx, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return zero, fmt.Errorf("begin snapshot read: %w", err)
	}

	result, err := fn(ctx, boundExecutor{DBTX: tx, s: execScope{dialect: s.dialect, tx: true}})
	if err != nil {
		return zero, errors.Join(err, ignoreTxDone(tx.Rollback()))
	}

	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("commit snapshot read: %w", err)
	}

	return result, nil
}

func ignoreTxDone(err error) error {
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}

	return err
}

func splitCommaValues(value string) []string {
	var result []string

	for part := range strings.SplitSeq(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}

	return result
}

// AnySubquery is a built query used as a subquery where its columns do not matter,
// as in Exists. A *Query and a Subquery both satisfy it; only TSQ implements it.
type AnySubquery interface {
	subquery() exprInfo
}

// Subquery is a built single-column query holding a T, usable as the right-hand
// side of a comparison or of IN. Make one with AsSubquery or BuildSubquery.
type Subquery[T any] interface {
	AnySubquery
	RHS[T]
	SetRHS[T]
}

func (q *Query[O]) subquery() exprInfo {
	if q == nil {
		return exprInfo{err: errors.New("subquery cannot be nil")}
	}

	return exprInfo{sql: sqlQuery(q)}
}

func (q *Query[O]) renderQuery(r *renderer) {
	r.writeText("(")
	q.spec.render(r, renderMode{})
	r.writeText(")")
}

func (q *Query[O]) correlatedTables() map[string]Table { return q.spec.correlatedNames() }

type typedSubquery[O, T any] struct {
	q *Query[O]
}

func (s typedSubquery[O, T]) subquery() exprInfo               { return s.q.subquery() }
func (s typedSubquery[O, T]) operand() exprInfo                { return nullWhenEmpty(s.q.subquery()) }
func (s typedSubquery[O, T]) setOperand(negated bool) exprInfo { return s.q.subquery() }
func (typedSubquery[O, T]) rhsValue(T)                         {}
func (typedSubquery[O, T]) setValue(T)                         {}

// nullWhenEmpty marks a scalar subquery, which is NULL when it returns no row.
func nullWhenEmpty(info exprInfo) exprInfo {
	info.null = nullness{always: true}
	return info
}

// AsSubquery returns the query as a typed subquery. It must select exactly selected.
func (q *Query[O]) AsSubquery[T any](selected ValueColumn[T]) (Subquery[T], error) {
	if err := q.checkSingleSelect(selected); err != nil {
		return nil, fmt.Errorf("subquery: %w", err)
	}

	return typedSubquery[O, T]{q: q}, nil
}

// BuildSubquery builds stage and returns it as a typed subquery selecting selected.
func BuildSubquery[O, T any](stage QueryStage[O], selected ValueColumn[T]) (Subquery[T], error) {
	if isNilValue(stage) {
		return nil, errors.New("subquery builder cannot be nil")
	}

	q, err := stage.Build()
	if err != nil {
		return nil, err
	}

	return q.AsSubquery(selected)
}

// noopExecutor lets Query.SQL render without a database.
type noopExecutor struct{}

func (noopExecutor) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("no database")
}

func (noopExecutor) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }

func (noopExecutor) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errors.New("no database")
}
