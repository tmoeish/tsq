package tsq

// FieldPointer binds a selected column value to a concrete field on an owner.
type FieldPointer[O Owner, T any] func(*O) *T

// scanPointer adapts a typed FieldPointer to the untyped scan path.
type scanPointer func(holder any) any

// SQLColumn is the runtime view of a selectable SQL expression.
type SQLColumn interface {
	SQLExpr() string                    // SQLExpr returns the rendered expression, such as "users.name".
	OutputName() string                 // OutputName returns the default scan alias for the expression.
	JSONFieldName() string              // JSONFieldName returns the stable field name used by JSON-facing helpers.
	Table() Table                       // Table returns the primary table that owns the expression.
	Name() string                       // Name returns the physical column name when the expression comes from a table column.
	QualifiedName() string              // QualifiedName returns the expression with its table qualifier or transformation applied.
	scanPointer() scanPointer           // scanPointer returns the runtime adapter used when scanning result rows.
	referencedTables() map[string]Table // referencedTables returns every table referenced by the expression.
}

// SQLColumns 将一组类型安全的列转换为擦除类型的运行时列列表。
func SQLColumns[O Owner](cols ...BoundColumn[O]) []SQLColumn {
	result := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		result = append(result, col)
	}

	return result
}

// BoundColumn is a selectable expression bound to a specific owner type.
type BoundColumn[O Owner] interface {
	SQLColumn
	selectOwner(O) // 幻影方法，用于范型类型约束
}

// TypedColumn is a selectable expression that also carries the scanned Go value type.
type TypedColumn[O Owner, T any] interface {
	BoundColumn[O]
	columnValue(T)
}

// Column is the user-facing typed SQL expression API that preserves fluent
// chaining without exposing the concrete implementation.
type Column[O Owner, T any] interface {
	// TypedColumn exposes the common SQL expression metadata and typed value marker.
	TypedColumn[O, T]
	// SearchColumn marks the expression as usable in keyword search expansion.
	SearchColumn
	// FieldPointer returns the runtime scan adapter for the bound destination field.
	FieldPointer() scanPointer
	// WithTable returns a copy of the column rebound to a different table source.
	WithTable(table Table) Column[O, T]
	// As returns a copy of the column that targets an aliased table reference.
	As(alias string) Column[O, T]

	// EQVar compares the column to a runtime-bound value with =.
	EQVar() Predicate[O]
	// NEVar compares the column to a runtime-bound value with <>.
	NEVar() Predicate[O]
	// GTVar compares the column to a runtime-bound value with >.
	GTVar() Predicate[O]
	// GTEVar compares the column to a runtime-bound value with >=.
	GTEVar() Predicate[O]
	// LTVar compares the column to a runtime-bound value with <.
	LTVar() Predicate[O]
	// LTEVar compares the column to a runtime-bound value with <=.
	LTEVar() Predicate[O]
	// InVar binds a slice at execution time for IN predicates.
	InVar() Predicate[O]
	// StartsWithVar defers a prefix match to execution time, which tsq rejects for portability.
	StartsWithVar() Predicate[O]
	// NStartsWithVar defers a negated prefix match to execution time, which tsq rejects for portability.
	NStartsWithVar() Predicate[O]
	// EndsWithVar defers a suffix match to execution time, which tsq rejects for portability.
	EndsWithVar() Predicate[O]
	// NEndsWithVar defers a negated suffix match to execution time, which tsq rejects for portability.
	NEndsWithVar() Predicate[O]
	// ContainsVar defers a contains match to execution time, which tsq rejects for portability.
	ContainsVar() Predicate[O]
	// NContainsVar defers a negated contains match to execution time, which tsq rejects for portability.
	NContainsVar() Predicate[O]
	// BetweenVar compares the column to two runtime-bound values with BETWEEN.
	BetweenVar() Predicate[O]
	// NBetweenVar compares the column to two runtime-bound values with NOT BETWEEN.
	NBetweenVar() Predicate[O]

	// EQ compares the column to arg with =.
	EQ(arg T) Predicate[O]
	// NE compares the column to arg with <>.
	NE(arg T) Predicate[O]
	// GT compares the column to arg with >.
	GT(arg T) Predicate[O]
	// GTE compares the column to arg with >=.
	GTE(arg T) Predicate[O]
	// LT compares the column to arg with <.
	LT(arg T) Predicate[O]
	// LTE compares the column to arg with <=.
	LTE(arg T) Predicate[O]
	// StartsWith compares the column to a bound prefix pattern.
	StartsWith(str string) Predicate[O]
	// NStartsWith compares the column to a negated bound prefix pattern.
	NStartsWith(str string) Predicate[O]
	// EndsWith compares the column to a bound suffix pattern.
	EndsWith(str string) Predicate[O]
	// NEndsWith compares the column to a negated bound suffix pattern.
	NEndsWith(str string) Predicate[O]
	// Contains compares the column to a bound contains pattern.
	Contains(str string) Predicate[O]
	// NContains compares the column to a negated bound contains pattern.
	NContains(str string) Predicate[O]
	// Between compares the column to an inclusive range.
	Between(start, end T) Predicate[O]
	// NBetween compares the column to values outside an inclusive range.
	NBetween(start, end T) Predicate[O]
	// In compares the column to an explicit list of bound values.
	In(args ...T) Predicate[O]
	// NIn compares the column to a negated list of bound values.
	NIn(args ...T) Predicate[O]
	// IsNull checks whether the column value is NULL.
	IsNull() Predicate[O]
	// IsNotNull checks whether the column value is not NULL.
	IsNotNull() Predicate[O]

	// EQCol compares the column to another column with =.
	EQCol(other typedColumnInternal[T]) Predicate[O]
	// NECol compares the column to another column with <>.
	NECol(other typedColumnInternal[T]) Predicate[O]
	// GTCol compares the column to another column with >.
	GTCol(other typedColumnInternal[T]) Predicate[O]
	// GTECol compares the column to another column with >=.
	GTECol(other typedColumnInternal[T]) Predicate[O]
	// LTCol compares the column to another column with <.
	LTCol(other typedColumnInternal[T]) Predicate[O]
	// LTECol compares the column to another column with <=.
	LTECol(other typedColumnInternal[T]) Predicate[O]
	// StartsWithCol compares the column to another column with a prefix match, which tsq rejects for portability.
	StartsWithCol(other typedColumnInternal[T]) Predicate[O]
	// NStartsWithCol compares the column to another column with a negated prefix match, which tsq rejects for portability.
	NStartsWithCol(other typedColumnInternal[T]) Predicate[O]
	// EndsWithCol compares the column to another column with a suffix match, which tsq rejects for portability.
	EndsWithCol(other typedColumnInternal[T]) Predicate[O]
	// NEndsWithCol compares the column to another column with a negated suffix match, which tsq rejects for portability.
	NEndsWithCol(other typedColumnInternal[T]) Predicate[O]
	// ContainsCol compares the column to another column with a contains match, which tsq rejects for portability.
	ContainsCol(other typedColumnInternal[T]) Predicate[O]
	// NContainsCol compares the column to another column with a negated contains match, which tsq rejects for portability.
	NContainsCol(other typedColumnInternal[T]) Predicate[O]
	// EQSub compares the column to a scalar subquery with =.
	EQSub(sq subquery) Predicate[O]
	// NESub compares the column to a scalar subquery with <>.
	NESub(sq subquery) Predicate[O]
	// GTSub compares the column to a scalar subquery with >.
	GTSub(sq subquery) Predicate[O]
	// GTESub compares the column to a scalar subquery with >=.
	GTESub(sq subquery) Predicate[O]
	// LTSub compares the column to a scalar subquery with <.
	LTSub(sq subquery) Predicate[O]
	// LTESub compares the column to a scalar subquery with <=.
	LTESub(sq subquery) Predicate[O]
	// LikeSub compares the column to a scalar subquery with LIKE.
	LikeSub(sq subquery) Predicate[O]
	// NLikeSub compares the column to a scalar subquery with NOT LIKE.
	NLikeSub(sq subquery) Predicate[O]
	// InSub compares the column to a membership subquery with IN.
	InSub(sq subquery) Predicate[O]
	// NInSub compares the column to a membership subquery with NOT IN.
	NInSub(sq subquery) Predicate[O]
	// ExistsSub returns an EXISTS predicate for the supplied subquery.
	ExistsSub(sq subquery) Predicate[O]
	// NExistsSub returns a NOT EXISTS predicate for the supplied subquery.
	NExistsSub(sq subquery) Predicate[O]
	// Unique returns a deferred portability error because UNIQUE subquery predicates are not supported.
	Unique(sq subquery) Predicate[O]
	// NUnique returns a deferred portability error because NOT UNIQUE subquery predicates are not supported.
	NUnique(sq subquery) Predicate[O]

	// Pred formats a custom predicate template around the receiver column.
	// The format must contain one %s placeholder for the receiver column plus
	// one %s placeholder for each extra argument.
	//
	// Extra arguments may be:
	//   - another SQL expression such as a Column
	//   - an Expression such as Bind(...)
	//   - a plain Go value, which TSQ turns into a bound parameter
	//
	// Raw subqueries are rejected; use the dedicated EQSub/NESub/GTSub/InSub/
	// ExistsSub helpers instead.
	//
	// Example:
	//
	//	users.Name.Pred("LOWER(%s) = LOWER(%s)", tsq.Bind("alice"))
	Pred(format string, args ...any) Predicate[O]
	// Expr formats the receiver column into a custom SQL expression template.
	// The format must contain exactly one %s placeholder, which receives the
	// receiver column expression.
	//
	// This is an escape hatch for expression wrappers that TSQ does not model
	// directly, such as CAST(%s AS TEXT) or (%s COLLATE NOCASE).
	//
	// Example:
	//
	//	users.Name.Expr("LOWER(%s)")
	Expr(format string) Column[O, T]
	// Exprf formats the receiver column plus extra SQL expressions into a custom
	// SQL expression template. The first %s placeholder receives the receiver
	// column; each additional %s placeholder receives the corresponding argument
	// expression.
	//
	// Extra arguments may be Columns, Expressions, or plain Go values.
	//
	// Example:
	//
	//	users.Name.Exprf("COALESCE(%s, %s)", orgs.Name)
	Exprf(format string, args ...any) Column[O, T]

	// Count wraps the column in COUNT and marks it as an aggregate expression.
	Count() Column[O, int64]
	// Sum wraps the column in SUM and marks it as an aggregate expression.
	Sum() Column[O, T]
	// Avg wraps the column in AVG and marks it as an aggregate expression.
	Avg() Column[O, float64]
	// Max wraps the column in MAX and marks it as an aggregate expression.
	Max() Column[O, T]
	// Min wraps the column in MIN and marks it as an aggregate expression.
	Min() Column[O, T]
	// Distinct wraps the column in DISTINCT and marks it as a distinct expression.
	Distinct() Column[O, T]

	// Upper wraps the column in UPPER.
	Upper() Column[O, T]
	// Lower wraps the column in LOWER.
	Lower() Column[O, T]
	// Substring wraps the column in SUBSTRING using start and length.
	Substring(start, length int) Column[O, T]
	// Length wraps the column in LENGTH.
	Length() Column[O, T]
	// Trim wraps the column in TRIM.
	Trim() Column[O, T]
	// Concat appends str to the column expression with CONCAT.
	Concat(str string) Column[O, T]
	// Now returns a NOW expression.
	Now() Column[O, T]
	// Date wraps the column in DATE.
	Date() Column[O, T]
	// Year wraps the column in YEAR.
	Year() Column[O, T]
	// Month wraps the column in MONTH.
	Month() Column[O, T]
	// Day wraps the column in DAY.
	Day() Column[O, T]
	// Round wraps the column in ROUND with precision.
	Round(precision int) Column[O, T]
	// Ceil wraps the column in CEIL.
	Ceil() Column[O, T]
	// Floor wraps the column in FLOOR.
	Floor() Column[O, T]
	// Abs wraps the column in ABS.
	Abs() Column[O, T]
	// Coalesce wraps the column in COALESCE with a fallback value.
	Coalesce(value any) Column[O, T]
	// NullIf wraps the column in NULLIF with value.
	NullIf(value any) Column[O, T]

	// Asc creates an ascending ORDER BY clause for this column.
	Asc() OrderBy
	// Desc creates a descending ORDER BY clause for this column.
	Desc() OrderBy
}

// ResultColumn is the user-facing typed projection API returned by MapInto.
// It keeps result bindings inspectable without exposing the concrete
// projection implementation.
type ResultColumn[O Owner, T any] interface {
	TypedColumn[O, T]
	FieldPointer() scanPointer
}

// TableColumn is a physical source column that belongs to a table owner.
type TableColumn[O Table] interface {
	BoundColumn[O]
	SearchColumn
	tableColumnOwner(O)
	tableSource() Table
	columnName() string
}

// SearchColumn marks expressions that may participate in keyword search expansion.
type SearchColumn interface {
	SQLColumn
	searchColumn()
}

type typedColumnInternal[T any] interface {
	SQLColumn
	columnValue(T)
}
