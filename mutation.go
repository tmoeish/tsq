package tsq

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// mutationKind distinguishes the two statement shapes a Mutation can render.
type mutationKind string

const (
	mutationKindUpdate mutationKind = "update"
	mutationKindDelete mutationKind = "delete"
)

// mutationAssignment is one rendered SET item: an unqualified column identifier
// (PostgreSQL rejects table-qualified targets in SET) and its value expression.
type mutationAssignment struct {
	column string
	expr   string
	args   []any
}

// mutationSpec accumulates the state shared by the update and delete builders.
type mutationSpec[O Table] struct {
	kind        mutationKind
	table       Table
	assignments []mutationAssignment
	filters     []Condition
	buildErr    error
}

// MutationStage is the buildable state of an UPDATE or DELETE statement after
// Where(...). It deliberately exposes no further clause methods: a statement
// gets exactly one WHERE, and it is the type system that says so.
type MutationStage[O Table] interface {
	Build() (*Mutation[O], error)
	MustBuild() *Mutation[O]
	Exec(ctx context.Context, tx SQLExecutor, args ...any) (int64, error)
}

// Mutation is a compiled UPDATE or DELETE statement scoped by a WHERE clause.
// Like Query, it is immutable and safe to share across goroutines, and it is
// rendered for the concrete dialect only at execution time.
//
// Mutations never check the optimistic-lock version column. An UPDATE on a
// table that declares one still increments it, so that in-memory rows loaded
// before the bulk change fail their own Update with ErrOptimisticLockConflict.
type Mutation[O Table] struct {
	kind     mutationKind
	table    string
	sql      string
	args     []any
	argState queryArgState
}

type updateBuilder[O Table] struct {
	spec mutationSpec[O]
}

type deleteBuilder[O Table] struct {
	spec mutationSpec[O]
}

// UpdateTable starts an UPDATE statement against the table represented by O.
//
// Assignments are added with Set, SetVal, and SetVar; the statement becomes
// buildable only after Where(...). Use Update(ctx, tx, item) instead when the
// caller holds the row and wants optimistic locking.
func UpdateTable[O Table]() *updateBuilder[O] {
	return &updateBuilder[O]{spec: newMutationSpec[O](mutationKindUpdate)}
}

// DeleteFrom starts a DELETE statement against the table represented by O.
//
// The statement becomes buildable only after Where(...). Use Delete(ctx, tx,
// item) instead when the caller holds the row and wants optimistic locking.
func DeleteFrom[O Table]() *deleteBuilder[O] {
	return &deleteBuilder[O]{spec: newMutationSpec[O](mutationKindDelete)}
}

func newMutationSpec[O Table](kind mutationKind) mutationSpec[O] {
	var table O

	spec := mutationSpec[O]{kind: kind}

	// O is expected to be the generated value type (Course, not *Course): a
	// pointer owner is a nil pointer here and its metadata is unreachable.
	if isNilValue(table) {
		spec.buildErr = errors.New("mutation owner must be a table value type, not a pointer")
		return spec
	}

	if err := validateTableInput(table, "mutation table"); err != nil {
		spec.buildErr = err
		return spec
	}

	spec.table = table

	return spec
}

func (spec mutationSpec[O]) clone() mutationSpec[O] {
	spec.assignments = append([]mutationAssignment(nil), spec.assignments...)
	spec.filters = append([]Condition(nil), spec.filters...)

	return spec
}

func (spec *mutationSpec[O]) setBuildError(err error) {
	if spec.buildErr == nil && err != nil {
		spec.buildErr = err
	}
}

// Set assigns rhs to col. rhs follows the same contract as the comparison
// predicates: a typed column or expression of the same value type, or a typed
// scalar subquery. Plain Go values go through SetVal, runtime placeholders
// through SetVar.
func (b *updateBuilder[O]) Set[T any](col TypedColumn[O, T], rhs RHS[T]) *updateBuilder[O] {
	next := b.cloneBuilder()
	next.spec.addAssignment(col, predicateRHSArg(rhs))

	return next
}

// SetVal assigns a bound Go value to col.
func (b *updateBuilder[O]) SetVal[T any](col TypedColumn[O, T], value T) *updateBuilder[O] {
	next := b.cloneBuilder()
	next.spec.addAssignment(col, Bind(value))

	return next
}

// SetVar assigns a value supplied to Exec at execution time. SET placeholders
// are bound before WHERE placeholders, in the order the assignments were added.
func (b *updateBuilder[O]) SetVar[T any](col TypedColumn[O, T]) *updateBuilder[O] {
	next := b.cloneBuilder()
	next.spec.addAssignment(col, varMarker)

	return next
}

// Where scopes the UPDATE. Every condition is ANDed; use Or(...) for
// alternatives. A full-table update needs an explicit always-true condition
// such as And(); a WHERE-less statement is not expressible on purpose.
func (b *updateBuilder[O]) Where(conds ...Condition) MutationStage[O] {
	next := b.cloneBuilder()
	next.spec.setWhere(conds...)

	return &mutationBuildStage[O]{spec: next.spec}
}

func (b *updateBuilder[O]) cloneBuilder() *updateBuilder[O] {
	if b == nil {
		panic(errQueryBuilderNil)
	}

	return &updateBuilder[O]{spec: b.spec.clone()}
}

// Where scopes the DELETE. Every condition is ANDed; use Or(...) for
// alternatives. A full-table delete needs an explicit always-true condition
// such as And(); a WHERE-less statement is not expressible on purpose.
func (b *deleteBuilder[O]) Where(conds ...Condition) MutationStage[O] {
	if b == nil {
		panic(errQueryBuilderNil)
	}

	spec := b.spec.clone()
	spec.setWhere(conds...)

	return &mutationBuildStage[O]{spec: spec}
}

type mutationBuildStage[O Table] struct {
	spec mutationSpec[O]
}

// Build compiles the statement or returns the first validation error.
func (s *mutationBuildStage[O]) Build() (*Mutation[O], error) {
	if s == nil {
		return nil, errQueryBuilderNil
	}

	return buildMutation(s.spec)
}

// MustBuild compiles the statement or panics if validation fails.
//
// MustBuild is intended for package-level statement variables whose shape is
// fixed at compile time. Statements assembled from user input should call Build
// and check the returned error.
func (s *mutationBuildStage[O]) MustBuild() *Mutation[O] {
	mutation, err := s.Build()
	if err != nil {
		panic(err)
	}

	return mutation
}

// Exec builds and executes the statement, returning the affected row count.
func (s *mutationBuildStage[O]) Exec(ctx context.Context, tx SQLExecutor, args ...any) (int64, error) {
	mutation, err := s.Build()
	if err != nil {
		return 0, err
	}

	return mutation.Exec(ctx, tx, args...)
}

func (spec *mutationSpec[O]) addAssignment(col SQLColumn, value any) {
	if spec.buildErr != nil {
		return
	}

	column, err := spec.assignableColumn(col)
	if err != nil {
		spec.setBuildError(err)
		return
	}

	for _, existing := range spec.assignments {
		if existing.column == column {
			spec.setBuildError(fmt.Errorf("column %s is assigned more than once", column))
			return
		}
	}

	// A column value carries its own build error (a malformed Exprf, say) and
	// argumentToExpression would flatten it into a plain fragment, so it is
	// validated as a column before it is rendered.
	if valueCol, ok := value.(SQLColumn); ok {
		if _, err := validateColumnInput(valueCol); err != nil {
			spec.setBuildError(err)
			return
		}

		if err := spec.validateOwnTable(valueCol.referencedTables(), "assignment value"); err != nil {
			spec.setBuildError(err)
			return
		}
	}

	expr := argumentToExpression(value)
	if err := expressionBuildError(expr); err != nil {
		spec.setBuildError(err)
		return
	}

	spec.assignments = append(spec.assignments, mutationAssignment{
		column: column,
		expr:   expr.Expr(),
		args:   expr.Args(),
	})
}

// assignableColumn checks that col is a physical column of the target table and
// returns its bare name. Expressions built with Expr/Exprf and columns rebound
// onto another table or alias are rejected: the left side of SET must be a
// column of the table being updated.
func (spec *mutationSpec[O]) assignableColumn(col SQLColumn) (string, error) {
	table, err := validateColumnInput(col)
	if err != nil {
		return "", err
	}

	if transformed, ok := col.(transformedColumn); ok && transformed.isTransformedExpression() {
		return "", fmt.Errorf("assignment target %s must be a physical table column", col.OutputName())
	}

	name := strings.TrimSpace(col.Name())
	if name == "" {
		return "", errors.New("assignment target must be a physical table column")
	}

	if err := spec.validateOwnTable(map[string]Table{table.Table(): table}, "assignment target "+name); err != nil {
		return "", err
	}

	if version := strings.TrimSpace(spec.table.VersionColumn()); version != "" && name == version {
		return "", fmt.Errorf(
			"column %s is the optimistic-lock version of table %s and is incremented automatically; it cannot be assigned",
			name,
			spec.table.Table(),
		)
	}

	return name, nil
}

func (spec *mutationSpec[O]) setWhere(conds ...Condition) {
	if spec.buildErr != nil {
		return
	}

	if len(conds) == 0 {
		spec.setBuildError(errors.New("Where() requires at least one condition; use And() for an explicit full-table statement"))
		return
	}

	filters := make([]Condition, 0, len(conds))

	for _, cond := range conds {
		_, tables, _, err := validateConditionInput(cond)
		if err != nil {
			spec.setBuildError(err)
			return
		}

		if err := spec.validateOwnTable(tables, "condition"); err != nil {
			spec.setBuildError(err)
			return
		}

		filters = append(filters, cond)
	}

	spec.filters = filters
}

// validateOwnTable rejects references to any table other than the unaliased
// target. UPDATE ... FROM / multi-table DELETE and aliased targets are spelled
// differently on every built-in dialect, so a single statement shape cannot be
// fixed at Build() time for them.
func (spec *mutationSpec[O]) validateOwnTable(tables map[string]Table, what string) error {
	for _, table := range tables {
		if isNilValue(table) {
			continue
		}

		if alias := tableAliasName(table); alias != "" {
			return fmt.Errorf("%s references alias %s; %s statements target the unaliased table", what, alias, spec.kind)
		}

		if physicalTableName(table) != physicalTableName(spec.table) {
			return fmt.Errorf(
				"%s references table %s but the %s statement targets %s",
				what,
				table.Table(),
				spec.kind,
				spec.table.Table(),
			)
		}
	}

	return nil
}

func buildMutation[O Table](spec mutationSpec[O]) (*Mutation[O], error) {
	if spec.buildErr != nil {
		return nil, spec.buildErr
	}

	if len(spec.filters) == 0 {
		return nil, errors.New("mutation requires a WHERE clause")
	}

	var (
		builder strings.Builder
		args    []any
	)

	tableSQL := rawTableSourceIdentifier(spec.table)

	switch spec.kind {
	case mutationKindUpdate:
		if len(spec.assignments) == 0 {
			return nil, errors.New("update requires at least one assignment")
		}

		builder.WriteString("UPDATE ")
		builder.WriteString(tableSQL)
		builder.WriteString(" SET ")

		for i, assignment := range spec.assignments {
			if i > 0 {
				builder.WriteString(", ")
			}

			builder.WriteString(rawIdentifier(assignment.column))
			builder.WriteString(" = ")
			builder.WriteString(assignment.expr)
			args = append(args, assignment.args...)
		}

		if version := strings.TrimSpace(spec.table.VersionColumn()); version != "" {
			versionSQL := rawIdentifier(version)

			builder.WriteString(", ")
			builder.WriteString(versionSQL)
			builder.WriteString(" = ")
			builder.WriteString(versionSQL)
			builder.WriteString(" + 1")
		}

	case mutationKindDelete:
		builder.WriteString("DELETE FROM ")
		builder.WriteString(tableSQL)
	default:
		return nil, fmt.Errorf("unsupported mutation kind %q", spec.kind)
	}

	whereSQL, whereArgs := buildConditionSQL(" WHERE ", spec.filters)
	builder.WriteString(whereSQL)

	args = append(args, whereArgs...)

	return &Mutation[O]{
		kind:     spec.kind,
		table:    spec.table.Table(),
		sql:      builder.String(),
		args:     args,
		argState: scanQueryArgState(args),
	}, nil
}

// SQL returns the statement in canonical form, before dialect rendering.
func (m *Mutation[O]) SQL() string {
	if m == nil {
		return ""
	}

	return renderCanonicalSQL(m.sql)
}

// Exec runs the statement and returns the number of affected rows. args fill
// the SetVar and *Var placeholders in statement order: SET first, then WHERE.
func (m *Mutation[O]) Exec(ctx context.Context, tx SQLExecutor, args ...any) (int64, error) {
	return traceExecutor1(ctx, tx, func(ctx context.Context) (int64, error) {
		return execMutationFn(ctx, tx, m, args...)
	})
}

func execMutationFn[O Table](ctx context.Context, tx SQLExecutor, m *Mutation[O], args ...any) (int64, error) {
	if m == nil {
		return 0, errors.New("mutation cannot be nil")
	}

	if m.sql == "" {
		return 0, errors.New("mutation is not built")
	}

	resolvedSQL, finalArgs, err := resolveQueryWithState(m.sql, m.args, args, "", m.argState)
	if err != nil {
		return 0, err
	}

	if err := validateOperationalExecutorForSQL(tx, resolvedSQL); err != nil {
		return 0, err
	}

	sqlText := renderSQLForExecutor(tx, resolvedSQL)

	logSQLForExecutor(ctx, tx, string(m.kind), sqlText, finalArgs)

	result, err := tx.ExecContext(ctx, sqlText, finalArgs...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute %s on %s: %w", m.kind, m.table, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read rows affected by %s on %s: %w", m.kind, m.table, err)
	}

	return affected, nil
}
