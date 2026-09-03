package tsq

import (
	"errors"
	"fmt"
)

func (spec querySpec[O]) validateSetOperations() error {
	if len(spec.SetOps) == 0 {
		return nil
	}

	if len(spec.KeywordSearch) > 0 {
		return errors.New("set operations do not support keyword search")
	}

	leftCount := len(spec.Selects)
	for _, op := range spec.SetOps {
		if len(op.spec.Selects) != leftCount {
			return fmt.Errorf(
				"set operation %s requires matching select column counts: left=%d right=%d",
				op.op,
				leftCount,
				len(op.spec.Selects),
			)
		}

		if len(op.spec.KeywordSearch) > 0 {
			return errors.New("set operations do not support keyword search")
		}

		// Set-operation operands sit inside the same enclosing query, so they
		// inherit the outer tables the left-hand side declared.
		if err := op.spec.validateJoinGraph(spec.correlatedTables()); err != nil {
			return err
		}

		if err := op.spec.validateSetOperations(); err != nil {
			return err
		}
	}

	return nil
}

// validatePaging rejects paging shapes no supported dialect accepts.
func (spec querySpec[O]) validatePaging() error {
	// MySQL and SQLite both reject OFFSET without a preceding LIMIT, and PostgreSQL
	// accepts it, so allowing it would produce a query that runs on one dialect and is
	// a syntax error on the others. Build() is where that asymmetry is cheapest to
	// catch: the alternative is a database error at execution time on two of three.
	if spec.Offset != nil && spec.Limit == nil {
		return errors.New("Offset requires Limit: a bare OFFSET is a syntax error on mysql and sqlite")
	}

	return nil
}

// correlatedTables returns the outer tables this query may reference without
// joining them, keyed by table name.
func (spec querySpec[O]) correlatedTables() map[string]struct{} {
	tables := make(map[string]struct{}, len(spec.Correlated))
	for _, table := range spec.Correlated {
		if isNilValue(table) {
			continue
		}

		tables[table.Table()] = struct{}{}
	}

	return tables
}

// validateJoinGraph validates that joins form a valid directed acyclic graph (DAG).
// Tables in outer are provided by an enclosing query and may be referenced without
// being joined; they are additive to whatever this spec declares itself.
func (spec querySpec[O]) validateJoinGraph(outer map[string]struct{}) error {
	if err := validateTableInput(spec.From, "from table"); err != nil {
		return err
	}

	allTables := spec.pageQueryTables()
	introduced := make(map[string]struct{}, len(spec.Joins)+1)

	introduced[spec.From.Table()] = struct{}{}

	correlated := spec.correlatedTables()
	for name := range outer {
		correlated[name] = struct{}{}
	}

	for _, item := range spec.Joins {
		if isNilValue(item.table) {
			return errors.New("join table cannot be nil")
		}

		switch item.joinType {
		case crossJoinType:
			tableName := item.table.Table()
			if _, exists := introduced[tableName]; exists {
				return fmt.Errorf("table %s is already present in join graph", tableName)
			}

			introduced[tableName] = struct{}{}
		default:
			tableName := item.table.Table()
			if _, exists := introduced[tableName]; exists {
				return fmt.Errorf("join table %s is already present; aliases are required for repeated joins", tableName)
			}

			if len(item.on) == 0 {
				return fmt.Errorf("%s %s requires at least one ON condition", item.joinType, tableName)
			}

			condTables := spec.tablesForConditions(item.on)
			if _, exists := condTables[tableName]; !exists {
				return fmt.Errorf("%s %s ON conditions must reference joined table %s", item.joinType, tableName, tableName)
			}

			connectedToIntroduced := false

			for condTable := range condTables {
				if condTable == tableName {
					continue
				}

				if _, exists := introduced[condTable]; !exists {
					return fmt.Errorf("join condition table %s is not connected to the current FROM/JOIN graph", condTable)
				}

				connectedToIntroduced = true
			}

			if !connectedToIntroduced {
				return fmt.Errorf("%s %s ON conditions must reference at least one table already in the FROM/JOIN graph", item.joinType, tableName)
			}

			introduced[tableName] = struct{}{}
		}
	}

	for tableName := range correlated {
		if _, exists := introduced[tableName]; !exists {
			continue
		}

		return fmt.Errorf(
			"table %s is declared as a correlated outer table but this query also puts it in its own "+
				"FROM/JOIN clause; the local table would shadow the outer one and the predicate would "+
				"stop being correlated",
			tableName,
		)
	}

	for tableName := range allTables {
		if _, exists := introduced[tableName]; exists {
			continue
		}

		if _, exists := correlated[tableName]; exists {
			continue
		}

		// The advice deliberately stops short of recommending a join outright.
		// When this query is being built as a subquery, the offending table is
		// usually an outer table referenced by a correlated predicate, and joining
		// it here would shadow the outer table instead of correlating with it: the
		// predicate silently becomes uncorrelated and the query still runs.
		return fmt.Errorf(
			"table %s is referenced but is not in this query's FROM/JOIN graph; "+
				"join it explicitly if it belongs to this query, or, if it belongs to an enclosing "+
				"query and this is a correlated reference, declare it with Correlate(%s)",
			tableName, tableName,
		)
	}

	return nil
}
