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

		if err := op.spec.validateJoinGraph(); err != nil {
			return err
		}

		if err := op.spec.validateSetOperations(); err != nil {
			return err
		}
	}

	return nil
}

// validateJoinGraph validates that joins form a valid directed acyclic graph (DAG).
func (spec querySpec[O]) validateJoinGraph() error {
	if err := validateTableInput(spec.From, "from table"); err != nil {
		return err
	}

	allTables := spec.pageQueryTables()
	introduced := make(map[string]struct{}, len(spec.Joins)+1)

	introduced[spec.From.Table()] = struct{}{}

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

	for tableName := range allTables {
		if _, exists := introduced[tableName]; exists {
			continue
		}

		return fmt.Errorf(
			"table %s is referenced outside the join graph; use CrossJoin to include it explicitly",
			tableName,
		)
	}

	return nil
}
