package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func resolveSchemaPolicy(policy SchemaPolicy) SchemaPolicy {
	if policy == "" {
		return SchemaPolicyManual
	}

	return policy
}

func validateSchemaPolicy(policy SchemaPolicy) error {
	switch policy {
	case SchemaPolicyManual, SchemaPolicyValidate, SchemaPolicyCreateMissing, SchemaPolicyReconcile, SchemaPolicyManaged:
		return nil
	default:
		return fmt.Errorf("invalid schema policy %q", policy)
	}
}

func inspectIndexDefinition(
	db *sql.DB,
	sqlDialect tsqdialect.Dialect,
	table string,
	idx string,
) (tsqdialect.IndexDefinition, bool, error) {
	return sqlDialect.InspectIndexDefinition(context.Background(), db, table, idx)
}

func validateIndexDefinition(
	table string,
	unique bool,
	idx string,
	fields []string,
	existing tsqdialect.IndexDefinition,
) error {
	if existing.Table != table {
		return fmt.Errorf(
			"index %s already exists on table %s, expected table %s",
			idx,
			existing.Table,
			table,
		)
	}

	if existing.Unique != unique || !sameOrderedFields(existing.Fields, fields) {
		return fmt.Errorf(
			"index %s on table %s has definition unique=%t fields=%v, expected unique=%t fields=%v",
			idx,
			table,
			existing.Unique,
			existing.Fields,
			unique,
			fields,
		)
	}

	return nil
}

func sameOrderedFields(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}

func validateIndexIdentifiers(table, idx string, fields []string) error {
	if err := validateBuiltInIdentifier(table); err != nil {
		return fmt.Errorf("%s: %w", "invalid table name", err)
	}

	if err := validateBuiltInIdentifier(idx); err != nil {
		return fmt.Errorf("%s: %w", "invalid index name", err)
	}

	if len(fields) == 0 {
		return errors.New("index fields cannot be empty")
	}

	for _, field := range fields {
		if err := validateBuiltInIdentifier(field); err != nil {
			return fmt.Errorf("invalid index field %s: %w", field, err)
		}
	}

	return nil
}

func validateBuiltInIdentifier(name string) error {
	if !builtInIdentifierPattern.MatchString(name) {
		return fmt.Errorf("invalid SQL identifier: %s", name)
	}

	return nil
}

func upsertIndex(
	db *sql.DB,
	sqlDialect tsqdialect.Dialect,
	policy SchemaPolicy,
	table string,
	unique bool,
	idx string,
	fields []string,
) error {
	if db == nil {
		return errors.New("database connection cannot be nil")
	}

	if sqlDialect == nil {
		return errors.New("database dialect is required")
	}

	if err := validateIndexIdentifiers(table, idx, fields); err != nil {
		return err
	}

	mode := resolveSchemaPolicy(policy)
	if mode == SchemaPolicyManual {
		return nil
	}

	definition, found, err := inspectIndexDefinition(db, sqlDialect, table, idx)
	if err != nil {
		return err
	}

	if found {
		if err := validateIndexDefinition(table, unique, idx, fields, definition); err == nil || mode == SchemaPolicyValidate || mode == SchemaPolicyCreateMissing {
			return err
		}

		if _, err := db.ExecContext(context.Background(), sqlDialect.DDLDropIndex(table, idx)); err != nil {
			return err
		}
	}

	if !found && mode == SchemaPolicyValidate {
		return &ErrIndexMissing{
			Table:  table,
			Name:   idx,
			Fields: append([]string(nil), fields...),
			Unique: unique,
		}
	}

	_, err = sqlDialect.EnsureIndex(context.Background(), db, table, unique, idx, fields)

	return err
}
