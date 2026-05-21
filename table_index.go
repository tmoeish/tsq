package tsq

import (
	"context"
	"errors"
	"fmt"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

// UpsertIndex ensures a declared index exists or validates it, depending on InitOptions.
func upsertIndex(db *engine, table string, unique bool, idx string, fields []string) error {
	if db == nil {
		return errors.New("engine cannot be nil")
	}

	if db.dialect == nil {
		return errors.New("database dialect is required")
	}

	if err := validateIndexIdentifiers(table, idx, fields); err != nil {
		return err
	}

	mode := db.effectiveIndexInitMode()
	if mode == IndexInitSkip {
		return nil
	}

	definition, found, err := inspectIndexDefinition(db, table, idx)
	if err != nil {
		return err
	}

	if found {
		return validateIndexDefinition(table, unique, idx, fields, definition)
	}

	if mode == IndexInitValidate {
		return &ErrIndexMissing{
			Table:  table,
			Name:   idx,
			Fields: append([]string(nil), fields...),
			Unique: unique,
		}
	}

	_, err = db.dialect.EnsureIndex(context.Background(), db.db, table, unique, idx, fields)
	if err != nil {
		return err
	}

	return nil
}

func (e *engine) effectiveIndexInitMode() IndexInitMode {
	if e == nil || e.indexInitMode == "" {
		return IndexInitUpsert
	}

	return e.indexInitMode
}

func resolveIndexInitMode(options *InitOptions) IndexInitMode {
	if options == nil {
		return IndexInitSkip
	}

	if options.IndexMode != "" {
		return options.IndexMode
	}

	if options.UpsertIndexes {
		return IndexInitUpsert
	}

	return IndexInitSkip
}

func validateIndexInitMode(mode IndexInitMode) error {
	switch mode {
	case IndexInitSkip, IndexInitUpsert, IndexInitValidate:
		return nil
	default:
		return fmt.Errorf("invalid index init mode %q", mode)
	}
}

func inspectIndexDefinition(
	db *engine,
	table string,
	idx string,
) (tsqdialect.IndexDefinition, bool, error) {
	return db.dialect.InspectIndexDefinition(context.Background(), db.db, table, idx)
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
			return fmt.Errorf("invalid index field %s"+": %w", field, err)
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

func quoteDialectIdentifier(sqlDialect tsqdialect.Dialect, name string) (string, error) {
	if err := validateBuiltInIdentifier(name); err != nil {
		return "", err
	}

	if sqlDialect == nil {
		return canonicalQuoteIdentifier(name), nil
	}

	if err := sqlDialect.ValidateIdentifier(name); err != nil {
		return "", err
	}

	return sqlDialect.QuoteField(name), nil
}
