package tsq

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// UpsertIndex ensures a declared index exists or validates it, depending on InitOptions.
func UpsertIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	if db == nil {
		return errors.New("engine cannot be nil")
	}

	if db.Dialect == nil {
		return errors.New("database dialect is required")
	}

	if err := validateIndexIdentifiers(table, idx, fields); err != nil {
		return err
	}

	mode := db.effectiveIndexInitMode()
	if mode == IndexInitSkip {
		return db.emitSchemaEvent(SchemaEvent{
			Kind:  SchemaEventSkipIndex,
			Table: table,
			Name:  idx,
		})
	}

	definition, found, err := inspectIndexDefinition(db, table, idx)
	if err != nil {
		return err
	}

	if found {
		if err := validateIndexDefinition(table, unique, idx, fields, definition); err != nil {
			return err
		}

		if err := db.emitSchemaEvent(SchemaEvent{
			Kind:  SchemaEventValidateIndex,
			Table: table,
			Name:  idx,
		}); err != nil {
			return err
		}

		return nil
	}

	if mode == IndexInitValidate {
		return &ErrIndexMissing{
			Table:  table,
			Name:   idx,
			Fields: append([]string(nil), fields...),
			Unique: unique,
		}
	}

	query, err := db.Dialect.EnsureIndex(context.Background(), db, table, unique, idx, fields)
	if err != nil {
		return err
	}

	if query != "" {
		return db.emitSchemaEvent(SchemaEvent{
			Kind:  SchemaEventCreateIndex,
			Table: table,
			Name:  idx,
			SQL:   query,
		})
	}

	return nil
}

func (e *Engine) effectiveIndexInitMode() IndexInitMode {
	return loadDBSchemaConfig(e).indexInitMode
}

func (e *Engine) emitSchemaEvent(event SchemaEvent) (err error) {
	handler := loadDBSchemaConfig(e).schemaEventHandler
	if e == nil || handler == nil {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf(
				"schema event handler panicked for %s on %s: %v",
				event.Kind,
				event.Table,
				r,
			)
		}
	}()

	handler(event)

	return nil
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
	db *Engine,
	table string,
	idx string,
) (IndexDefinition, bool, error) {
	return db.Dialect.InspectIndexDefinition(context.Background(), db, table, idx)
}

func validateIndexDefinition(
	table string,
	unique bool,
	idx string,
	fields []string,
	existing IndexDefinition,
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

func columnsCSV(fields []string) string {
	return strings.Join(fields, ",")
}

func parseColumnsCSV(csv string) []string {
	if csv == "" {
		return nil
	}

	return strings.Split(csv, ",")
}

func finishCreateIndex(
	db *Engine,
	table string,
	unique bool,
	idx string,
	fields []string,
	createErr error,
) error {
	if createErr == nil {
		return nil
	}

	definition, found, err := inspectIndexDefinition(db, table, idx)
	if err == nil && found && validateIndexDefinition(table, unique, idx, fields, definition) == nil {
		return nil
	}

	return createErr
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

func quoteDialectIdentifier(dialect Dialect, name string) (string, error) {
	if err := validateBuiltInIdentifier(name); err != nil {
		return "", err
	}

	if dialect == nil {
		return canonicalQuoteIdentifier(name), nil
	}

	if err := dialect.ValidateIdentifier(name); err != nil {
		return "", err
	}

	return dialect.QuoteField(name), nil
}

func quoteDialectIdentifiers(dialect Dialect, names []string) ([]string, error) {
	quoted := make([]string, len(names))

	for i, name := range names {
		value, err := quoteDialectIdentifier(dialect, name)
		if err != nil {
			return nil, err
		}

		quoted[i] = value
	}

	return quoted, nil
}
