package tsq

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type buildErrorCarrier interface {
	buildError() error
}

type tableColumnLister interface {
	Cols() []SQLColumn
}

type transformedColumn interface {
	isTransformedExpression() bool
}

func isNilValue(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func validateColumnInput(col SQLColumn) (Table, error) {
	if isNilValue(col) {
		return nil, errors.New("column cannot be nil")
	}

	if carrier, ok := col.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return nil, carrier.buildError()
	}

	table := columnPrimaryTable(col)

	refs := col.referencedTables()
	if len(refs) == 0 && isNilValue(table) {
		if name := strings.TrimSpace(col.OutputName()); name != "" {
			return nil, fmt.Errorf("column %s must reference at least one table", name)
		}

		return nil, errors.New("column must reference at least one table")
	}

	if isNilValue(table) {
		if name := strings.TrimSpace(col.OutputName()); name != "" {
			return nil, fmt.Errorf("column %s table cannot be nil", name)
		}

		return nil, errors.New("column table cannot be nil")
	}

	if carrier, ok := table.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return nil, carrier.buildError()
	}

	if err := validateColumnBelongsToTable(col, table); err != nil {
		return nil, err
	}

	return table, nil
}

func validateColumnBelongsToTable(col SQLColumn, table Table) error {
	source, ok := col.(interface {
		tableSource() Table
		columnName() string
	})
	if !ok {
		return nil
	}

	if cols, ok := tableColumns(table); ok {
		if len(cols) == 0 {
			goto transformed
		}

		for _, candidate := range cols {
			if isNilValue(candidate) {
				continue
			}

			if candidate.Name() == source.columnName() {
				return nil
			}
		}

		return fmt.Errorf("column %s does not belong to table %s", source.columnName(), table.Table())
	}

transformed:
	if transformed, ok := col.(transformedColumn); ok && transformed.isTransformedExpression() && source.columnName() == "case" {
		return nil
	}

	if isNilValue(source.tableSource()) {
		return nil
	}

	if source.tableSource().Table() == table.Table() {
		return nil
	}

	return fmt.Errorf("column %s does not belong to table %s", source.columnName(), table.Table())
}

func tableColumns(table Table) ([]SQLColumn, bool) {
	if lister, ok := table.(tableColumnLister); ok {
		return lister.Cols(), true
	}

	rv := reflect.ValueOf(table)
	if !rv.IsValid() {
		return nil, false
	}

	method := rv.MethodByName("Cols")
	if !method.IsValid() {
		return nil, false
	}

	mt := method.Type()
	if mt.NumIn() != 0 || mt.NumOut() != 1 {
		return nil, false
	}

	result := method.Call(nil)[0]
	if !result.IsValid() || result.Kind() != reflect.Slice {
		return nil, false
	}

	cols := make([]SQLColumn, 0, result.Len())
	for i := 0; i < result.Len(); i++ {
		col, ok := result.Index(i).Interface().(SQLColumn)
		if !ok {
			return nil, false
		}

		cols = append(cols, col)
	}

	return cols, true
}

func validateBoundColumn[O Owner](col BoundColumn[O]) error {
	_, err := validateColumnInput(col)
	return err
}

func validateSearchColumn(col SearchColumn) error {
	_, err := validateColumnInput(col)
	return err
}

func validateTableInput(table Table, label string) error {
	if isNilValue(table) {
		return fmt.Errorf("%s cannot be nil", label)
	}

	if carrier, ok := table.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return carrier.buildError()
	}

	if strings.TrimSpace(table.Table()) == "" {
		return fmt.Errorf("%s name cannot be empty", label)
	}

	return nil
}

func validateConditionInput(cond Condition) (string, map[string]Table, []any, error) {
	if isNilValue(cond) {
		return "", nil, nil, errors.New("condition cannot be nil")
	}

	if carrier, ok := cond.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return "", nil, nil, carrier.buildError()
	}

	clause := strings.TrimSpace(conditionClause(cond))
	if clause == "" {
		return "", nil, nil, errors.New("condition clause cannot be empty")
	}

	tables := cond.Tables()
	if tables == nil {
		return clause, map[string]Table{}, cond.Args(), nil
	}

	for name, table := range tables {
		if isNilValue(table) {
			if name == "" {
				return "", nil, nil, errors.New("condition table cannot be nil")
			}

			return "", nil, nil, fmt.Errorf("condition table %s cannot be nil", name)
		}
	}

	return clause, tables, cond.Args(), nil
}

func conditionClause(cond Condition) string {
	if raw, ok := cond.(rawConditionClauser); ok {
		return raw.rawClause()
	}

	return cond.Clause()
}

func columnPrimaryTable(col SQLColumn) Table {
	if isNilValue(col) {
		return nil
	}

	if source, ok := col.(interface{ tableSource() Table }); ok && !isNilValue(source.tableSource()) {
		return source.tableSource()
	}

	refs := col.referencedTables()
	if len(refs) == 0 {
		return nil
	}

	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}

	sort.Strings(names)

	return refs[names[0]]
}
