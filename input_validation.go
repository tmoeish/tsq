package tsq

import (
	"reflect"
	"strings"

	"github.com/juju/errors"
)

type buildErrorCarrier interface {
	buildError() error
}

type tableColumnLister interface {
	Cols() []AnyColumn
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

func validateColumnInput(col AnyColumn) (Table, error) {
	if isNilValue(col) {
		return nil, errors.New("column cannot be nil")
	}

	if carrier, ok := col.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return nil, errors.Trace(carrier.buildError())
	}

	table := col.Table()
	if isNilValue(table) {
		if name := strings.TrimSpace(col.Name()); name != "" {
			return nil, errors.Errorf("column %s table cannot be nil", name)
		}

		return nil, errors.New("column table cannot be nil")
	}

	if carrier, ok := table.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return nil, errors.Trace(carrier.buildError())
	}

	if err := validateColumnBelongsToTable(col, table); err != nil {
		return nil, errors.Trace(err)
	}

	return table, nil
}

func validateColumnBelongsToTable(col AnyColumn, table Table) error {
	lister, ok := table.(tableColumnLister)
	if !ok {
		return nil
	}

	cols := lister.Cols()
	if len(cols) == 0 {
		return nil
	}

	for _, candidate := range cols {
		if isNilValue(candidate) {
			continue
		}

		if candidate.Name() == col.Name() {
			return nil
		}
	}

	if transformed, ok := col.(transformedColumn); ok && transformed.isTransformedExpression() && col.Name() == "case" {
		return nil
	}

	return errors.Errorf("column %s does not belong to table %s", col.Name(), table.Table())
}

func validateTableInput(table Table, label string) error {
	if isNilValue(table) {
		return errors.Errorf("%s cannot be nil", label)
	}

	if carrier, ok := table.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return errors.Trace(carrier.buildError())
	}

	if strings.TrimSpace(table.Table()) == "" {
		return errors.Errorf("%s name cannot be empty", label)
	}

	return nil
}

func validateConditionInput(cond Condition) (string, map[string]Table, []any, error) {
	if isNilValue(cond) {
		return "", nil, nil, errors.New("condition cannot be nil")
	}

	if carrier, ok := cond.(buildErrorCarrier); ok && carrier.buildError() != nil {
		return "", nil, nil, errors.Trace(carrier.buildError())
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

			return "", nil, nil, errors.Errorf("condition table %s cannot be nil", name)
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
