package tsq

import (
	"reflect"
	"strings"

	"github.com/juju/errors"
)

type buildErrorCarrier interface {
	buildError() error
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

func validateColumnInput(col Column) (Table, error) {
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

	return table, nil
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
