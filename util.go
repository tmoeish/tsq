package tsq

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/go-sql-driver/mysql"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

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

func validateBuiltInIdentifier(name string) error {
	if !builtInIdentifierPattern.MatchString(name) {
		return fmt.Errorf("invalid SQL identifier %q (must match [A-Za-z_][A-Za-z0-9_]*)", name)
	}

	return nil
}

func validateIdentifierForDialect(identifier string, d tsqdialect.Dialect) error {
	if err := validateBuiltInIdentifier(identifier); err != nil {
		return err
	}

	return tsqdialect.ValidateIdentifier(d, identifier)
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return mysqlErr.Number == 1062
	}

	return isSQLiteDuplicateKeyError(err) || isPostgresDuplicateKeyError(err)
}

func normalizePageReq(page *PageRequest) *PageRequest {
	return normalizePageReqWithLimit(page, DefaultMaxPageSize)
}

func normalizePageReqWithLimit(page *PageRequest, maxSize int) *PageRequest {
	if page == nil {
		page = &PageRequest{}
	}

	normalized := *page
	normalized.Normalize(maxSize)

	return &normalized
}

// validatePredicateValue rejects values that cannot be compared with = in SQL: NULL
// needs IS NULL, and collections need IN.
func validatePredicateValue(arg any) error {
	errNull := errors.New("NULL is not a comparable value; use IsNull or IsNotNull")

	if isNilValue(arg) {
		return errNull
	}

	if valuer, ok := arg.(interface{ Value() (any, error) }); ok {
		value, err := valuer.Value()
		if err != nil {
			return fmt.Errorf("evaluate %T: %w", arg, err)
		}

		if value == nil {
			return errNull
		}

		return nil
	}

	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return errNull
		}

		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}

		return fmt.Errorf("%v is a collection; use InVal or a ListParam", v.Type())
	case reflect.Map:
		return fmt.Errorf("%v is not a comparable SQL value", v.Type())
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			return nil
		}

		return fmt.Errorf("%v is not a comparable SQL value", v.Type())
	default:
		return nil
	}
}
