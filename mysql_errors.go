package tsq

import "reflect"

// mysqlErrorNumber returns the server error number of a go-sql-driver/mysql
// *MySQLError in err's chain. The type has no method to match an interface on,
// and importing the driver would put it in every TSQ user's module graph, so the
// Number field is read by reflection.
func mysqlErrorNumber(err error) (uint16, bool) {
	for _, candidate := range errorChain(err) {
		if n, ok := mysqlNumber(candidate); ok {
			return n, true
		}
	}

	return 0, false
}

// errorChain flattens err and everything it wraps, the way errors.As walks it. The
// drivers whose error types carry codes in fields rather than methods need the walk
// without a target type to match on.
func errorChain(err error) []error {
	var (
		chain []error
		queue = []error{err}
	)

	for len(queue) > 0 {
		e := queue[0]
		queue = queue[1:]

		if e == nil {
			continue
		}

		chain = append(chain, e)

		switch u := e.(type) { //nolint:errorlint // this loop is the walk errors.As would do
		case interface{ Unwrap() error }:
			queue = append(queue, u.Unwrap())
		case interface{ Unwrap() []error }:
			queue = append(queue, u.Unwrap()...)
		}
	}

	return chain
}

func mysqlNumber(e error) (uint16, bool) {
	v := reflect.ValueOf(e)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Struct {
		return 0, false
	}

	t := v.Elem().Type()
	if t.Name() != "MySQLError" || t.PkgPath() != "github.com/go-sql-driver/mysql" {
		return 0, false
	}

	n := v.Elem().FieldByName("Number")
	if !n.IsValid() {
		return 0, false
	}

	return reflect.TypeAssert[uint16](n)
}
