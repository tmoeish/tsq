package tsq

import "reflect"

// mysqlErrorNumber returns the server error number of a go-sql-driver/mysql
// *MySQLError in err's chain. The type has no method to match an interface on,
// and importing the driver would put it in every TSQ user's module graph, so the
// Number field is read by reflection.
func mysqlErrorNumber(err error) (uint16, bool) {
	queue := []error{err}

	for len(queue) > 0 {
		e := queue[0]
		queue = queue[1:]

		if e == nil {
			continue
		}

		if n, ok := mysqlNumber(e); ok {
			return n, true
		}

		switch u := e.(type) { //nolint:errorlint // this loop is the chain walk errors.As would do
		case interface{ Unwrap() error }:
			queue = append(queue, u.Unwrap())
		case interface{ Unwrap() []error }:
			queue = append(queue, u.Unwrap()...)
		}
	}

	return 0, false
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
	if n.Kind() != reflect.Uint16 {
		return 0, false
	}

	return uint16(n.Uint()), true
}
