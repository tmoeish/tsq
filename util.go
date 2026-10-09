package tsq

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
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

func validateIdentifierForDialect(identifier string, d sqld.Dialect) error {
	if err := validateBuiltInIdentifier(identifier); err != nil {
		return err
	}

	return sqld.ValidateIdentifier(d, identifier)
}

// validatePredicateValue rejects values that cannot be compared with = in SQL: NULL
// needs IS NULL, and collections need IN.
func validatePredicateValue(arg any) error {
	errNull := errors.New("NULL is not a comparable value; use IsNull or IsNotNull")

	if isNilValue(arg) {
		return errNull
	}

	if valuer, ok := arg.(driver.Valuer); ok {
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

		return fmt.Errorf("%v is a collection; use In with Vals or a ListParam", v.Type())
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

// bindValueFor is bindValue for the dialect the statement runs on. The MySQL
// driver writes the zero time.Time as '0000-00-00', which MySQL refuses in its
// default mode ("Incorrect datetime value") where PostgreSQL and SQLite store
// year 1: a row whose NOT NULL time field was never set could not be inserted
// there. It is bound as the year 1 it is, and reads back as the zero time.
func bindValueFor(d sqld.Dialect, v any) any {
	bound := bindValue(v)

	if d != nil && d.Name() == sqld.MySQL {
		switch t := bound.(type) {
		case time.Time:
			if t.IsZero() {
				return "0001-01-01 00:00:00"
			}
		case *time.Time:
			if t != nil && t.IsZero() {
				return "0001-01-01 00:00:00"
			}
		}
	}

	// SQLite keeps a time as the text the driver writes, and the two drivers
	// write different text: modernc.org/sqlite Go's String() form ("... +0000
	// UTC"), which mattn/go-sqlite3 reads back as the zero time without an
	// error and SQLite's own date functions read as NULL. TSQ writes the form
	// both drivers and SQLite read, with a fixed six-digit fraction, so that the
	// text of two times compares and sorts as the times do.
	if d != nil && d.Name() == sqld.SQLite {
		if t, ok := bound.(time.Time); ok {
			return t.UTC().Format(sqliteTimeLayout)
		}
	}

	return bound
}

// sqliteTimeLayout is the text a time is bound as on SQLite: what mattn/go-sqlite3
// writes, and modernc.org/sqlite with _time_format=sqlite, at a fixed precision.
const sqliteTimeLayout = "2006-01-02 15:04:05.000000-07:00"

// bindValue is the value TSQ passes to the driver for v. Times go in UTC, whatever
// zone the caller's value is in: SQLite keeps a time as the text of the value, and
// text in two zones does not sort or compare the way the times do.
func bindValue(v any) any {
	// A json.RawMessage that was never set is not JSON: bound as the empty bytes
	// every other byte slice becomes, MySQL and PostgreSQL refused it for a JSON
	// column. It is the JSON null, as encoding/json writes a nil RawMessage.
	// It stays a RawMessage: a driver that writes its parameters into the statement
	// (MySQL's interpolateParams, pgx's simple protocol) spells a plain []byte as
	// binary, which a JSON column refuses.
	if raw, ok := v.(json.RawMessage); ok && len(raw) == 0 {
		return json.RawMessage("null")
	}

	// A []byte field, or one of a named byte-slice type (json.RawMessage), is a
	// NOT NULL column, and its zero value is nil, which the drivers bind as NULL:
	// an Insert that left it unset failed. The nullable form is sql.Null[[]byte].
	if b, ok := v.([]byte); ok && b == nil {
		return []byte{}
	}

	if _, codec := v.(driver.Valuer); !codec {
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 && rv.IsNil() {
			return []byte{}
		}
	}

	if isNilValue(v) {
		return v
	}

	switch x := v.(type) {
	case time.Time:
		return boundTime(x)
	case *time.Time:
		return boundTime(*x)
	case driver.Valuer:
		// sql.Null[[16]byte].Value converts its array with the driver's default
		// converter, which refuses arrays, so the bytes are taken from the field.
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Struct && sqlNullOf(rv.Type(), byteArrayType) {
			if !rv.Field(1).Bool() {
				return nil
			}

			b, _ := byteArrayBytes(rv.Field(0).Interface())

			return b
		}

		if value, err := x.Value(); err == nil {
			if t, ok := value.(time.Time); ok {
				return boundTime(t)
			}
		}

		return v
	}

	if b, ok := byteArrayBytes(v); ok {
		return b
	}

	return v
}

// byteArrayBytes is the bytes of a [N]byte, or of a pointer to one, which the
// drivers bind as a slice and never as an array; ok is false for any other value.
// A codec type keeps its own Value.
func byteArrayBytes(v any) (b []byte, ok bool) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, false
		}

		rv = rv.Elem()
	}

	if !rv.IsValid() || !byteArrayType(rv.Type()) {
		return nil, false
	}

	b = make([]byte, rv.Len())
	reflect.Copy(reflect.ValueOf(b), rv)

	return b, true
}

// byteArrayType reports a [N]byte, or a type of that shape, without a Scan of its
// own: a key kept as its bytes, which database/sql reads and writes only as a slice.
func byteArrayType(t reflect.Type) bool {
	return t != nil && t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 &&
		!reflect.PointerTo(t).Implements(reflect.TypeFor[sql.Scanner]()) &&
		!t.Implements(reflect.TypeFor[driver.Valuer]())
}

// boundTime is t as every statement binds it: in UTC, and cut to the microsecond
// the columns keep. Bound with its nanoseconds, a time was stored whole by SQLite
// (as text), rounded by MySQL and cut by PostgreSQL, so a row and a predicate that
// carried the same instant from another source met in three different ways.
func boundTime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}
