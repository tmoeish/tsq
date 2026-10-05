package tsq

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// scanAdapter wraps the pointer to a field in a sql.Scanner, for a field type
// database/sql does not read from every driver. Most types need none.
type scanAdapter func(field any) any

// scanAdapterFor picks the adapter of a field type, once, when its column is
// declared:
//
//   - a time. SQLite keeps a time as text, and its drivers hand back a time.Time
//     only for a column declared as one: MAX(created_at), COALESCE(seen_at, ...) and
//     every other expression came back as a string, which database/sql does not
//     put into a time.Time.
//   - a named bool (type Flag bool). MySQL and SQLite report a boolean as an
//     integer, which database/sql converts for a bool and for nothing named after
//     one.
func scanAdapterFor(field reflect.Type) scanAdapter {
	switch field {
	case reflect.TypeFor[time.Time]():
		return func(p any) any { return (*timeDest)(p.(*time.Time)) }
	case reflect.TypeFor[*time.Time]():
		return func(p any) any { return timePointerDest{p.(**time.Time)} }
	case reflect.TypeFor[sql.NullTime]():
		return func(p any) any { return (*nullTimeDest)(p.(*sql.NullTime)) }
	case reflect.TypeFor[sql.Null[time.Time]]():
		return func(p any) any { return (*nullOfTimeDest)(p.(*sql.Null[time.Time])) }
	}

	switch {
	case namedBool(field):
		return func(p any) any { return boolDest{field: reflect.ValueOf(p).Elem()} }
	case field.Kind() == reflect.Pointer && namedBool(field.Elem()):
		return func(p any) any { return boolDest{field: reflect.ValueOf(p).Elem(), form: boolPointer} }
	case field.Kind() == reflect.Struct && field.PkgPath() == "database/sql" && strings.HasPrefix(field.Name(), "Null[") &&
		field.NumField() == 2 && namedBool(field.Field(0).Type):
		return func(p any) any { return boolDest{field: reflect.ValueOf(p).Elem(), form: boolNull} }
	}

	return nil
}

func namedBool(t reflect.Type) bool {
	return t.Kind() == reflect.Bool && t != reflect.TypeFor[bool]()
}

// timeTextLayouts are the spellings a time has where it reaches TSQ as text: what
// the two SQLite drivers write, what SQLite's own CURRENT_TIMESTAMP and date
// functions give, and MySQL's DATETIME on a pool opened without parseTime.
var timeTextLayouts = []string{
	"2006-01-02 15:04:05.999999999 -0700 MST", // time.Time.String, modernc.org/sqlite
	"2006-01-02 15:04:05.999999999-07:00",     // mattn/go-sqlite3
	"2006-01-02T15:04:05.999999999-07:00",
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

func parseTimeText(text string) (time.Time, error) {
	// time.Time.String appends the monotonic clock reading of a time that has one.
	if i := strings.Index(text, " m="); i >= 0 {
		text = text[:i]
	}

	for _, layout := range timeTextLayouts {
		if t, err := time.Parse(layout, text); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("cannot read %q as a time", text)
}

// scanTime reads a driver value as a time; null reports SQL NULL.
func scanTime(src any) (t time.Time, null bool, err error) {
	switch v := src.(type) {
	case nil:
		return time.Time{}, true, nil
	case time.Time:
		return v, false, nil
	case string:
		t, err = parseTimeText(v)
	case []byte:
		t, err = parseTimeText(string(v))
	default:
		err = fmt.Errorf("unsupported Scan, storing driver.Value type %T into type *time.Time", src)
	}

	return t, false, err
}

type timeDest time.Time

func (d *timeDest) Scan(src any) error {
	t, null, err := scanTime(src)
	if err != nil {
		return err
	}

	if null {
		return fmt.Errorf("converting NULL to time.Time is unsupported")
	}

	*d = timeDest(t)

	return nil
}

type timePointerDest struct{ p **time.Time }

func (d timePointerDest) Scan(src any) error {
	t, null, err := scanTime(src)
	if err != nil {
		return err
	}

	if null {
		*d.p = nil

		return nil
	}

	*d.p = &t

	return nil
}

type nullTimeDest sql.NullTime

func (d *nullTimeDest) Scan(src any) error {
	t, null, err := scanTime(src)
	if err != nil {
		return err
	}

	*d = nullTimeDest{Time: t, Valid: !null}

	return nil
}

type nullOfTimeDest sql.Null[time.Time]

func (d *nullOfTimeDest) Scan(src any) error {
	t, null, err := scanTime(src)
	if err != nil {
		return err
	}

	*d = nullOfTimeDest{V: t, Valid: !null}

	return nil
}

type boolForm uint8

const (
	boolValue   boolForm = iota // type Flag bool
	boolPointer                 // *Flag
	boolNull                    // sql.Null[Flag]
)

// boolDest reads a boolean into a named bool field, in any of its nullable forms.
type boolDest struct {
	field reflect.Value
	form  boolForm
}

func (d boolDest) Scan(src any) error {
	target := d.field

	if src == nil {
		if d.form == boolValue {
			return fmt.Errorf("converting NULL to %s is unsupported", target.Type())
		}

		target.SetZero()

		return nil
	}

	value, err := driver.Bool.ConvertValue(src)
	if err != nil {
		return fmt.Errorf("converting driver.Value type %T to %s: %w", src, target.Type(), err)
	}

	switch d.form {
	case boolPointer:
		fresh := reflect.New(target.Type().Elem())
		target.Set(fresh)
		target = fresh.Elem()
	case boolNull:
		target.Field(1).SetBool(true)
		target = target.Field(0)
	}

	target.SetBool(value.(bool))

	return nil
}
