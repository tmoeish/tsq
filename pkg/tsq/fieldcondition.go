package tsq

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

type ICond interface {
	Tables() map[string]Table
	Clause() string
}

type Cond struct {
	tables map[string]Table
	clause string
}

func And(conds ...ICond) Cond {
	tables := make(map[string]Table)
	var clauses []string
	for _, c := range conds {
		for tn, t := range c.Tables() {
			tables[tn] = t
		}
		clauses = append(clauses, c.Clause())
	}

	return Cond{
		tables: tables,
		clause: "(" + strings.Join(clauses, " AND ") + ")",
	}
}

func Or(conds ...ICond) Cond {
	tables := make(map[string]Table)
	var clauses []string
	for _, c := range conds {
		for tn, t := range c.Tables() {
			tables[tn] = t
		}
		clauses = append(clauses, c.Clause())
	}

	return Cond{
		tables: tables,
		clause: "(" + strings.Join(clauses, " OR ") + ")",
	}
}

func (c Cond) Tables() map[string]Table {
	return c.tables
}

func (c Cond) Clause() string {
	return c.clause
}

func (f Column[T]) EQVar() Cond {
	return f.OpVar("=")
}

func (f Column[T]) GTVar() Cond {
	return f.OpVar(">")
}

func (f Column[T]) GETVar() Cond {
	return f.OpVar(">=")
}

func (f Column[T]) LTVar() Cond {
	return f.OpVar("<")
}

func (f Column[T]) LETVar() Cond {
	return f.OpVar("<=")
}

func (f Column[T]) LikeVar() Cond {
	return f.OpVar(" LIKE ")
}

func (f Column[T]) NotLikeVar() Cond {
	return f.OpVar(" NOT LIKE ")
}

func (f Column[T]) NEVar() Cond {
	return f.OpVar("<>")
}

func (f Column[T]) OpVar(op string) Cond {
	return Cond{
		tables: map[string]Table{
			f.table.Table(): f.table,
		},
		clause: f.FullName() + op + "?",
	}
}

func (f Column[T]) In(args ...T) Cond {
	arr := make([]string, len(args))
	for i := range len(args) {
		s, err := mysqlVal(args[i])
		if err != nil {
			panic(err)
		}
		arr[i] = s
	}

	return Cond{
		tables: map[string]Table{
			f.table.Table(): f.table,
		},
		clause: f.FullName() + " IN (" + strings.Join(arr, ", ") + ")",
	}
}

func (f Column[T]) InSubQuery(sqb *Query) Cond {
	// TODO indent
	sqLines := strings.Split(sqb.listQuery, "\n")
	for i := range len(sqLines) {
		sqLines[i] = "\t" + sqLines[i]
	}

	return Cond{
		tables: map[string]Table{
			f.table.Table(): f.table,
		},
		clause: f.FullName() + fmt.Sprintf(" IN (\n%s\n)", strings.Join(sqLines, "\n")),
	}
}

func (f Column[T]) EQ(arg T) Cond {
	return f.condWithConst("=", arg)
}

func (f Column[T]) NE(arg T) Cond {
	return f.condWithConst("<>", arg)
}

func (f Column[T]) EQField(arg Column[T]) Cond {
	return f.condWithField("=", arg)
}

func (f Column[T]) NEField(arg Column[T]) Cond {
	return f.condWithField("=", arg)
}

func (f Column[T]) condWithField(op string, field Column[T]) Cond {
	c := Cond{
		tables: make(map[string]Table),
		clause: f.FullName() + op + field.FullName(),
	}
	c.tables[f.table.Table()] = f.table
	c.tables[field.Table().Table()] = field.Table()

	return c
}

func (f Column[T]) condWithConst(op string, con T) Cond {
	s, err := mysqlVal(con)
	if err != nil {
		panic(err)
	}

	c := Cond{
		tables: make(map[string]Table),
		clause: f.FullName() + op + s,
	}
	c.tables[f.table.Table()] = f.table

	return c
}

func mysqlVal(arg any) (string, error) {
	valuer, ok := arg.(driver.Valuer)
	if ok {
		val, err := valuer.Value()
		if err != nil {
			return "", err
		}
		return mysqlVal(val)
	}

	var buf []byte
	switch v := arg.(type) {
	case int64:
		buf = strconv.AppendInt(buf, v, 10)
	case uint64:
		// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
		buf = strconv.AppendUint(buf, v, 10)
	case float64:
		buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
	case bool:
		if v {
			buf = append(buf, '1')
		} else {
			buf = append(buf, '0')
		}
	// case time.Time:
	//	if v.IsZero() {
	//		buf = append(buf, "'0000-00-00'"...)
	//	} else {
	//		buf = append(buf, '\'')
	//		buf, err = appendDateTime(buf, v.In(mc.cfg.Loc))
	//		if err != nil {
	//			return "", err
	//		}
	//		buf = append(buf, '\'')
	//	}
	// case json.RawMessage:
	//	buf = append(buf, '\'')
	//	if mc.status&statusNoBackslashEscapes == 0 {
	//		buf = escapeBytesBackslash(buf, v)
	//	} else {
	//		buf = escapeBytesQuotes(buf, v)
	//	}
	//	buf = append(buf, '\'')
	// case []byte:
	//	if v == nil {
	//		buf = append(buf, "NULL"...)
	//	} else {
	//		buf = append(buf, "_binary'"...)
	//		if mc.status&statusNoBackslashEscapes == 0 {
	//			buf = escapeBytesBackslash(buf, v)
	//		} else {
	//			buf = escapeBytesQuotes(buf, v)
	//		}
	//		buf = append(buf, '\'')
	//	}
	case string:
		buf = append(buf, '\'')
		//if mc.status&statusNoBackslashEscapes == 0 {
		//	buf = escapeStringBackslash(buf, v)
		//} else {
		buf = escapeStringQuotes(buf, v)
		//}
		buf = append(buf, '\'')
	default:
		return fmt.Sprintf("%v", arg), nil
	}
	return string(buf), nil
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := range len(v) {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}
