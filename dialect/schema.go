package dialect

// ColumnKind is the portable family of a column type; each engine maps it, with
// ColumnType's size and sign details, to its own SQL type.
type ColumnKind string

// The column kinds.
const (
	KindBool   ColumnKind = "bool"
	KindBytes  ColumnKind = "bytes"
	KindFloat  ColumnKind = "float"
	KindInt    ColumnKind = "int"
	KindString ColumnKind = "string"
	KindTime   ColumnKind = "time"
)

// ColumnType describes a column type independently of any engine. RawType, when
// set, is used verbatim instead of the mapping from Kind.
type ColumnType struct {
	Kind     ColumnKind
	Bits     int
	Unsigned bool
	Nullable bool
	Size     int
	RawType  string
}

// ColumnSpec describes one table column as generated code declares it.
type ColumnSpec struct {
	Name          string
	Type          ColumnType
	PrimaryKey    bool
	AutoIncrement bool
	Default       string
	// Fill says who provides the value of the column.
	Fill Fill
	// Generated is the expression of a generated column, empty otherwise.
	Generated string
}

// Fill says who provides a column's value.
type Fill uint8

const (
	// FillCaller is the default: the value comes from the row being written.
	FillCaller Fill = iota
	// FillDefault lets the database apply its DEFAULT when the field is unset.
	FillDefault
	// FillGenerated is a column the database computes; it is never written.
	FillGenerated
)
