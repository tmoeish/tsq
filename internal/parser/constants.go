package parser

const (
	TSQFileSuffix = ".tsq.go"
	GoFileSuffix  = ".go"
)

const (
	TagDB   = "db"
	TagTSQ  = "tsq"
	TagJSON = "json"
)

const (
	TagIgnore = "-"
)

var PrimitiveTypes = map[string]struct{}{
	"bool":       {},
	"string":     {},
	"int":        {},
	"int8":       {},
	"int16":      {},
	"int32":      {},
	"int64":      {},
	"uint":       {},
	"uint8":      {},
	"uint16":     {},
	"uint32":     {},
	"uint64":     {},
	"uintptr":    {},
	"byte":       {},
	"rune":       {},
	"float32":    {},
	"float64":    {},
	"complex64":  {},
	"complex128": {},
}
