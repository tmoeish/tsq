package parser

// 文件后缀常量
const (
	TSQFileSuffix = "_tsq.go"
	GoFileSuffix  = ".go"
)

// 标签名称常量
const (
	TagDB   = "db"
	TagTSQ  = "tsq"
	TagJSON = "json"
)

// 特殊值常量
const (
	TagIgnore = "-"
)

// 默认字段名常量
const (
	DefaultPKField = "ID"
	DefaultVField  = "V"
	DefaultCTField = "CT"
	DefaultMTField = "MT"
	DefaultDTField = "DT"
)

// Go 原始类型集合
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
