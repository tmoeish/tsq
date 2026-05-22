package tsq

import (
	"context"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
	"github.com/tmoeish/tsq/v4/internal/buildinfo"
)

type (
	VersionInfo              = buildinfo.Info
	Dialect                  = tsqdialect.Dialect
	DialectName              = tsqdialect.Name
	DialectCapability        = tsqdialect.Capability
	DDLAlterColumnMode       = tsqdialect.DDLAlterColumnMode
	DDLColumnKind            = tsqdialect.DDLColumnKind
	DDLColumnType            = tsqdialect.DDLColumnType
	DDLColumnSpec            = tsqdialect.DDLColumnSpec
	IndexDefinition          = tsqdialect.IndexDefinition
	ErrUnsupportedCapability = tsqdialect.ErrUnsupportedCapability
	SQLiteDialect            = tsqdialect.SQLiteDialect
	MySQLDialect             = tsqdialect.MySQLDialect
	PostgresDialect          = tsqdialect.PostgresDialect
)

const (
	DialectMySQL                         = tsqdialect.MySQL
	DialectPostgres                      = tsqdialect.Postgres
	DialectSQLite                        = tsqdialect.SQLite
	DialectUnknown                       = tsqdialect.Unknown
	DialectCapabilityCTE                 = tsqdialect.CapabilityCTE
	DialectCapabilityExcept              = tsqdialect.CapabilityExcept
	DialectCapabilityFullOuterJoin       = tsqdialect.CapabilityFullOuterJoin
	DialectCapabilityIntersect           = tsqdialect.CapabilityIntersect
	DialectCapabilitySelectForUpdate     = tsqdialect.CapabilitySelectForUpdate
	DialectCapabilitySelectForShare      = tsqdialect.CapabilitySelectForShare
	DialectCapabilitySelectForNoWait     = tsqdialect.CapabilitySelectForNoWait
	DialectCapabilitySelectForSkipLocked = tsqdialect.CapabilitySelectForSkipLocked
	DDLAlterColumnDirect                 = tsqdialect.DDLAlterColumnDirect
	DDLAlterColumnRebuild                = tsqdialect.DDLAlterColumnRebuild
	DDLColumnKindBool                    = tsqdialect.DDLColumnKindBool
	DDLColumnKindBytes                   = tsqdialect.DDLColumnKindBytes
	DDLColumnKindFloat                   = tsqdialect.DDLColumnKindFloat
	DDLColumnKindInt                     = tsqdialect.DDLColumnKindInt
	DDLColumnKindString                  = tsqdialect.DDLColumnKindString
	DDLColumnKindTime                    = tsqdialect.DDLColumnKindTime
)

func GetVersionInfo() *VersionInfo {
	return buildinfo.Current()
}

var exportCompatRuntime = &Runtime{
	traceManager: newTraceManager(),
}

func AddTracer(tracer Tracer) {
	exportCompatRuntime.AddTracer(tracer)
}

func ClearTracers() {
	exportCompatRuntime.ClearTracers()
}

func GetTracers() []Tracer {
	return exportCompatRuntime.GetTracers()
}

func Trace(ctx context.Context, fn func(ctx context.Context) error) error {
	return exportCompatRuntime.Trace(ctx, fn)
}

func Trace1[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	return trace1WithRuntime(exportCompatRuntime, ctx, fn)
}

func PrintCost(next TraceFn) TraceFn {
	return printCost(next)
}

func PrintError(next TraceFn) TraceFn {
	return printError(next)
}

func PrintSQL(next TraceFn) TraceFn {
	return printSQLTracer(next)
}
