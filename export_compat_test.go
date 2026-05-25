package tsq

import (
	"context"
	"log/slog"

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

var exportCompatRuntime = &Runtime{}

func AddTracer(tracer Tracer) {
	if tracer == nil {
		return
	}
	if len(exportCompatRuntime.tracers) >= maxTracers {
		slog.Warn("maximum tracer limit reached", "limit", maxTracers)
		return
	}

	exportCompatRuntime.tracers = append(exportCompatRuntime.tracers, tracer)
}

func ClearTracers() {
	exportCompatRuntime.tracers = nil
}

func Trace1[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	return trace1WithRuntime(exportCompatRuntime, ctx, fn)
}

func PrintCost(next func(ctx context.Context) error) func(ctx context.Context) error {
	return printCost(next)
}

func PrintError(next func(ctx context.Context) error) func(ctx context.Context) error {
	return printError(next)
}

func PrintSQL(next func(ctx context.Context) error) func(ctx context.Context) error {
	return printSQLTracer(next)
}
