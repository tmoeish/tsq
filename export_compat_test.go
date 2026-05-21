package tsq

import (
	"context"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
	"github.com/tmoeish/tsq/v4/internal/buildinfo"
)

type (
	VersionInfo              = buildinfo.Info
	Dialect                  = tsqdialect.Dialect
	DialectName              = tsqdialect.DialectName
	DialectCapability        = tsqdialect.DialectCapability
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
	DialectMySQL                         = tsqdialect.DialectMySQL
	DialectPostgres                      = tsqdialect.DialectPostgres
	DialectSQLite                        = tsqdialect.DialectSQLite
	DialectUnknown                       = tsqdialect.DialectUnknown
	DialectCapabilityCTE                 = tsqdialect.DialectCapabilityCTE
	DialectCapabilityExcept              = tsqdialect.DialectCapabilityExcept
	DialectCapabilityFullOuterJoin       = tsqdialect.DialectCapabilityFullOuterJoin
	DialectCapabilityIntersect           = tsqdialect.DialectCapabilityIntersect
	DialectCapabilitySelectForUpdate     = tsqdialect.DialectCapabilitySelectForUpdate
	DialectCapabilitySelectForShare      = tsqdialect.DialectCapabilitySelectForShare
	DialectCapabilitySelectForNoWait     = tsqdialect.DialectCapabilitySelectForNoWait
	DialectCapabilitySelectForSkipLocked = tsqdialect.DialectCapabilitySelectForSkipLocked
	DDLAlterColumnDirect                 = tsqdialect.DDLAlterColumnDirect
	DDLAlterColumnRebuild                = tsqdialect.DDLAlterColumnRebuild
	DDLColumnKindBool                    = tsqdialect.DDLColumnKindBool
	DDLColumnKindBytes                   = tsqdialect.DDLColumnKindBytes
	DDLColumnKindFloat                   = tsqdialect.DDLColumnKindFloat
	DDLColumnKindInt                     = tsqdialect.DDLColumnKindInt
	DDLColumnKindString                  = tsqdialect.DDLColumnKindString
	DDLColumnKindTime                    = tsqdialect.DDLColumnKindTime
)

func GetVersion() string {
	return buildinfo.Version()
}

func GetBuildTime() string {
	return buildinfo.BuildTime()
}

func GetGitCommit() string {
	return buildinfo.GitCommit()
}

func GetGitBranch() string {
	return buildinfo.GitBranch()
}

func GetVersionInfo() *VersionInfo {
	return buildinfo.Current()
}

func AddTracer(tracer Tracer) {
	defaultRuntime.AddTracer(tracer)
}

func ClearTracers() {
	defaultRuntime.ClearTracers()
}

func GetTracers() []Tracer {
	return defaultRuntime.GetTracers()
}

func Trace(ctx context.Context, fn func(ctx context.Context) error) error {
	return trace(ctx, fn)
}

func Trace1[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	return trace1(ctx, fn)
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
