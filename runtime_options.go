package tsq

import (
	"errors"
	"fmt"
)

// RuntimeOption configures a Runtime while it is being constructed.
//
// Options replaced a single *RuntimeOptions struct pointer passed as a variadic
// argument, which made "no options" and "one options value" the same signature
// and left every field's zero value doing double duty as "unset".
type RuntimeOption func(*runtimeConfig)

// runtimeConfig accumulates the applied options before any of them is validated,
// so a bad value is reported once, from the constructor, rather than per option.
type runtimeConfig struct {
	tablePolicy SchemaPolicy
	indexPolicy SchemaPolicy
	tracers     []Tracer
	logger      Logger
	logSQL      bool
	maxPageSize int
}

// WithSchemaPolicy sets how the runtime manages both tables and indexes.
//
// Use it for the common case; WithTablePolicy and WithIndexPolicy exist for a
// schema whose tables come from migrations while its indexes do not, or the
// reverse.
func WithSchemaPolicy(policy SchemaPolicy) RuntimeOption {
	return func(cfg *runtimeConfig) {
		cfg.tablePolicy = policy
		cfg.indexPolicy = policy
	}
}

// WithTablePolicy sets how the runtime manages declared tables and columns.
func WithTablePolicy(policy SchemaPolicy) RuntimeOption {
	return func(cfg *runtimeConfig) {
		cfg.tablePolicy = policy
	}
}

// WithIndexPolicy sets how the runtime manages declared indexes.
func WithIndexPolicy(policy SchemaPolicy) RuntimeOption {
	return func(cfg *runtimeConfig) {
		cfg.indexPolicy = policy
	}
}

// WithTracers appends to the runtime's tracer chain. Repeated calls accumulate.
func WithTracers(tracers ...Tracer) RuntimeOption {
	return func(cfg *runtimeConfig) {
		cfg.tracers = appendTracers(cfg.tracers, tracers...)
	}
}

// WithLogger sets the logger that receives schema bootstrap decisions, executed
// DDL and execution-time warnings. It defaults to slog.Default().
func WithLogger(logger Logger) RuntimeOption {
	return func(cfg *runtimeConfig) {
		cfg.logger = logger
	}
}

// WithSQLLogging logs every rendered statement and its bound arguments through
// the logger at debug level.
//
// Arguments are logged verbatim, so leave it off wherever query parameters carry
// secrets or personal data.
func WithSQLLogging() RuntimeOption {
	return func(cfg *runtimeConfig) {
		cfg.logSQL = true
	}
}

// WithMaxPageSize caps PageRequest.Size for paged queries run through this
// runtime. It defaults to DefaultMaxPageSize; a size below 1 is an error, not a
// request for the default.
func WithMaxPageSize(size int) RuntimeOption {
	return func(cfg *runtimeConfig) {
		cfg.maxPageSize = size
	}
}

// newRuntimeConfig applies options in order and validates the result.
func newRuntimeConfig(options []RuntimeOption) (*runtimeConfig, error) {
	// The default is set before the options, so an explicit 0 is seen as the
	// mistake it is rather than taken for "not set".
	cfg := &runtimeConfig{maxPageSize: DefaultMaxPageSize}

	for _, option := range options {
		if option == nil {
			return nil, errors.New("runtime option cannot be nil")
		}

		option(cfg)
	}

	cfg.tablePolicy = resolveSchemaPolicy(cfg.tablePolicy)
	if err := validateSchemaPolicy(cfg.tablePolicy); err != nil {
		return nil, err
	}

	cfg.indexPolicy = resolveSchemaPolicy(cfg.indexPolicy)
	if err := validateSchemaPolicy(cfg.indexPolicy); err != nil {
		return nil, err
	}

	if cfg.maxPageSize < 1 {
		return nil, fmt.Errorf("invalid max page size %d: it must be at least 1", cfg.maxPageSize)
	}

	if cfg.logger == nil {
		cfg.logger = defaultRuntimeLogger()
	}

	return cfg, nil
}
