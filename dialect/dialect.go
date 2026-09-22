// Package dialect names the SQL engines TSQ supports and what each can do, and
// describes table columns independently of any engine.
//
// TSQ supports MySQL, PostgreSQL and SQLite, and nothing else: the constructs the
// three spell differently are chosen inside the library by Name. This package
// therefore holds names and facts, not an interface to implement.
package dialect

import (
	"fmt"
	"strings"
)

// Name identifies one of the supported SQL engines.
type Name string

// The supported engines.
const (
	MySQL    Name = "mysql"
	Postgres Name = "postgres"
	SQLite   Name = "sqlite"
)

// Capability is an optional SQL feature an engine may or may not support.
type Capability string

// The optional features TSQ checks when a statement runs.
const (
	CapabilityCTE                 Capability = "CTE"
	CapabilityExcept              Capability = "EXCEPT"
	CapabilityFullOuterJoin       Capability = "FULL_OUTER_JOIN"
	CapabilityIntersect           Capability = "INTERSECT"
	CapabilitySelectForUpdate     Capability = "SELECT_FOR_UPDATE"
	CapabilitySelectForShare      Capability = "SELECT_FOR_SHARE"
	CapabilitySelectForNoWait     Capability = "SELECT_FOR_NOWAIT"
	CapabilitySelectForSkipLocked Capability = "SELECT_FOR_SKIP_LOCKED"
	// CapabilityFullTextSearch reports a full-text index and a matching predicate.
	// Where it is missing, TSQ matches the term as a substring instead, which finds
	// different rows: no stemming, no ranking, and no word boundaries.
	CapabilityFullTextSearch Capability = "FULL_TEXT_SEARCH"
)

// capabilities is each engine's position on every capability.
var capabilities = map[Name]map[Capability]bool{
	// Baseline is MySQL 8.0 (5.7 reached end of life in 2023-10): CTEs since 8.0,
	// INTERSECT/EXCEPT since 8.0.31. FULL OUTER JOIN is still absent in MySQL 8.
	MySQL: {
		CapabilityCTE:                 true,
		CapabilityExcept:              true,
		CapabilityFullOuterJoin:       false,
		CapabilityIntersect:           true,
		CapabilitySelectForUpdate:     true,
		CapabilitySelectForShare:      true,
		CapabilitySelectForNoWait:     true,
		CapabilitySelectForSkipLocked: true,
		CapabilityFullTextSearch:      true,
	},
	// PostgreSQL supports all of them at every version TSQ targets, but the entries
	// are still spelled out: a future capability must be an explicit decision here
	// too, not something PostgreSQL inherits by being the permissive one.
	Postgres: {
		CapabilityCTE:                 true,
		CapabilityExcept:              true,
		CapabilityFullOuterJoin:       true,
		CapabilityIntersect:           true,
		CapabilitySelectForUpdate:     true,
		CapabilitySelectForShare:      true,
		CapabilitySelectForNoWait:     true,
		CapabilitySelectForSkipLocked: true,
		CapabilityFullTextSearch:      true,
	},
	// Baseline is SQLite 3.39 (2022-06), which is when FULL OUTER JOIN landed.
	// SQLite has no row-level locking at all: it serializes writers instead.
	SQLite: {
		CapabilityCTE:                 true,
		CapabilityExcept:              true,
		CapabilityFullOuterJoin:       true,
		CapabilityIntersect:           true,
		CapabilitySelectForUpdate:     false,
		CapabilitySelectForShare:      false,
		CapabilitySelectForNoWait:     false,
		CapabilitySelectForSkipLocked: false,
		CapabilityFullTextSearch:      false,
	},
}

// Supports reports whether engine supports capability. An unknown engine or
// capability is unsupported.
func Supports(engine Name, capability Capability) bool {
	supported, declared := capabilities[engine][canonicalCapability(string(capability))]

	return declared && supported
}

// UnsupportedCapabilityError reports a statement that needs a capability its
// engine lacks. TSQ returns it when the statement runs, not when it is built.
type UnsupportedCapabilityError struct {
	// Capability is what the statement needs.
	Capability Capability
	// Dialect is the engine that lacks it.
	Dialect Name
}

// Check returns an *UnsupportedCapabilityError when engine lacks capability, and
// nil otherwise.
func Check(engine Name, capability Capability) error {
	if Supports(engine, capability) {
		return nil
	}

	return &UnsupportedCapabilityError{Capability: canonicalCapability(string(capability)), Dialect: engine}
}

func (e *UnsupportedCapabilityError) Error() string {
	engine := string(e.Dialect)
	if engine == "" {
		engine = "unknown"
	}

	return fmt.Sprintf("operation %s is not supported by %s dialect; %s",
		displayCapability(e.Capability), engine, capabilityHint(e.Capability))
}

// canonicalCapability maps the SQL spellings users write to a Capability.
func canonicalCapability(operation string) Capability {
	value := strings.ToUpper(strings.TrimSpace(operation))

	switch value {
	case "FULL JOIN", "FULL OUTER JOIN":
		return CapabilityFullOuterJoin
	case "EXCEPT", "MINUS":
		return CapabilityExcept
	case "FOR UPDATE":
		return CapabilitySelectForUpdate
	case "FOR SHARE":
		return CapabilitySelectForShare
	case "NOWAIT":
		return CapabilitySelectForNoWait
	case "SKIP LOCKED":
		return CapabilitySelectForSkipLocked
	default:
		return Capability(value)
	}
}

func displayCapability(capability Capability) string {
	switch capability {
	case CapabilityFullOuterJoin:
		return "FULL JOIN"
	case CapabilitySelectForUpdate:
		return "FOR UPDATE"
	case CapabilitySelectForShare:
		return "FOR SHARE"
	case CapabilitySelectForNoWait:
		return "NOWAIT"
	case CapabilitySelectForSkipLocked:
		return "SKIP LOCKED"
	default:
		return string(capability)
	}
}

func capabilityHint(capability Capability) string {
	switch capability {
	case CapabilityCTE:
		return "use a subquery or split the query"
	case CapabilityFullOuterJoin:
		return "use LEFT/RIGHT JOIN with UNION, or execute on sqlite/postgres"
	case CapabilityIntersect:
		return "use IN/EXISTS filtering"
	case CapabilityExcept:
		return "use NOT EXISTS filtering"
	case CapabilitySelectForUpdate, CapabilitySelectForShare:
		return "execute on a dialect that supports row-locking reads"
	case CapabilitySelectForNoWait, CapabilitySelectForSkipLocked:
		return "execute on a dialect that supports row-lock wait modifiers"
	default:
		return "use a simpler query shape or a dialect that supports this capability"
	}
}
