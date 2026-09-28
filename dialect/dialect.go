// Package dialect names the SQL engines TSQ supports and what each can do, and
// describes table columns independently of any engine.
//
// TSQ supports MySQL, PostgreSQL and SQLite, and nothing else: the constructs the
// three spell differently are chosen inside the library by Name. This package
// therefore holds names and facts, not an interface to implement.
package dialect

import (
	"fmt"
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

// The optional features TSQ checks when a statement runs. Each is named after the
// builder method that needs it, and its value is the SQL an error names.
const (
	CapabilityCTE          Capability = "CTE"
	CapabilityExcept       Capability = "EXCEPT"
	CapabilityExceptAll    Capability = "EXCEPT ALL"
	CapabilityFullJoin     Capability = "FULL JOIN"
	CapabilityIntersect    Capability = "INTERSECT"
	CapabilityIntersectAll Capability = "INTERSECT ALL"
	CapabilityForUpdate    Capability = "FOR UPDATE"
	CapabilityForShare     Capability = "FOR SHARE"
	CapabilityNoWait       Capability = "NOWAIT"
	CapabilitySkipLocked   Capability = "SKIP LOCKED"
	// CapabilityFullTextSearch reports a full-text index and a matching predicate.
	// Where it is missing, TSQ matches the term as a substring instead, which finds
	// different rows: no stemming, no ranking, and no word boundaries.
	CapabilityFullTextSearch Capability = "FULL TEXT SEARCH"
)

// capabilities is each engine's position on every capability.
var capabilities = map[Name]map[Capability]bool{
	// Baseline is MySQL 8.0 (5.7 reached end of life in 2023-10): CTEs since 8.0,
	// INTERSECT/EXCEPT since 8.0.31. FULL OUTER JOIN is still absent in MySQL 8.
	MySQL: {
		CapabilityCTE:            true,
		CapabilityExcept:         true,
		CapabilityExceptAll:      true,
		CapabilityFullJoin:       false,
		CapabilityIntersect:      true,
		CapabilityIntersectAll:   true,
		CapabilityForUpdate:      true,
		CapabilityForShare:       true,
		CapabilityNoWait:         true,
		CapabilitySkipLocked:     true,
		CapabilityFullTextSearch: true,
	},
	// PostgreSQL supports all of them at every version TSQ targets, but the entries
	// are still spelled out: a future capability must be an explicit decision here
	// too, not something PostgreSQL inherits by being the permissive one.
	Postgres: {
		CapabilityCTE:            true,
		CapabilityExcept:         true,
		CapabilityExceptAll:      true,
		CapabilityFullJoin:       true,
		CapabilityIntersect:      true,
		CapabilityIntersectAll:   true,
		CapabilityForUpdate:      true,
		CapabilityForShare:       true,
		CapabilityNoWait:         true,
		CapabilitySkipLocked:     true,
		CapabilityFullTextSearch: true,
	},
	// Baseline is SQLite 3.39 (2022-06), which is when FULL OUTER JOIN landed.
	// SQLite has no row-level locking at all: it serializes writers instead, and
	// its INTERSECT and EXCEPT have no ALL form.
	SQLite: {
		CapabilityCTE:            true,
		CapabilityExcept:         true,
		CapabilityExceptAll:      false,
		CapabilityFullJoin:       true,
		CapabilityIntersect:      true,
		CapabilityIntersectAll:   false,
		CapabilityForUpdate:      false,
		CapabilityForShare:       false,
		CapabilityNoWait:         false,
		CapabilitySkipLocked:     false,
		CapabilityFullTextSearch: false,
	},
}

// Supports reports whether engine supports capability. An unknown engine or
// capability is unsupported.
func Supports(engine Name, capability Capability) bool {
	supported, declared := capabilities[engine][capability]

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

	return &UnsupportedCapabilityError{Capability: capability, Dialect: engine}
}

func (e *UnsupportedCapabilityError) Error() string {
	engine := string(e.Dialect)
	if engine == "" {
		engine = "unknown"
	}

	return fmt.Sprintf("operation %s is not supported by %s dialect; %s",
		e.Capability, engine, capabilityHint(e.Capability))
}

func capabilityHint(capability Capability) string {
	switch capability {
	case CapabilityCTE:
		return "use a subquery or split the query"
	case CapabilityFullJoin:
		return "use LEFT/RIGHT JOIN with UNION, or execute on sqlite/postgres"
	case CapabilityIntersect:
		return "use IN/EXISTS filtering"
	case CapabilityExcept:
		return "use NOT EXISTS filtering"
	case CapabilityIntersectAll, CapabilityExceptAll:
		return "use the distinct form (Intersect / Except) if duplicates need not be kept"
	case CapabilityForUpdate, CapabilityForShare:
		return "execute on a dialect that supports row-locking reads"
	case CapabilityNoWait, CapabilitySkipLocked:
		return "execute on a dialect that supports row-lock wait modifiers"
	default:
		return "use a simpler query shape or a dialect that supports this capability"
	}
}
