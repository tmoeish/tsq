package tsq

// Constraints for generic type parameters used throughout the package

// Scannable is a constraint for types that can be scanned from database results
// This improves type safety by explicitly declaring which types are safe to scan
// Note: In Go 1.18+, we can use constraints.Ordered for numeric/string types
// For now we use any, but this documents the intended constraint pattern
type Scannable any

// Comparable is a constraint for types that support comparison operations
// Used in condition building and sorting contexts
type Comparable any

// SQLValue is a constraint for types that can be used as SQL values
// This includes primitives, strings, and pointer types
type SQLValue any

// TableRow is a constraint for types that represent database rows
// Typically structs with database field mappings
// This documents the pattern for result scanning
type TableRow any
