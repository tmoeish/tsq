package tsq

// MIGRATION_GUIDE.md - Migration Guide for TSQ Query Builder
//
// This document provides migration paths for users coming from older versions
// or other query builders.

/*

# TSQ Query Builder Migration Guide

## Version Compatibility Matrix

| Version | Status | Notes |
|---------|--------|-------|
| 1.0.x   | Legacy | Use version 2.0+ |
| 2.0.x   | Current | All features working |
| 2.1+    | Current | With error handling improvements |

## Key Changes in Phase 4

### 1. Error State Short-Circuiting (Task 1)

**New Behavior**: Methods no longer execute after an error is set.

#### Old Pattern
```go
qb := Select(invalidCol).
	From(table1).
    Join(table2, col1.EQCol(col2)).
    GroupBy(col3)
// All methods execute even though invalidCol is bad
```

#### New Pattern
```go
qb := Select(invalidCol).
	From(table1).
    Join(table2, col1.EQCol(col2)). // Short-circuits, doesn't execute
    GroupBy(col3)       // Short-circuits, doesn't execute

// Check error after building
if qb.buildErr != nil {
    return fmt.Errorf("query build failed: %w", err)
}
```

**Migration**: No code changes needed; this is transparent. However, you should
add error checking after building queries where you weren't before.

### 2. Pagination Bounds Checking (Task 2)

**New Behavior**: Offset calculation includes overflow protection.

#### Old Pattern
```go
pageReq := &PageReq{Page: 1000000, Size: 1000}
offset := pageReq.Offset()
// Could overflow if values are very large
```

#### New Pattern
```go
pageReq := &PageReq{Page: 1000000, Size: 1000}
offset := pageReq.Offset()
// Safe; returns 0 if overflow would occur
```

**Migration**: If you were manually calculating offsets with multiplication,
consider using PageReq.Offset() instead.

### 3. Dialect Validation (Task 3)

**New Behavior**: FULL OUTER JOIN validation by dialect.

#### Old Pattern
```go
// SQL generation succeeds, but execution still depends on dialect support.
qb := Select(col1).From(table1).FullJoin(table2, col1.EQCol(col2))
```

#### New Pattern
```go
// For MySQL, this is caught early
validator := NewDialectValidator(mysqlDialect)
if err := validator.ValidateCapability("FULL_OUTER_JOIN"); err != nil {
    return err  // Operation not supported
}
```

**Migration**: Existing code continues to work. To enable validation,
use NewDialectValidator before operations.

### 4. SQL Caching (Task 6)

**New Feature**: Optional LRU cache for compiled queries.

#### Usage
```go
cache := NewSQLRenderCache(SQLCacheConfig{
    Enabled: true,
    MaxSize: 1000,
})

// Cache is used internally if enabled
sql, err := query.Build()

// Monitor effectiveness
stats := cache.Stats()
if stats["hit_rate"] < 0.5 {
    cache.Clear()  // Disable caching
}
```

**Migration**: Caching is disabled by default. Enable only if you have
high-volume query building that profiles show as a bottleneck.

### 5. Field Pointer Safety (Task 7)

**New Behavior**: Better error messages for field pointer issues.

#### Old Pattern
```go
col := NewCol[int](table, "id", "id", func(h any) any {
    return &h.(*User).ID  // Panics if h is nil
})
```

#### New Pattern
```go
col := NewCol[int](table, "id", "id", func(h any) any {
    if h == nil {
        return nil  // Handle nil safely
    }
    u, ok := h.(*User)
    if !ok {
        return nil  // Handle type mismatch
    }
    return &u.ID
})

// Or use Into() with validation
err := FieldPointerValidator("id", col.FieldPointer())
if err != nil {
    return fmt.Errorf("invalid field pointer: %w", err)
}
```

**Migration**: Update field pointers to handle edge cases. This prevents
runtime panics.

### 6. Error Messages (Task 11)

**New Behavior**: More actionable error messages.

#### Old Pattern
```
Error: "unknown sort field: email"
Error: "ambiguous sort field: id"
Error: "ORDER BY fields count(3) and ORDER directions count(2) mismatch"
```

#### New Pattern
```
Error: "sort field \"email\" is not in selected columns; available sort fields 
        should be from SELECT clause"
Error: "sort field \"id\" matches multiple columns; use qualified name 
        (table.column) or add alias to disambiguate"
Error: "ORDER BY count mismatch: got 3 sort fields and 2 sort directions; 
        counts must match (provide one direction per field or none for defaults)"
```

**Migration**: No code changes needed. Error messages are more helpful for
debugging and user feedback.

### 7. Dialect Extensibility (Task 13)

**New Feature**: Custom dialect registration.

#### Usage
```go
// Register custom dialect
ext := DialectExtension{
    Name: "firebird",
    Keywords: []string{"SELECT", "FROM", ...},
    Capabilities: map[string]bool{
        "FULL_OUTER_JOIN": false,
        "CTE": true,
    },
}

ExtendDialect(ext)

// Now validate against custom dialect
registry := GetKeywordRegistry()
if !registry.HasCapability("firebird", "FULL_OUTER_JOIN") {
    // Handle unsupported operation
}
```

**Migration**: Existing code unchanged. New code can register custom dialects
for better validation.

## Breaking Changes

### None in Phase 4

The Phase 4 updates are fully backward compatible. Existing code continues
to work without modifications.

### Deprecations

The following patterns are discouraged but still work:

1. **Building queries with errors**: While queries now short-circuit on error,
   it's better to check for errors explicitly rather than rely on silent
   short-circuiting.

2. **Manual offset calculation**: Use PageReq.Offset() instead of
   manual multiplication to handle edge cases.

3. **Ignoring type mismatches in field pointers**: Always validate and handle
   type mismatches gracefully.

## Migration Checklist

When upgrading to Phase 4:

- [ ] Review error handling code
  - Add error checks after query building
  - Handle specific error types appropriately

- [ ] Update field pointers
  - Add nil checks
  - Add type assertions with fallback
  - Validate with FieldPointerValidator

- [ ] Enable features as needed
  - Enable SQL caching only if profiling shows benefit
  - Register custom dialects if using non-standard databases
  - Use dialect validation for safer queries

- [ ] Update error handling in logging
  - Use error context for debugging
  - Log panics with context if they occur
  - Monitor performance with cache stats

- [ ] Test with edge cases
  - Zero/negative page numbers
  - Very large page numbers
  - Empty result sets
  - Type mismatches

## Rollback Plan

If you need to rollback from Phase 4:

1. **Error state short-circuiting is transparent**: No code changes needed
   to revert. Queries will just continue to execute after errors (old behavior).

2. **Pagination overflow protection is transparent**: No code changes needed.
   The overflow check is a safety addition.

3. **Dialect validation is optional**: Simply don't use NewDialectValidator
   if you need the old behavior.

4. **SQL caching is optional**: Disabled by default, no impact if not used.

5. **Error message changes**: Just error messages, no code impact.

## Version Timeline

```
v1.x (Legacy)
  ├─ Basic query building
  └─ Limited error handling

v2.0 (Current)
  ├─ Generics support
  ├─ Better error types
  └─ Phase 4 improvements

v2.1+ (Planned)
  ├─ Query result caching
  ├─ Performance optimizations
  └─ Additional dialect support
```

## FAQ

### Q: Do I need to change my code?
A: No. Phase 4 is fully backward compatible. However, we recommend:
- Adding explicit error checking after query building
- Updating field pointers to handle edge cases
- Monitoring query performance

### Q: What if my queries panic?
A: Panics are now wrapped with context. Use the GetContext() method to
understand what operation panicked and why.

### Q: Should I enable SQL caching?
A: Only if profiling shows query building is a bottleneck. Monitor cache
stats to verify effectiveness (aim for >50% hit rate).

### Q: How do I migrate from an older version?
A: There are no breaking changes. Update the package and test your code.
Most existing code will work unchanged.

### Q: Can I mix old and new patterns?
A: Yes. The improvements are additions, not replacements. Both patterns work.

*/
