package examples

// TSQ Best Practices Guide
//
// This guide documents recommended patterns for using the TSQ query builder
// in production applications.

// ============================================================
// 1. ERROR HANDLING
// ============================================================

// Best Practice 1.1: Validate inputs early
// - Check table and column existence before building queries
// - Use ValidateStrict() for strict input validation
// - Handle errors from Build() before executing queries
//
// Good:
//   if err := pageReq.ValidateStrict(); err != nil {
//     return fmt.Errorf("invalid pagination: %w", err)
//   }
//   sql, err := query.Build()
//   if err != nil {
//     return fmt.Errorf("build failed: %w", err)
//   }
//
// Bad:
//   sql, _ := query.Build()  // Ignoring errors is dangerous

// Best Practice 1.2: Use context for timeouts
// - Always provide context with timeouts for database operations
// - Prevents queries from hanging indefinitely
// - Allows graceful cancellation
//
// Good:
//   ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
//   defer cancel()
//   rows, err := db.QueryContext(ctx, sql, args...)
//
// Bad:
//   rows, err := db.Query(sql, args...)  // No timeout

// Best Practice 1.3: Use error wrapping with %w
// - Preserve error context through call stack
// - Enables errors.Is() and errors.As() checks
// - Makes debugging easier
//
// Good:
//   if err != nil {
//     return fmt.Errorf("failed to scan user: %w", err)
//   }
//
// Bad:
//   if err != nil {
//     return errors.New("failed to scan")  // Lost error info
//   }

// Best Practice 1.4: Check specific error types
// - Use errors.As() to check for specific error types
// - Different errors need different handling strategies
// - Provide meaningful messages to users
//
// Good:
//   var unknownField *tsq.ErrUnknownSortField
//   if errors.As(err, &unknownField) {
//     return fmt.Errorf("field not found, use one of: id, name, email")
//   }
//
// Bad:
//   if err != nil {
//     return fmt.Errorf("query failed: %w", err)  // Too generic

// ============================================================
// 2. PAGINATION
// ============================================================

// Best Practice 2.1: Use ValidateStrict() for API input
// - Rejects invalid pagination without mutation
// - Returns clear error messages
// - Fails fast
//
// Good:
//   if err := pageReq.ValidateStrict(); err != nil {
//     log.Printf("invalid pagination: %v", err)
//     return nil, err
//   }
//
// Bad:
//   pageReq.Validate()  // Silently corrects values, might hide bugs

// Best Practice 2.2: Handle pagination overflow safely
// - Use large page numbers carefully
// - Offset may return 0 to prevent overflow
// - Consider MAX_PAGE limits
//
// Good:
//   pageReq := &PageReq{Page: largeNum, Size: normalSize}
//   offset := pageReq.Offset()  // Safe, handles overflow
//
// Bad:
//   offset := largeNum * normalSize  // Can overflow

// Best Practice 2.3: Always check HasNext/HasPrev
// - Enables proper pagination UI
// - Prevents requesting non-existent pages
// - Improves user experience
//
// Good:
//   if resp.HasNext() {
//     nextBtn.Show()
//   } else {
//     nextBtn.Hide()
//   }
//
// Bad:
//   if resp.Page < resp.TotalPage {  // Less clear than HasNext()

// ============================================================
// 3. TRANSACTIONS
// ============================================================

// Best Practice 3.1: Always defer rollback
// - Rollback is safe even if committed
// - Ensures cleanup in all paths
// - Prevents connection leaks
//
// Good:
//   tx, err := db.Begin()
//   if err != nil {
//     return err
//   }
//   defer func() {
//     if rollErr := tx.Rollback(); rollErr != nil && rollErr != sql.ErrTxDone {
//       log.Printf("rollback error: %v", rollErr)
//     }
//   }()
//
// Bad:
//   tx, _ := db.Begin()
//   // No defer, might leak transaction

// Best Practice 3.2: Commit at the end, not in middle
// - All operations should succeed before commit
// - Failing operations should prevent commit
// - Makes transaction logic easier to follow
//
// Good:
//   // Do all operations
//   if err := doOp1(); err != nil {
//     return err
//   }
//   if err := doOp2(); err != nil {
//     return err
//   }
//   return tx.Commit().Error
//
// Bad:
//   if err := doOp1(); err != nil {
//     tx.Rollback()
//     return err
//   }
//   if err := doOp2(); err != nil {
//     tx.Rollback()
//     return err
//   }

// Best Practice 3.3: Use transactions for related operations
// - Group related updates together
// - Ensures consistency
// - Prevents partial updates
//
// Good:
//   tx, _ := db.Begin()
//   defer tx.Rollback()
//   // Update user and their profile in one transaction
//   updateUser(tx, user)
//   updateProfile(tx, profile)
//   return tx.Commit()
//
// Bad:
//   updateUser(db, user)
//   updateProfile(db, profile)  // Could partially succeed

// ============================================================
// 4. FIELD POINTERS
// ============================================================

// Best Practice 4.1: Validate field pointers during initialization
// - Check field pointer exists and works
// - Fail fast with clear error
// - Don't wait for runtime scanning
//
// Good:
//   fp := func(h any) any {
//     if h == nil {
//       return nil
//     }
//     u, ok := h.(*User)
//     if !ok {
//       return nil
//     }
//     return &u.ID
//   }
//   col := NewCol[int](table, "id", "id", fp)
//
// Bad:
//   col := NewCol[int](table, "id", "id", func(h any) any {
//     return &h.(*User).ID  // Panics if h is nil
//   })

// Best Practice 4.2: Use Into() for different result types
// - Map database columns to different struct fields
// - Useful for DTOs and result mapping
// - Avoids code duplication
//
// Good:
//   // Database col -> DTO field mapping
//   userID.Into(func(h any) any { return &h.(*UserDTO).UserID }, "user_id")
//
// Bad:
//   // Creating separate columns for each mapping

// Best Practice 4.3: Handle type mismatches gracefully
// - Field pointer can be called with different holder types
// - Always check type before dereferencing
// - Return nil for type mismatches
//
// Good:
//   fp := func(h any) any {
//     if u, ok := h.(*User); ok {
//       return &u.ID
//     }
//     return nil
//   }
//
// Bad:
//   fp := func(h any) any {
//     return &h.(*User).ID  // Panics on type mismatch
//   }

// ============================================================
// 5. SORTING
// ============================================================

// Best Practice 5.1: Validate sort fields
// - Check field exists before using
// - Provide user-friendly error messages
// - Prevent ambiguous sorts
//
// Good:
//   var unknownField *ErrUnknownSortField
//   if errors.As(err, &unknownField) {
//     return fmt.Errorf("sort field %q not found", unknownField.Field)
//   }
//
// Bad:
//   if err != nil {
//     log.Println("sort error")
//   }

// Best Practice 5.2: Use qualified names for joins
// - Avoid ambiguity in joined queries
// - Use table.column format
// - Makes queries self-documenting
//
// Good:
//   orderBy := "orders.id"  // Clear which table
//
// Bad:
//   orderBy := "id"  // Ambiguous when joining users and orders

// Best Practice 5.3: Document available sort fields
// - Keep list of sortable fields in documentation
// - Update when schema changes
// - Help users understand what they can sort by
//
// Good:
//   // SortableFields: id, name, email, created_at
//   var allowedFields = []string{"id", "name", "email", "created_at"}
//
// Bad:
//   // No documentation of available fields

// ============================================================
// 6. CACHING
// ============================================================

// Best Practice 6.1: Use SQL cache in high-volume scenarios
// - Cache frequently built queries
// - Reduces CPU usage for query compilation
// - Only enable if proven beneficial
//
// Good:
//   cache := NewSQLRenderCache(SQLCacheConfig{
//     Enabled: true,
//     MaxSize: 1000,
//   })
//   // Monitor cache stats
//   stats := cache.Stats()
//
// Bad:
//   cache := NewSQLRenderCache(SQLCacheConfig{})  // Default off

// Best Practice 6.2: Monitor cache effectiveness
// - Track hit rate and size
// - Adjust MaxSize based on usage
// - Disable if hit rate is too low
//
// Good:
//   stats := cache.Stats()
//   if hitRate < 0.5 {
//     log.Warn("cache hit rate too low, disabling cache")
//     cache.Clear()
//   }
//
// Bad:
//   // Fire and forget, no monitoring

// ============================================================
// 7. PRODUCTION CONSIDERATIONS
// ============================================================

// Best Practice 7.1: Log errors at appropriate levels
// - ERROR: Query build failures, constraint violations
// - WARN: Retryable errors, timeouts
// - INFO: Query counts, cache stats
//
// Good:
//   if err != nil {
//     log.Error("query build failed", "err", err)
//     return err
//   }
//
// Bad:
//   fmt.Println("Error: " + err.Error())  // Not structured logging

// Best Practice 7.2: Avoid exposing query structure to users
// - Build errors should be caught before user sees them
// - Provide user-friendly error messages
// - Log full errors for debugging
//
// Good:
//   if err != nil {
//     log.Error("query failed", "err", err)  // Log full error
//     return fmt.Errorf("database error")  // Generic message to user
//   }
//
// Bad:
//   return fmt.Errorf("query failed: %w", err)  // Exposes internals

// Best Practice 7.3: Set reasonable limits
// - Max page size (1000)
// - Max query timeout (30s)
// - Max result rows (10000)
//
// Good:
//   const MaxPageSize = 1000
//   const QueryTimeout = 30 * time.Second
//   const MaxResults = 10000
//
// Bad:
//   // No limits, unbounded queries can exhaust resources
