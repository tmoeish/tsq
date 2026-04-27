# TSQ Examples

This directory contains example code demonstrating TSQ query builder usage.

## Files Structure

### Generated Files
The following files are **auto-generated** by the `tsq gen` command:
- `database/*_tsq.go` - Generated query builder code for each table
- `database/userorder_dto_tsq.go` - Generated DTO query builder

To regenerate these files after modifying table structs:
```bash
go run ./cmd/tsq gen ./examples
```

### Manual Files
- `database/*.go` - Table struct definitions (manual)
- `database/mock.sql` - Test database schema
- `main.go` - Example usage demonstrating query building
- `main_test.go` - Integration tests

## Running Examples

```bash
# Run the main example
go run ./examples/main.go

# Run tests
go test ./examples -v
```

## Example Patterns

See `main.go` for:
- Database initialization with tsq.DbMap
- Pagination with PageReq
- Keyword search with multiple columns
- Chunked insertion for bulk operations

## Notes

- Generated code should not be manually edited
- When table definitions change, regenerate with `tsq gen ./examples`
- The auto-generated code respects the v2.0.0+ API (tsq.DbMap, tsq.Dialect)
