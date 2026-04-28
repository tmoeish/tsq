# TSQ Examples

This directory contains the runnable TSQ example suite.

## Files Structure

### Generated Files
The following files are **auto-generated** by the `tsq gen` command:
- `database/*_tsq.go` - Generated query builder code for each table
- `database/userorder_result_tsq.go` - Generated Result query builder

To regenerate these files after modifying table structs:
```bash
go run ./cmd/tsq gen ./examples/database
```

### Manual Files
- `database/*.go` - Table struct definitions (manual)
- `database/userorder.go` - Result definition and Result pagination query
- `database/mock.sql` - Test database schema
- `main.go` - End-to-end example runner covering the main TSQ capabilities
- `main_test.go` - Smoke tests for the runnable examples

## Running Examples

```bash
# Regenerate generated code and build the example binary
make examples

# Run the compiled example suite from the repository root
./bin/examples

# Run tests
go test ./examples -v
```

## Example Patterns

See `main.go` for:
- CRUD generated methods
- Alias/rebinding queries
- Aggregation and GROUP BY
- Keyword search and pagination
- Result join queries
- `InVar`-based dynamic `IN (...)` filters
- Non-recursive `WITH` / CTE queries
- Set operations (`UNION`, `INTERSECT`, `EXCEPT`)
- Chunked insert / update / delete / delete-by-ids

## Notes

- Generated code should not be manually edited
- When table definitions change, regenerate with `make examples` or `tsq gen ./examples/database`
- The auto-generated code respects the current Build-based API (`tsq.DbMap`, `tsq.Dialect`, `QueryBuilder.Build`)
