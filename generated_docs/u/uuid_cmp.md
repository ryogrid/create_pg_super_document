# uuid_cmp

## Location
[src/backend/utils/adt/uuid.c:229-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L229-L240)

## Overview
Serves as the B-tree index comparison function for PostgreSQL UUID data type, returning an integer indicating the relative ordering of two UUID values.

## Definition
```c
Datum uuid_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `uuid_cmp` function is specifically designed as a handler for B-tree index operations on UUID columns. Unlike the boolean comparison functions (uuid_eq, uuid_ne, uuid_gt, etc.), this function returns an integer value that indicates the relative ordering of two UUID values: negative if the first UUID is less than the second, zero if they are equal, and positive if the first UUID is greater than the second. This tri-state comparison result is essential for B-tree index construction and maintenance, enabling efficient sorting, searching, and range operations on UUID columns. The function delegates the actual comparison logic to `uuid_internal_cmp` and directly returns its integer result.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1` (pg_uuid_t*): Pointer to the first UUID value to compare
  - `arg2` (pg_uuid_t*): Pointer to the second UUID value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_UUID_P`: Macro to extract UUID arguments from function call
  - [uuid_internal_cmp](uuid_internal_cmp.md): Internal function performing the actual UUID comparison
  - `PG_RETURN_INT32`: Macro to return 32-bit integer result
  - [pg_uuid_t](../p/pg_uuid_t.md): UUID data type structure
- Called from (representative examples):
  - B-tree index operations during INSERT, UPDATE, DELETE
  - ORDER BY clauses on UUID columns
  - Range queries and index scans on UUID data

## Notes and Other Information
- Returns the raw integer result from `uuid_internal_cmp` without modification
- Critical for B-tree index performance on UUID columns
- Enables PostgreSQL to efficiently sort and search UUID values
- Used internally by the query planner for cost estimation and optimization
- The comparison is lexicographic byte-wise, providing deterministic ordering
- Essential component of the UUID operator class for indexing support