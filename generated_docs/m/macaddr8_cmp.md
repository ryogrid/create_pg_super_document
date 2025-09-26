# macaddr8_cmp

## Location
[src/backend/utils/adt/mac8.c:325-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L325-L337)

## Overview
PostgreSQL function interface for comparing two macaddr8 values, returning a tri-state comparison result for use in SQL queries and B-tree operations.

## Definition
```c
Datum macaddr8_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the PostgreSQL function manager interface for macaddr8 comparison operations. It extracts two macaddr8 arguments from the PostgreSQL function call context and delegates the actual comparison logic to `macaddr8_cmp_internal`. The function follows PostgreSQL's fmgr (function manager) conventions and is used by the SQL engine for ORDER BY clauses, B-tree index operations, and explicit comparison operations in SQL.

The function is typically registered in the system catalogs and can be invoked through SQL comparison operators or explicitly through function calls.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro which provides access to:
  - First argument: macaddr8 value (accessed via `PG_GETARG_MACADDR8_P(0)`)
  - Second argument: macaddr8 value (accessed via `PG_GETARG_MACADDR8_P(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MACADDR8_P (macro for extracting macaddr8 arguments)
  - [macaddr8_cmp_internal](macaddr8_cmp_internal.md) (performs actual comparison)
  - PG_RETURN_INT32 (macro for returning int32 result)
  - [macaddr8](macaddr8.md) (structure type)
- Called from (representative examples):
  - SQL comparison operations
  - B-tree index operations
  - ORDER BY clauses

## Notes and Other Information
- Returns a Datum containing an int32 value: -1, 0, or 1 for less-than, equal, or greater-than respectively
- This is a PostgreSQL fmgr v1 calling convention function
- Used internally by PostgreSQL's query engine for macaddr8 sorting and comparison
- The function is likely registered in pg_proc system catalog for SQL accessibility
- Follows PostgreSQL's standard comparison function interface pattern