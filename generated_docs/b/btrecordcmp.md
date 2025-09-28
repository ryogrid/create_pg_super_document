# btrecordcmp

## Location
[src/backend/utils/adt/rowtypes.c:1313-1330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1313-L1330)

## Overview
The `btrecordcmp` function implements the B-tree comparison function for PostgreSQL record (composite) types, used for indexing and sorting operations.

## Definition
```c
Datum btrecordcmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the B-tree comparison support function for record/composite types in PostgreSQL. It's specifically designed to work with PostgreSQL's B-tree index access method, providing the comparison logic needed for indexing composite types.

The function delegates to `record_cmp` for the actual comparison and returns the result as a 32-bit integer. This follows the standard B-tree comparison convention where negative values indicate the first argument is less than the second, zero indicates equality, and positive values indicate the first argument is greater than the second.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing the two records to be compared

## Dependencies
- Functions called/Symbols referenced:
  - [record_cmp](../r/record_cmp.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's B-tree access method support for composite types
- The function name follows PostgreSQL's naming convention for B-tree comparison functions (bt + type + cmp)
- Returns an integer comparison result suitable for B-tree operations
- Used internally by PostgreSQL's indexing system when creating B-tree indexes on composite types
- Located in src/backend/utils/adt/rowtypes.c at lines 1313-1330

## Simplified Source

```c
// Simplified version of btrecordcmp
Datum btrecordcmp(PG_FUNCTION_ARGS) {
    // Use record_cmp and return the result as 32-bit integer for B-tree operations
    PG_RETURN_INT32(record_cmp(fcinfo));
}
```

Key simplifications made:
- The function is already extremely simple - just returns record_cmp result as int32
- No additional simplification needed as it's a one-line wrapper
- Delegates all comparison logic to record_cmp for consistency and maintainability
- Perfect example of adapter pattern for B-tree interface compliance