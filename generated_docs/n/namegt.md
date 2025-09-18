# namegt

## Location
src/backend/utils/adt/name.c: 184 - 192

## Overview
The `namegt` function compares two PostgreSQL Name values to determine if the first is greater than the second according to the specified collation.

## Definition
```c
Datum namegt(PG_FUNCTION_ARGS)
```

## Detailed Description
`namegt` is a PostgreSQL built-in function that performs greater-than comparison between two Name data type values. It extracts two Name arguments from the function call context, compares them using the `namecmp` function with the current collation setting, and returns a boolean result indicating whether the first name is lexicographically greater than the second. This function implements the greater-than operator (>) for Name types and is used in SQL ORDER BY clauses, range queries, and filtering operations.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access function arguments
- `arg1`: First Name argument extracted using `PG_GETARG_NAME(0)`
- `arg2`: Second Name argument extracted using `PG_GETARG_NAME(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NAME` - Macro to extract Name arguments
  - `namecmp` - Core name comparison function
  - `PG_GET_COLLATION` - Macro to get current collation
  - `PG_RETURN_BOOL` - Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator system)

## Notes and Other Information
- This function implements the greater-than operator (>) for the Name data type in PostgreSQL
- The function relies on `namecmp` for the actual comparison logic, which returns positive value when arg1 > arg2
- Returns true (1) when the first name is lexicographically greater than the second, false (0) otherwise
- Part of PostgreSQL's type system infrastructure for the Name built-in type
- Used internally by PostgreSQL's sorting and indexing mechanisms for Name columns
- Collation-aware comparison ensures proper ordering according to locale-specific rules
- Complementary to `namelt` - provides the opposite comparison logic