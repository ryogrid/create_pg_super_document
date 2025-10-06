# namele

## Location
[src/backend/utils/adt/name.c:175-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/name.c#L175-L183)

## Overview
The `namele` function compares two PostgreSQL Name values to determine if the first is less than or equal to the second according to the specified collation.

## Definition
```c
Datum namele(PG_FUNCTION_ARGS)
```

## Detailed Description
`namele` is a PostgreSQL built-in function that performs less-than-or-equal comparison between two Name data type values. It extracts two Name arguments from the function call context, compares them using the `namecmp` function with the current collation setting, and returns a boolean result indicating whether the first name is lexicographically less than or equal to the second. This function implements the less-than-or-equal operator (<=) for Name types and is used in SQL range queries, filtering operations, and sorting comparisons.

## Parameters / Member Variables
- `arg1`: First Name argument extracted using `PG_GETARG_NAME(0)`
- `arg2`: Second Name argument extracted using `PG_GETARG_NAME(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NAME` - Macro to extract Name arguments
  - `[namecmp](namecmp.md)` - Core name comparison function
  - `PG_GET_COLLATION` - Macro to get current collation
  - `PG_RETURN_BOOL` - Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator system)

## Notes and Other Information
- This function implements the less-than-or-equal operator (<=) for the Name data type in PostgreSQL
- The function relies on `namecmp` for the actual comparison logic, which returns zero or negative value when arg1 <= arg2
- Returns true (1) when the first name is lexicographically less than or equal to the second, false (0) otherwise
- Part of PostgreSQL's type system infrastructure for the Name built-in type
- Used internally by PostgreSQL's query planning and execution for range scans and filtering
- Collation-aware comparison ensures proper ordering according to locale-specific rules
- Combines both equality and less-than semantics in a single operation

## Simplified Source

```c
Datum
namele(PG_FUNCTION_ARGS)
{
    Name arg1 = PG_GETARG_NAME(0);
    Name arg2 = PG_GETARG_NAME(1);

    // Compare names using collation and return true if arg1 <= arg2
    PG_RETURN_BOOL(namecmp(arg1, arg2, PG_GET_COLLATION()) <= 0);
}
```