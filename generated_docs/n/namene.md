# namene

## Location
[src/backend/utils/adt/name.c:157-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/name.c#L157-L165)

## Overview
The `namene` function compares two PostgreSQL Name values for inequality, returning true if they are not equal according to the specified collation.

## Definition
```c
Datum namene(PG_FUNCTION_ARGS)
```

## Detailed Description
`namene` is a PostgreSQL built-in function that performs inequality comparison between two Name data type values. It extracts two Name arguments from the function call context, compares them using the `namecmp` function with the current collation setting, and returns a boolean result indicating whether the names are not equal. This function implements the not-equal operator (!=) for Name types and is typically used in SQL WHERE clauses and JOIN conditions.

## Parameters / Member Variables
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
- This function implements the inequality operator (!=, <>) for the Name data type in PostgreSQL
- The function relies on `namecmp` for the actual comparison logic, ensuring consistent collation handling
- Returns true (1) when names are not equal, false (0) when they are equal
- Part of PostgreSQL's type system infrastructure for the Name built-in type
- Complementary function to `nameeq` - returns the logical opposite result