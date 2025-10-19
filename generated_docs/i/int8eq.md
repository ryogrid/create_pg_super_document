# int8eq

## Location
[src/backend/utils/adt/int8.c:113-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L113-L121)

## Overview
Performs equality comparison between two int8 (bigint) values and returns a boolean result.

## Definition

```c
Datum
int8eq(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the equality operator (=) for PostgreSQL's int8 data type (bigint). It takes two 64-bit integer values and performs a direct comparison to determine if they are equal. This function is part of the PostgreSQL type system's relational operator infrastructure and is automatically called when the equality operator is used with int8 values in SQL queries, WHERE clauses, JOIN conditions, and other comparison contexts.

The function performs a simple direct comparison of the two 64-bit integer values and returns a boolean Datum indicating whether they are equal.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides access to:
  -  (int64): The first 64-bit integer value to compare
  -  (int64): The second 64-bit integer value to compare

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract int64 arguments from function arguments (used twice)
  - : Macro to return boolean result as Datum
- Called from (representative examples):
  - No direct references found in the current codebase (used internally by the type system)

## Notes and Other Information
- This function is registered in the PostgreSQL type system as the equality operator function for the int8/bigint data type
- Part of the relational operator family for int8, which includes other comparison functions (lt, le, gt, ge, ne)
- Used extensively in query execution for equality tests in WHERE clauses, JOIN conditions, and other SQL constructs
- The comparison is a simple C-level integer equality test with no overflow concerns
- Returns PostgreSQL boolean type (true/false) as a Datum
- Located in src/backend/utils/adt/int8.c in the relational operators section

## Simplified Source
```c
/*
 * Equality comparison for int8 (bigint) values
 */
Datum int8eq(PG_FUNCTION_ARGS) {
    // Extract two 64-bit integer arguments
    int64 val1 = PG_GETARG_INT64(0);
    int64 val2 = PG_GETARG_INT64(1);

    // Compare for equality and return boolean result
    PG_RETURN_BOOL(val1 == val2);
}
```