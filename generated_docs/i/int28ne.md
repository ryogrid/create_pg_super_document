# int28ne

## Location
[src/backend/utils/adt/int8.c:350-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L350-L358)

## Overview
A PostgreSQL built-in function that compares a 2-byte integer (smallint/int16) with an 8-byte integer (bigint/int64) to determine if they are not equal.

## Definition
Datum int28ne(PG_FUNCTION_ARGS)

## Detailed Description
The int28ne function implements the not-equal comparison operator between a 2-byte integer and an 8-byte integer. This function is part of PostgreSQL system for handling mixed-precision integer comparisons in the int28 family. It extracts the int16 value from the first argument and the int64 value from the second argument, performs a direct comparison, and returns a boolean result indicating whether the values are not equal.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument structure containing:
  - Argument 0: int16 value (2-byte integer)
  - Argument 1: int64 value (8-byte integer)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16
  - PG_GETARG_INT64
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL cross-type comparison operators for the int28 family
- Handles automatic type promotion from int16 to int64 for comparison
- Returns true if the int16 value does not equal the int64 value, false otherwise
- Part of the int28relop family for 16-bit val1 relop 64-bit val2 operations
- Located in src/backend/utils/adt/int8.c, which contains various int8 (bigint) operations

## Simplified Source

```c
Datum int28ne(PG_FUNCTION_ARGS) {
    // Extract 2-byte and 8-byte integer arguments
    int16 val1 = PG_GETARG_INT16(0);
    int64 val2 = PG_GETARG_INT64(1);

    // Return boolean result of not-equal comparison
    PG_RETURN_BOOL(val1 != val2);
}
```