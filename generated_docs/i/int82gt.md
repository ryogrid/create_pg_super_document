# int82gt

## Location
[src/backend/utils/adt/int8.c:311-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L311-L319)

## Overview
A PostgreSQL built-in function that compares an 8-byte integer (bigint/int64) with a 2-byte integer (smallint/int16) to determine if the first value is greater than the second.

## Definition
Datum int82gt(PG_FUNCTION_ARGS)

## Detailed Description
The int82gt function implements the greater-than comparison operator between an 8-byte integer and a 2-byte integer. This function is part of PostgreSQL system for handling mixed-precision integer comparisons. It extracts the int64 value from the first argument and the int16 value from the second argument, performs a direct comparison, and returns a boolean result indicating whether the first value is greater than the second.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument structure containing:
  - Argument 0: int64 value (8-byte integer)
  - Argument 1: int16 value (2-byte integer)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64
  - PG_GETARG_INT16
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL cross-type comparison operators
- Handles automatic type promotion from int16 to int64 for comparison
- Returns true if the int64 value is greater than the int16 value, false otherwise
- Located in src/backend/utils/adt/int8.c, which contains various int8 (bigint) operations

## Simplified Source
```c
Datum int82gt(PG_FUNCTION_ARGS) {
    // Extract 8-byte and 2-byte integer arguments
    int64 val1 = PG_GETARG_INT64(0);
    int16 val2 = PG_GETARG_INT16(1);

    // Compare and return boolean result (greater than)
    PG_RETURN_BOOL(val1 > val2);
}
```