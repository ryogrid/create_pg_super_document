# int48gt

## Location
[src/backend/utils/adt/int8.c:254-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L254-L262)

## Overview
This function compares a 4-byte (int32) integer with an 8-byte (int64) integer and returns true if the 4-byte integer is greater than the 8-byte integer.

## Definition
```c
Datum int48gt(PG_FUNCTION_ARGS)
```

## Detailed Description
The int48gt function is a PostgreSQL built-in function that performs a "greater than" comparison between a 4-byte integer (int32/integer) and an 8-byte integer (int64/bigint). This is part of the int48relop family of functions that compare 32-bit values with 64-bit values. The function follows PostgreSQL's function calling convention and returns a boolean result indicating whether the first integer is greater than the second.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function calling convention macro that provides access to function arguments
  - First argument (index 0): int32 value to compare
  - Second argument (index 1): int64 value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32: Extracts 4-byte integer from function arguments
  - PG_GETARG_INT64: Extracts 8-byte integer from function arguments
  - PG_RETURN_BOOL: Returns boolean result as PostgreSQL Datum
- Called from:
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int8.c at lines 254-262
- Part of the int48relop family of comparison functions
- The int32 value is implicitly promoted to int64 during the comparison
- Used internally by PostgreSQL for mixed integer type operations and comparisons

## Simplified Source
```c
Datum int48gt(PG_FUNCTION_ARGS) {
    // Extract 4-byte and 8-byte integer arguments
    int32 val1 = PG_GETARG_INT32(0);
    int64 val2 = PG_GETARG_INT64(1);

    // Compare and return boolean result
    PG_RETURN_BOOL(val1 > val2);
}
```