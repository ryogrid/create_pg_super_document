# int48ne

## Location
src/backend/utils/adt/int8.c: 236 - 244

## Overview
This function compares a 4-byte (int32) integer with an 8-byte (int64) integer for inequality and returns true if they are not equal.

## Definition
```c
Datum int48ne(PG_FUNCTION_ARGS)
```

## Detailed Description
The int48ne function is a PostgreSQL built-in function that performs a "not equal" comparison between a 4-byte integer (int32/integer) and an 8-byte integer (int64/bigint). This is part of the int48relop family of functions that compare 32-bit values with 64-bit values. The function follows PostgreSQL's function calling convention and returns a boolean result indicating whether the two integers are not equal.

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
- Located in src/backend/utils/adt/int8.c at lines 236-244
- Part of the int48relop family of comparison functions
- The int32 value is implicitly promoted to int64 during the comparison
- Used internally by PostgreSQL for mixed integer type operations and comparisons