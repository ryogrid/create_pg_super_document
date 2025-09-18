# in_range_int8_int8

## Location
src/backend/utils/adt/int8.c: 401 - 439

## Overview
A support function for int8 data type that determines whether a value falls within a range defined by a base value and an offset, used in SQL window functions with RANGE frames.

## Definition
Datum in_range_int8_int8(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the in_range support for int8 (64-bit integer) data types in PostgreSQL's window functions. It checks whether a given value is within a range defined by a base value plus or minus an offset. The function handles both PRECEDING and FOLLOWING cases in window frame specifications and includes overflow protection when computing the range boundaries. The function is specifically designed for use with SQL window functions that use RANGE frames with integer offsets.

## Parameters / Member Variables
- First argument (position 0): val - int8 value to test for inclusion in the range
- Second argument (position 1): base - int8 base value that defines the center of the range
- Third argument (position 2): offset - int8 offset value that defines the range size (must be non-negative)
- Fourth argument (position 3): sub - boolean flag indicating whether to subtract the offset (true for PRECEDING, false for FOLLOWING)
- Fifth argument (position 4): less - boolean flag indicating the comparison direction (true for <= comparison, false for >= comparison)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro to extract int8 arguments)
  - PG_GETARG_BOOL (macro to extract boolean arguments)
  - pg_add_s64_overflow (function to perform safe addition with overflow detection)
  - ereport (error reporting function)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's window function infrastructure for RANGE frames
- Includes overflow protection using pg_add_s64_overflow to handle edge cases safely
- The offset parameter must be non-negative; negative values trigger an error
- When overflow occurs during range calculation, the function returns a logically correct result based on the overflow direction
- Does not require int8_int4 or int8_int2 variants as implicit type coercion handles those cases
- Located in src/backend/utils/adt/int8.c:401-439
- Used internally by the window function processing system for range-based frame specifications