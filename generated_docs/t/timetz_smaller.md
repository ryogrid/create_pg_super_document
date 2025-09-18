# timetz_smaller

## Location
src/backend/utils/adt/date.c: 2579 - 2595

## Overview
Returns the smaller of two time-with-timezone values, comparing them based on their equivalent UTC times.

## Definition
```c
Datum timetz_smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL built-in function that compares two time-with-timezone (`timetz`) values and returns the one that represents the earlier time when converted to UTC. The function uses internal comparison logic to determine which time is smaller, accounting for timezone differences to ensure accurate temporal ordering.

The comparison is performed by `timetz_cmp_internal()`, which normalizes both times to a common reference point before comparison, ensuring that timezone differences are properly handled. This is the complement function to `timetz_larger`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention that provides access to:
  - Argument 0: First `TimeTzADT` pointer (time1) 
  - Argument 1: Second `TimeTzADT` pointer (time2)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMETZADT_P`: Extracts TimeTzADT arguments from function call
  - `[timetz_cmp_internal](timetz_cmp_internal.md)`: Internal comparison function for timetz values
  - `PG_RETURN_TIMETZADT_P`: Returns TimeTzADT result to caller
- Types referenced:
  - `TimeTzADT`: Time-with-timezone abstract data type
  - `Datum`: PostgreSQL generic data type for function return values
- Called from (representative examples):
  - No direct callers found in codebase (likely called via SQL function dispatch)

## Notes and Other Information
- This function is typically invoked through SQL's `LEAST()` function or direct comparison operations
- The comparison accounts for timezone offsets, so 13:00+01 is considered smaller than 14:00+00 even though the local times differ
- Returns a pointer to one of the input arguments rather than creating a new copy, which is efficient for immutable data types
- Part of PostgreSQL's date/time function family located in `src/backend/utils/adt/date.c:2579`
- Functionally opposite to `timetz_larger`, using `< 0` instead of `> 0` in the comparison logic