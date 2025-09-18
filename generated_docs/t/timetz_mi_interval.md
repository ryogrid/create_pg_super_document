# timetz_mi_interval

## Location
src/backend/utils/adt/date.c: 2623 - 2649

## Overview
Subtracts an interval from a time-with-timezone value, producing a new time-with-timezone result while preserving the original timezone.

## Definition
```c
Datum timetz_mi_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQL's subtraction operator for time-with-timezone minus interval operations (`timetz - interval`). It subtracts the time component of an interval from a time-with-timezone value, ensuring the result remains within a 24-hour day boundary through modular arithmetic. The timezone information from the original time is preserved in the result.

The function performs bounds checking to prevent infinite interval subtraction and uses microsecond precision arithmetic. When the subtraction would result in a negative time, it wraps around to the appropriate time within the same day using modular arithmetic with `USECS_PER_DAY`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention that provides access to:
  - Argument 0: `TimeTzADT` pointer (time) - the base time-with-timezone value  
  - Argument 1: `Interval` pointer (span) - the interval to subtract

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMETZADT_P`: Extracts TimeTzADT argument from function call
  - `PG_GETARG_INTERVAL_P`: Extracts Interval argument from function call
  - `INTERVAL_NOT_FINITE`: Macro to check for infinite intervals
  - `ereport`: PostgreSQL error reporting function
  - [errcode](../e/errcode.md): Error code specification
  - [errmsg](../e/errmsg.md): Error message specification  
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - `PG_RETURN_TIMETZADT_P`: Returns TimeTzADT result to caller
- Constants referenced:
  - `USECS_PER_DAY`: Microseconds per day constant
  - `INT64CONST`: 64-bit integer constant macro
  - `ERRCODE_DATETIME_VALUE_OUT_OF_RANGE`: Error code for datetime range errors
- Types referenced:
  - `TimeTzADT`: Time-with-timezone abstract data type
  - `Interval`: PostgreSQL interval data type  
  - `Datum`: PostgreSQL generic data type for function return values
- Called from (representative examples):
  - No direct callers found in codebase (likely called via SQL operator dispatch)

## Notes and Other Information
- Rejects infinite intervals with a specific error message: "cannot subtract infinite interval from time"
- Uses modular arithmetic to ensure the result time stays within 0-24 hour bounds
- Preserves the original timezone (`time->zone`) in the result unchanged
- Allocates new memory for the result rather than modifying input parameters
- Located in `src/backend/utils/adt/date.c:2623`
- Complement function to `timetz_pl_interval`, using subtraction instead of addition
- The date component of intervals is ignored since timetz represents only time-of-day information
- Handles negative results by adding `USECS_PER_DAY` to wrap around to the previous day's equivalent time