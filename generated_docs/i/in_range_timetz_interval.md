# in_range_timetz_interval

## Location
[src/backend/utils/adt/date.c:2650-2694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2650-L2694)

## Overview
Provides in_range support for timetz values, determining if a value falls within a specified range relative to a base time and interval offset, used in window functions.

## Definition
```c
Datum in_range_timetz_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQL's in_range support for time-with-timezone values used in window functions with RANGE frames. It determines whether a given timetz value (`val`) is within a specified interval range from a base timetz value (`base`), either preceding or following based on the `sub` parameter.

Unlike the standard timetz arithmetic functions, this function deliberately avoids day-boundary wraparound behavior since that would produce incorrect results for range comparisons in window functions. The function only considers the time portion of intervals, ignoring month and day components, and includes overflow protection for very large intervals.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention that provides access to:
  - Argument 0: `TimeTzADT` pointer (val) - the time value being tested
  - Argument 1: `TimeTzADT` pointer (base) - the base time for range calculation  
  - Argument 2: `Interval` pointer (offset) - the interval defining the range size
  - Argument 3: `bool` (sub) - whether to subtract (preceding) or add (following) the offset
  - Argument 4: `bool` (less) - whether to test for <= (true) or >= (false) comparison

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMETZADT_P`: Extracts TimeTzADT arguments from function call
  - `PG_GETARG_INTERVAL_P`: Extracts Interval argument from function call
  - `PG_GETARG_BOOL`: Extracts boolean arguments from function call
  - `ereport`: PostgreSQL error reporting function
  - [errcode](../e/errcode.md): Error code specification  
  - [errmsg](../e/errmsg.md): Error message specification
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md): Safe 64-bit integer addition with overflow detection
  - [timetz_cmp_internal](../t/timetz_cmp_internal.md): Internal comparison function for timetz values
  - `PG_RETURN_BOOL`: Returns boolean result to caller
- Constants referenced:
  - `ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE`: Error code for invalid window frame size
- Types referenced:
  - `TimeTzADT`: Time-with-timezone abstract data type
  - `Interval`: PostgreSQL interval data type
  - `Datum`: PostgreSQL generic data type for function return values
- Called from (representative examples):
  - No direct callers found in codebase (likely called via window function framework)

## Notes and Other Information
- Specifically designed for window function RANGE frame support, not general arithmetic
- Rejects negative interval offsets with error: "invalid preceding or following size in window function"
- Avoids day-boundary wraparound unlike `timetz_pl_interval` and `timetz_mi_interval`
- Uses overflow-safe arithmetic with `pg_add_s64_overflow` to handle very large intervals
- Ignores month and day components of intervals, only using the time portion
- Returns boolean result indicating whether `val` is within the specified range of `base`
- Located in `src/backend/utils/adt/date.c:2650`
- Part of PostgreSQL's window function infrastructure for time-based ranges
- Subtraction cannot overflow, but addition is protected against overflow conditions