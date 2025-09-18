# overlaps_timetz

## Location
src/backend/utils/adt/date.c: 2695 - 2709

## Overview
Implements the SQL OVERLAPS operator for time with timezone data types, determining whether two time intervals with timezones overlap according to the SQL specification.

## Definition


## Detailed Description
The `overlaps_timetz` function implements the SQL OVERLAPS operator for time intervals that include timezone information. It takes four TimeTzADT arguments representing two time intervals (ts1, te1) and (ts2, te2), and determines whether these intervals overlap. Like its non-timezone counterpart, it follows the SQL specification algorithm with complex null-handling logic that can return non-null results even when some inputs are null.

The function normalizes each interval by ensuring the start time is less than or equal to the end time, handling null values according to SQL specification rules. It then compares the intervals using three main cases: when the first interval starts after the second (ts1 > ts2), when it starts before (ts1 < ts2), and when they start at the same time (ts1 = ts2). The key difference from overlaps_time is that it uses timezone-aware comparison functions.

## Parameters / Member Variables
- `PG_GETARG_DATUM(0)` (ts1): Start time with timezone of the first interval
- `PG_GETARG_DATUM(1)` (te1): End time with timezone of the first interval  
- `PG_GETARG_DATUM(2)` (ts2): Start time with timezone of the second interval
- `PG_GETARG_DATUM(3)` (te2): End time with timezone of the second interval

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM
  - PG_ARGISNULL
  - DirectFunctionCall2
  - timetz_gt
  - timetz_lt
  - DatumGetBool
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function uses TimeTzADT data type but handles arguments as generic Datums to avoid dereferencing null values
- Implements complex null-handling logic per SQL specification requirements
- Uses local macros TIMETZ_GT and TIMETZ_LT that call timezone-aware comparison functions (timetz_gt, timetz_lt)
- Returns boolean true/false for overlap determination, or null when the result cannot be determined
- Unlike overlaps_time, this function accounts for timezone differences in time comparisons
- Located in src/backend/utils/adt/date.c:2695-2811