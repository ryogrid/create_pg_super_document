# overlaps_time

## Location
src/backend/utils/adt/date.c: 1783 - 1797

## Overview
Implements the SQL OVERLAPS operator for time data types, determining whether two time intervals overlap according to the SQL specification.

## Definition


## Detailed Description
The `overlaps_time` function implements the SQL OVERLAPS operator for time intervals. It takes four time arguments representing two time intervals (ts1, te1) and (ts2, te2), and determines whether these intervals overlap. The function follows the SQL specification algorithm, which includes complex null-handling logic that can return non-null results even when some inputs are null.

The function normalizes each interval by ensuring the start time is less than or equal to the end time, handling null values according to SQL specification rules. It then compares the intervals using three main cases: when the first interval starts after the second (ts1 > ts2), when it starts before (ts1 < ts2), and when they start at the same time (ts1 = ts2).

## Parameters / Member Variables
- `PG_GETARG_DATUM(0)` (ts1): Start time of the first interval
- `PG_GETARG_DATUM(1)` (te1): End time of the first interval  
- `PG_GETARG_DATUM(2)` (ts2): Start time of the second interval
- `PG_GETARG_DATUM(3)` (te2): End time of the second interval

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM
  - PG_ARGISNULL
  - DatumGetTimeADT
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function uses TimeADT data type but handles arguments as generic Datums to avoid dereferencing null values since TimeADT is pass-by-reference
- Implements complex null-handling logic per SQL specification requirements
- Uses local macros TIMEADT_GT and TIMEADT_LT for time comparisons
- Returns boolean true/false for overlap determination, or null when the result cannot be determined
- Located in src/backend/utils/adt/date.c:1783-1899