# time_smaller

## Location
src/backend/utils/adt/date.c: 1768 - 1782

## Overview
The time_smaller function returns the smaller of two TimeADT values, implementing the minimum operation for PostgreSQL's time data type.

## Definition
```c
Datum time_smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
This function compares two time values and returns the one that represents an earlier time in the day. It extracts both time arguments from the function call information, performs a simple comparison using the less-than operator, and returns the smaller value. Since TimeADT values are stored as microseconds since midnight, the comparison is straightforward numeric comparison.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing two TimeADT arguments to compare

## Dependencies
- Functions called/Symbols referenced:
  - TimeADT (data type)
  - PG_GETARG_TIMEADT (macro to extract time arguments)
  - PG_RETURN_TIMEADT (macro to return time result)
- Called from (representative examples):
  - Used internally by PostgreSQL for time comparison operations

## Notes and Other Information
- The function performs a direct comparison of the internal microsecond representation
- Located in src/backend/utils/adt/date.c at lines 1768-1782
- Part of PostgreSQL's suite of comparison functions for the time data type
- Returns the time argument that represents an earlier point in the day