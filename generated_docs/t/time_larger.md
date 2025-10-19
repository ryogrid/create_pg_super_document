# time_larger

## Location
[src/backend/utils/adt/date.c:1759-1767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1759-L1767)

## Overview
The time_larger function returns the larger of two TimeADT values, implementing the maximum operation for PostgreSQL's time data type.

## Definition
```c
Datum time_larger(PG_FUNCTION_ARGS)
```

## Detailed Description
This function compares two time values and returns the one that represents a later time in the day. It extracts both time arguments from the function call information, performs a simple comparison using the greater-than operator, and returns the larger value. Since TimeADT values are stored as microseconds since midnight, the comparison is straightforward numeric comparison.

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
- Located in src/backend/utils/adt/date.c at lines 1759-1767
- Part of PostgreSQL's suite of comparison functions for the time data type
- Returns the time argument that represents a later point in the day

## Simplified Source

```c
TimeADT time_larger(TimeADT time1, TimeADT time2) {
    // Return the later of the two times (maximum)
    return (time1 > time2) ? time1 : time2;
}
```