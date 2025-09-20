# tm2interval

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:972-986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L972-L986)

## Overview
tm2interval is a static utility function that converts a tm structure and fractional seconds into an interval data type, performing overflow checking to ensure valid interval values.

## Definition

```c
static int
tm2interval(struct tm *tm, fsec_t fsec, interval * span)
```
## Detailed Description
This function constructs an interval structure from decomposed time components stored in a tm structure plus fractional seconds. It first validates that the combined year and month values do not overflow integer limits, then computes the total months and total microseconds for the interval.

The function performs overflow checking on the month calculation to prevent integer overflow when combining years and months. The time calculation aggregates all time components (days, hours, minutes, seconds) into a single microsecond value, using nested multiplication with 64-bit constants to avoid intermediate overflow issues.

## Parameters / Member Variables
- : Pointer to tm structure containing the source time components (tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec)
- : Fractional seconds value of type fsec_t to be included in the interval
- : Pointer to interval structure that will receive the computed month and time values

## Dependencies
- Functions called/Symbols referenced:
  - fsec_t (fractional seconds type)
  - interval (interval data structure)
  - MONTHS_PER_YEAR (12 - constant for year-to-month conversion)
  - INT64CONST (macro for 64-bit integer constants)
  - USECS_PER_SEC (microseconds per second constant)
- Called from (representative examples):
  - [PGTYPESinterval_from_asc](../P/PGTYPESinterval_from_asc.md) (string to interval conversion function)

## Notes and Other Information
- Located in src/interfaces/ecpg/pgtypeslib/interval.c:972-986
- Part of the ECPG (Embedded C for PostgreSQL) interface library
- Returns 0 on success, -1 if month value would overflow integer limits
- Uses careful overflow checking with double-precision arithmetic before converting to integers
- Employs nested multiplication with INT64CONST macros to prevent intermediate overflow in time calculations
- Designed to be the inverse operation of interval2tm function
- Critical for parsing interval strings in client applications