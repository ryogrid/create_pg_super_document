# GetSQLCurrentTime

## Location
[src/backend/utils/adt/date.c:342-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L342-L361)

## Overview
GetSQLCurrentTime implements the SQL CURRENT_TIME and CURRENT_TIME(n) functions, returning the current time of day with timezone information.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
This function retrieves the current time with timezone information and returns it as a TimeTzADT structure. It supports precision specification through the typmod parameter, which allows controlling the fractional seconds precision in the result. The function gets the current time using GetCurrentTimeUsec, converts it to the appropriate timezone-aware time format, and applies any precision adjustments specified by the typmod parameter.

## Parameters / Member Variables
- `typmod`: Type modifier that specifies the precision (number of fractional seconds digits) for the returned time value

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimeUsec](GetCurrentTimeUsec.md)
  - [palloc](../p/palloc.md)
  - [tm2timetz](../t/tm2timetz.md)
  - [AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md)
- Types used:
  - TimeTzADT
  - [pg_tm](../p/pg_tm.md)
  - fsec_t
- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md)
  - PG_RETURN_TIMETZADT_P

## Notes and Other Information
- Implements SQL standard CURRENT_TIME and CURRENT_TIME(n) functions
- Returns a dynamically allocated TimeTzADT structure that must be managed by the caller
- The timezone component reflects the local timezone setting of the server
- Precision can be controlled via the typmod parameter, affecting fractional seconds display

## Simplified Source

```c
TimeTzADT *GetSQLCurrentTime(int32 typmod) {
    TimeTzADT *result;
    struct pg_tm tt, *tm = &tt;
    fsec_t fsec;
    int tz;

    // Get current time with microsecond precision and timezone
    GetCurrentTimeUsec(tm, &fsec, &tz);

    // Allocate result structure
    result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

    // Convert time components to TimeTzADT format
    tm2timetz(tm, fsec, tz, result);

    // Apply precision adjustment based on typmod
    AdjustTimeForTypmod(&(result->time), typmod);

    return result;
}
```