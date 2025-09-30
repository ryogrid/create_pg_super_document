# GetSQLLocalTime

## Location
[src/backend/utils/adt/date.c:362-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L362-L382)

## Overview
GetSQLLocalTime implements the SQL LOCALTIME and LOCALTIME(n) functions, returning the current local time without timezone information.

## Definition
```c
TimeADT GetSQLLocalTime(int32 typmod)
```

## Detailed Description
This function retrieves the current local time and returns it as a TimeADT value (time without timezone). Unlike GetSQLCurrentTime, this function discards timezone information and returns only the local time component. It supports precision specification through the typmod parameter for controlling fractional seconds precision. The function gets the current time using GetCurrentTimeUsec, converts it to a timezone-agnostic time format using tm2time, and applies precision adjustments.

## Parameters / Member Variables
- `typmod`: Type modifier that specifies the precision (number of fractional seconds digits) for the returned time value

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimeUsec](GetCurrentTimeUsec.md)
  - [tm2time](../t/tm2time.md)
  - [AdjustTimeForTypmod](../A/AdjustTimeForTypmod.md)
- Types used:
  - TimeADT
  - [pg_tm](../p/pg_tm.md)
  - fsec_t
- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md)
  - PG_RETURN_TIMETZADT_P

## Notes and Other Information
- Implements SQL standard LOCALTIME and LOCALTIME(n) functions
- Returns a TimeADT value (time without timezone) rather than TimeTzADT
- The result represents local time in the server's timezone but without timezone metadata
- Precision can be controlled via the typmod parameter for fractional seconds display
- More efficient than GetSQLCurrentTime when timezone information is not needed

## Simplified Source

```c
TimeADT GetSQLLocalTime(int32 typmod) {
    TimeADT result;
    struct pg_tm tt, *tm = &tt;
    fsec_t fsec;
    int tz;

    // Get current time with microsecond precision and timezone
    GetCurrentTimeUsec(tm, &fsec, &tz);

    // Convert to local time format (discarding timezone info)
    tm2time(tm, fsec, &result);

    // Apply precision adjustment based on typmod
    AdjustTimeForTypmod(&result, typmod);

    return result;
}
```