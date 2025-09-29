# GetCurrentDateTime

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:1058-1065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1058-L1065)

## Overview
GetCurrentDateTime is a convenience wrapper function that retrieves the current transaction start time ("now()") broken down as a struct pg_tm, converted according to the session timezone setting.

## Definition

```c
void
GetCurrentDateTime(struct tm *tm)
```
## Detailed Description
This function provides a simplified interface to obtain the current transaction timestamp without requiring fractional seconds or timezone offset information. It internally calls GetCurrentTimeUsec but discards the microsecond precision and timezone offset components, making it suitable for applications that only need the basic date and time components.

The function fills the provided pg_tm structure with the current transaction time, properly converted to the session's configured timezone. This ensures consistent behavior within a transaction, as all calls to "now()" functions return the same timestamp throughout a transaction's lifetime.

## Parameters / Member Variables
- : Pointer to a struct pg_tm that will be filled with the current date and time components

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimeUsec](GetCurrentTimeUsec.md)
  - struct pg_tm
  - fsec_t
- Called from (representative examples):
  - [GetSQLCurrentDate](GetSQLCurrentDate.md)
  - [time_timetz](../t/time_timetz.md)
  - [DecodeDateTime](../D/DecodeDateTime.md)
  - [DecodeTimeOnly](../D/DecodeTimeOnly.md)
  - [PGTYPESdate_today](../P/PGTYPESdate_today.md)
  - [PGTYPEStimestamp_current](../P/PGTYPEStimestamp_current.md)

## Notes and Other Information
- This is a convenience wrapper that simplifies the interface when fractional seconds and timezone offsets are not needed
- The function maintains transaction-level consistency by using the transaction start time
- The timezone conversion is handled automatically based on the session's timezone setting
- Part of PostgreSQL's internal datetime handling utilities located in src/backend/utils/adt/datetime.c

## Simplified Source

```c
void
GetCurrentDateTime(struct pg_tm *tm)
{
    fsec_t fsec;

    // Get current time with microseconds, but ignore the precision
    GetCurrentTimeUsec(tm, &fsec, NULL);
}
```