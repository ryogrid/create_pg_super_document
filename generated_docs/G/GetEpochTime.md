# GetEpochTime

## Location
[src/backend/utils/adt/timestamp.c:2168-2189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2168-L2189)

## Overview
A utility function that populates a pg_tm structure with the components of the Unix epoch time (January 1, 1970, 00:00:00 UTC).

## Definition
```c
void GetEpochTime(struct pg_tm *tm)
```

## Detailed Description
The GetEpochTime function initializes a pg_tm structure with the date and time components representing the Unix epoch (January 1, 1970, 00:00:00 UTC). It uses pg_gmtime to convert a zero pg_time_t value (representing epoch) into a broken-down time structure, then copies the relevant fields to the output parameter. The function adjusts the year by adding 1900 (as tm_year is years since 1900) and increments the month by 1 (as tm_mon is 0-based but PostgreSQL expects 1-based months).

## Parameters / Member Variables
- `tm`: A pointer to a pg_tm structure that will be filled with epoch time components

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tm](../p/pg_tm.md) (PostgreSQL's time structure type)
  - pg_time_t (PostgreSQL's time_t equivalent)
  - [pg_gmtime](../p/pg_gmtime.md) (PostgreSQL's gmtime equivalent)
  - elog (PostgreSQL's logging function)
  - Timestamp (timestamp data type context)
- Called from (representative examples):
  - [date_in](../d/date_in.md)
  - [SetEpochTimestamp](../S/SetEpochTimestamp.md)
  - TimestampTzPlusSeconds
  - [PGTYPESdate_from_asc](../P/PGTYPESdate_from_asc.md)

## Notes and Other Information
This function serves as a reference point for timestamp calculations throughout PostgreSQL. The epoch time is fundamental to Unix-based time systems and provides a consistent baseline for temporal computations. The function handles the conversion from the system's time representation to PostgreSQL's internal time structure format, ensuring proper field adjustments for year and month values. Error handling is included to detect failures in the pg_gmtime conversion, which would indicate a serious system-level issue.