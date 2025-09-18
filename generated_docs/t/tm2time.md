# tm2time

## Location
[src/backend/utils/adt/date.c:1416-1426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1416-L1426)

## Overview
Converts a tm structure to a PostgreSQL time data type, combining hours, minutes, seconds, and fractional seconds into a single TimeADT value.

## Definition


## Detailed Description
The tm2time function performs a conversion from a broken-down time representation (struct pg_tm) to PostgreSQL's internal time representation (TimeADT). It calculates the total number of microseconds since midnight by combining the hour, minute, second components from the tm structure with additional fractional seconds. The conversion follows the formula: ((hours * 60 + minutes) * 60 + seconds) * 1,000,000 + fractional_seconds.

## Parameters / Member Variables
- : Pointer to a pg_tm structure containing the broken-down time components (tm_hour, tm_min, tm_sec)
- : Fractional seconds component in microseconds (fsec_t type)
- : Pointer to TimeADT where the converted time value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - MINS_PER_HOUR (constant)
  - SECS_PER_MINUTE (constant)
  - USECS_PER_SEC (constant)
- Types used:
  - [pg_tm](../p/pg_tm.md) (struct)
  - fsec_t (fractional seconds type)
  - TimeADT (time abstract data type)
- Called from (representative examples):
  - [GetSQLLocalTime](../G/GetSQLLocalTime.md)
  - [time_in](time_in.md)
  - [parse_datetime](../p/parse_datetime.md)
  - PG_RETURN_TIMETZADT_P

## Notes and Other Information
- Always returns 0 (success), indicating this function does not perform overflow checking
- The function assumes input values are valid and within reasonable ranges
- [Result](../R/Result.md) is stored in microseconds since midnight
- Part of PostgreSQL's date/time handling infrastructure in src/backend/utils/adt/date.c