# time2tm

## Location
[src/backend/utils/adt/date.c:1488-1500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1488-L1500)

## Overview
Converts a PostgreSQL TimeADT value to a broken-down time structure, extracting hour, minute, second, and fractional second components.

## Definition
```c
int time2tm(TimeADT time, struct pg_tm *tm, fsec_t *fsec)
```

## Detailed Description
The time2tm function performs the inverse operation of tm2time, converting PostgreSQL's internal time representation (TimeADT, stored as microseconds since midnight) back into broken-down time components. It uses integer division and modulo operations to extract hours, minutes, seconds, and remaining fractional seconds. Only the time-related fields of the tm structure are filled; date-related fields are not modified.

## Parameters / Member Variables
- `time`: Input TimeADT value representing microseconds since midnight
- `tm`: Pointer to pg_tm structure where time components will be stored
- `fsec`: Pointer to fsec_t where fractional seconds (in microseconds) will be stored

## Dependencies
- Functions called/Symbols referenced:
  - USECS_PER_HOUR (constant)
  - USECS_PER_MINUTE (constant)
  - USECS_PER_SEC (constant)
- Types used:
  - TimeADT (time abstract data type)
  - [pg_tm](../p/pg_tm.md) (struct)
  - fsec_t (fractional seconds type)
- Called from (representative examples):
  - [time_out](time_out.md)
  - [time_part_common](time_part_common.md)
  - [time_timetz](time_timetz.md)
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md)
  - PG_RETURN_TIMETZADT_P

## Notes and Other Information
- Always returns 0 (success), indicating this function does not perform error checking
- Only fills in tm_hour, tm_min, tm_sec fields of the tm structure
- Fractional seconds are returned separately in the fsec parameter
- Uses successive division and subtraction to break down the total microseconds
- Complementary function to tm2time for time conversion operations
- Part of PostgreSQL's date/time handling infrastructure in src/backend/utils/adt/date.c