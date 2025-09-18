# tm2timetz

## Location
[src/backend/utils/adt/date.c:2263-2272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2263-L2272)

## Overview
The tm2timetz function converts a broken-down time structure (pg_tm) with fractional seconds and timezone information into PostgreSQL's TimeTzADT (Time with Time Zone) data type.

## Definition
```c
int tm2timetz(struct pg_tm *tm, fsec_t fsec, int tz, TimeTzADT *result)
```

## Detailed Description
This function performs the conversion from a human-readable time representation to PostgreSQL's internal TimeTzADT format. It calculates the total microseconds since midnight by combining hours, minutes, seconds, and fractional seconds, then stores this along with the timezone offset in the result structure.

The conversion follows these steps:
1. Converts hours to minutes, then to seconds, then to microseconds
2. Adds the fractional seconds component
3. Stores the timezone offset separately
4. Returns 0 on success

The function is fundamental to PostgreSQL's time zone-aware time operations and is used during input parsing and internal time manipulations.

## Parameters / Member Variables
- `tm`: Pointer to a broken-down time structure containing hour, minute, and second components
- `fsec`: Fractional seconds component (in microseconds)
- `tz`: Timezone offset in seconds from UTC
- `result`: Pointer to the output TimeTzADT structure to store the converted time

## Dependencies
- Functions called/Symbols referenced:
  - MINS_PER_HOUR (constant)
  - SECS_PER_MINUTE (constant) 
  - USECS_PER_SEC (constant)
- Called from (representative examples):
  - [GetSQLCurrentTime](../G/GetSQLCurrentTime.md)
  - [timetz_in](timetz_in.md)
  - [timestamptz_timetz](timestamptz_timetz.md)
  - [parse_datetime](../p/parse_datetime.md)
  - PG_RETURN_TIMETZADT_P

## Notes and Other Information
- Returns 0 on successful conversion (always succeeds with valid input)
- The function performs direct arithmetic conversion without validation of input ranges
- Part of PostgreSQL's Time With Time Zone ADT implementation in src/backend/utils/adt/date.c
- The timezone offset is stored as seconds from UTC (positive for east of UTC, negative for west)
- Used extensively in time parsing and timezone conversion operations