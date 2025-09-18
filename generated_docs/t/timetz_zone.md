# timetz_zone

## Location
[src/backend/utils/adt/date.c:3060-3121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L3060-L3121)

## Overview
Converts a time with time zone to a different time zone, applying appropriate DST rules as of the transaction start time.

## Definition


## Detailed Description
`timetz_zone` is a PostgreSQL built-in function that converts a time with time zone (TIMETZ) value to a different time zone. The function takes a timezone specification (as text) and a TIMETZ value, then returns a new TIMETZ value adjusted to the specified timezone. It handles three types of timezone specifications: fixed-offset abbreviations (e.g., '+05:00'), dynamic-offset abbreviations that change based on DST rules, and named timezones (e.g., 'America/New_York').

The conversion process adjusts the time component by the difference between the original and target timezone offsets, ensuring the result stays within the valid time range of a day (0 to 24 hours). DST rules are applied using the transaction start timestamp as the reference point for determining the appropriate offset.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `zone` (text): The target timezone specification (timezone name, abbreviation, or offset)
  - `t` (TimeTzADT*): The input time with time zone value to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - PG_GETARG_TIMETZADT_P
  - text_to_cstring_buffer
  - [DecodeTimezoneName](../D/DecodeTimezoneName.md)
  - [GetCurrentTransactionStartTimestamp](../G/GetCurrentTransactionStartTimestamp.md)
  - [DetermineTimeZoneAbbrevOffsetTS](../D/DetermineTimeZoneAbbrevOffsetTS.md)
  - [timestamp2tm](timestamp2tm.md)
  - [palloc](../p/palloc.md)
  - PG_RETURN_TIMETZADT_P
  - ereport
- Called from (representative examples):
  - [timetz_at_local](timetz_at_local.md)
  - SQL timezone conversion operations
  - AT TIME ZONE expressions with TIMETZ values

## Notes and Other Information
- Handles three timezone specification types: TZNAME_FIXED_OFFSET, TZNAME_DYNTZ, and named timezones
- Uses transaction start timestamp as reference for DST rule application, ensuring consistent results within a transaction
- Performs modular arithmetic to ensure the resulting time stays within a 24-hour period
- Applies C99 modulo correction for negative time values to handle timezone conversions that cross midnight
- The time adjustment formula: `result->time = t->time + (t->zone - tz) * USECS_PER_SEC`
- Memory allocation for result uses palloc() for proper PostgreSQL memory management
- Timezone offset values are stored in seconds from UTC, with negative values representing east of UTC