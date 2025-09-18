# interval_to_char

## Location
[src/backend/utils/adt/formatting.c:4326-4367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L4326-L4367)

## Overview
SQL callable function that formats an INTERVAL value into a string according to a specified format template, implementing the `to_char(interval, format)` SQL function.

## Definition
```c
Datum interval_to_char(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the SQL interface for formatting INTERVAL values into human-readable strings. It serves as the PostgreSQL built-in function `to_char()` when applied to INTERVAL data types, enabling formatted output of time spans and durations.

The function differs from timestamp formatting functions in its handling of interval-specific semantics. It converts the internal Interval representation to a broken-down interval structure (`pg_itm`) using `interval2itm()`, then maps the interval components to the standard time structure format used by the formatting engine.

A key distinction is in the `tm_yday` calculation, which for intervals approximates the total span in days using the formula: `(years * 12 + months) * 30 + days`. This provides a rough total day count for interval formatting purposes. The `tm_wday` field is meaningless for intervals since day-of-week concepts don't apply to durations.

The function calls `datetime_to_char_body()` with `is_interval=true` to enable interval-specific formatting behavior and passes the interval flag to ensure appropriate handling of negative intervals and duration-specific format codes.

## Parameters / Member Variables  
- Uses PostgreSQL's PG_FUNCTION_ARGS convention:
  - Argument 0: `Interval *it` - The interval value to format
  - Argument 1: `text *fmt` - The format string template containing formatting codes

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P
  - PG_GETARG_TEXT_PP
  - INTERVAL_NOT_FINITE
  - [interval2itm](interval2itm.md)
  - ZERO_tmtc/tmtcTm (TmToChar manipulation macros)
  - [datetime_to_char_body](../d/datetime_to_char_body.md) (with is_interval=true)
  - PG_GET_COLLATION
  - MONTHS_PER_YEAR/DAYS_PER_MONTH (constants for yday calculation)
- Called from (representative examples):
  - SQL queries using to_char() function with interval arguments
  - Direct SQL function calls

## Notes and Other Information
- Returns NULL for empty format strings or non-finite (infinity) intervals
- Uses interval2itm() to break down interval into component fields (years, months, days, hours, minutes, seconds, microseconds)
- tm_yday approximates total interval span: (years × 12 + months) × 30 + days
- tm_wday field is meaningless for intervals and left undefined
- Passes `is_interval=true` to datetime_to_char_body for interval-specific formatting
- Part of PostgreSQL's public SQL function interface, accessible via SQL to_char() calls
- Supports negative intervals and interval-specific format codes
- Microseconds stored in tmtc.fsec field for sub-second precision formatting