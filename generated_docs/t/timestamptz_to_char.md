# timestamptz_to_char

## Location
src/backend/utils/adt/formatting.c: 4285 - 4325

## Overview
SQL callable function that formats a TIMESTAMPTZ (timestamp with time zone) value into a string according to a specified format template, implementing the `to_char(timestamptz, format)` SQL function.

## Definition
```c
Datum timestamptz_to_char(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the SQL interface for formatting TIMESTAMPTZ (timestamp with time zone) values into human-readable strings. It serves as the PostgreSQL built-in function `to_char()` when applied to TIMESTAMPTZ data types.

The function is nearly identical to `timestamp_to_char()` but includes additional timezone handling capabilities. It validates inputs for empty format strings and non-finite timestamps, returning NULL for invalid cases. The key difference is in the `timestamp2tm()` call, which extracts timezone information (`&tz`) and timezone name (`&tmtcTzn(&tmtc)`) alongside the standard timestamp breakdown.

Like its timestamp counterpart, it manually calculates day-of-week and day-of-year values, then delegates formatting to `datetime_to_char_body()`. The timezone information becomes available for use in format codes that display timezone details.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS convention:
  - Argument 0: `TimestampTz dt` - The timestamptz value to format  
  - Argument 1: `text *fmt` - The format string template containing formatting codes

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP
  - PG_GETARG_TEXT_PP  
  - TIMESTAMP_NOT_FINITE
  - timestamp2tm (with timezone extraction)
  - date2j (for day-of-week and day-of-year calculations)
  - ZERO_tmtc/tmtcTm/tmtcFsec/tmtcTzn (TmToChar manipulation macros)
  - COPY_tm
  - datetime_to_char_body
  - PG_GET_COLLATION
- Called from (representative examples):
  - SQL queries using to_char() function with timestamptz arguments
  - Direct SQL function calls

## Notes and Other Information
- Returns NULL for empty format strings or non-finite (infinity) timestamps
- Extracts timezone offset and timezone name information for formatting
- Manually calculates tm_wday and tm_yday since timestamp2tm() doesn't provide them  
- Uses same Julian day calculation as timestamp_to_char for day-of-week: (julianday + 1) % 7
- Day-of-year calculated as difference from January 1st of the same year plus 1
- Part of PostgreSQL's public SQL function interface, accessible via SQL to_char() calls
- Timezone information enables format codes like TZ, OF, etc. to display timezone details
- Handles timestamp overflow with appropriate error reporting using ERRCODE_DATETIME_VALUE_OUT_OF_RANGE