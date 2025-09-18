# timestamp_to_char

## Location
src/backend/utils/adt/formatting.c: 4250 - 4284

## Overview
SQL callable function that formats a TIMESTAMP value into a string according to a specified format template, implementing the `to_char(timestamp, format)` SQL function.

## Definition
```c
Datum timestamp_to_char(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the SQL interface for formatting TIMESTAMP (without time zone) values into human-readable strings. It serves as the PostgreSQL built-in function `to_char()` when applied to TIMESTAMP data types.

The function first validates inputs, checking for empty format strings and non-finite timestamps (infinity values), returning NULL for invalid inputs. It then converts the internal Timestamp representation to a broken-down time structure using `timestamp2tm()`, and manually calculates the day-of-week and day-of-year fields that are not provided by the timestamp conversion.

After preparing the time data in a TmToChar structure, it delegates the actual formatting work to `datetime_to_char_body()`. The function handles error conditions gracefully, particularly timestamp values outside the representable range.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS convention:
  - Argument 0: `Timestamp dt` - The timestamp value to format
  - Argument 1: `text *fmt` - The format string template containing formatting codes

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP
  - PG_GETARG_TEXT_PP
  - TIMESTAMP_NOT_FINITE
  - [timestamp2tm](timestamp2tm.md)
  - [date2j](../d/date2j.md) (for day-of-week and day-of-year calculations)
  - ZERO_tmtc/tmtcTm/tmtcFsec (TmToChar manipulation macros)
  - COPY_tm
  - [datetime_to_char_body](../d/datetime_to_char_body.md)
  - PG_GET_COLLATION
- Called from (representative examples):
  - SQL queries using to_char() function with timestamp arguments
  - Direct SQL function calls

## Notes and Other Information
- Returns NULL for empty format strings or non-finite (infinity) timestamps
- Manually calculates tm_wday and tm_yday since timestamp2tm() doesn't provide them
- Uses date2j() (Julian day conversion) for day-of-week calculation: (julianday + 1) % 7
- Day-of-year calculated as difference from January 1st of the same year plus 1
- Part of PostgreSQL's public SQL function interface, accessible via SQL to_char() calls
- Handles timestamp overflow with appropriate error reporting using ERRCODE_DATETIME_VALUE_OUT_OF_RANGE