# timeofday

## Location
src/backend/utils/adt/timestamp.c: 1700 - 1729

## Overview
Returns the current system time as a formatted text string, providing high-precision timing information including microseconds and timezone.

## Definition
Datum timeofday(PG_FUNCTION_ARGS)

## Detailed Description
This function implements PostgreSQL's timeofday() SQL function, which returns the actual current system time (not transaction start time) as a human-readable text string. Unlike other timestamp functions that return the transaction start time for consistency, timeofday() returns the real current time each time it's called, making it useful for measuring elapsed time within a transaction or for high-precision timing.

The function uses gettimeofday() to obtain the current system time with microsecond precision, formats it using pg_strftime() with a specific format template, and returns the result as a PostgreSQL text datum. The format includes day of week, month, day, time with microseconds, year, and timezone abbreviation.

## Parameters / Member Variables
- No parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)

## Dependencies
- Functions called/Symbols referenced:
  - gettimeofday
  - pg_strftime
  - pg_localtime
  - cstring_to_text
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No references found (likely called directly via SQL)

## Notes and Other Information
- The function is located in src/backend/utils/adt/timestamp.c:1700-1729
- Returns text type (Datum) containing formatted timestamp string
- Unlike transaction-consistent timestamp functions, this returns actual current time on each call
- Provides microsecond precision through tp.tv_usec
- Uses session_timezone for timezone conversion
- Format string: "%%a %%b %%d %%H:%%M:%%S.%%06d %%Y %%Z" produces output like "Mon Jan 15 14:30:25.123456 2024 PST"
- Useful for performance measurement and debugging within transactions
- Part of PostgreSQL's extended timestamp function set beyond SQL standard functions