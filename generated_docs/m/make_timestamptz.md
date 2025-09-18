# make_timestamptz

## Location
src/backend/utils/adt/timestamp.c: 674 - 694

## Overview
PostgreSQL SQL function constructor that creates a timestamp with timezone from individual date and time components using the session timezone.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL-callable function that constructs a timestamp with timezone value from separate year, month, day, hour, minute, and second components. This function creates a timestamptz value by first constructing a plain timestamp using , then converting it to timestamptz using the current session's timezone setting via .

The function follows the same parameter extraction pattern as  but returns a timestamptz (timestamp with timezone) instead of a plain timestamp. The timezone conversion assumes the input date/time components are in the session's current timezone.

This function corresponds to the SQL function  that can be called from SQL queries.

## Parameters / Member Variables
- Function takes 6 PostgreSQL function arguments accessed via PG_GETARG macros:
  -  (int32): The year component
  -  (int32): The month component (1-12)
  -  (int32): The day component (1-31)
  -  (int32): The hour component (0-23)
  -  (int32): The minute component (0-59)
  -  (float8): The second component with fractional precision

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro)
  - PG_GETARG_FLOAT8 (macro)
  - make_timestamp_internal
  - timestamp2timestamptz
  - PG_RETURN_TIMESTAMPTZ (macro)
- Called from:
  - SQL queries (via function call mechanism)

## Notes and Other Information
- This is a PostgreSQL built-in SQL function accessible from SQL statements
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- The input date/time components are interpreted in the session's current timezone
- All validation and error handling for date/time components is performed by make_timestamp_internal
- The timezone conversion is handled by timestamp2timestamptz using the session timezone
- Can be called from SQL as: SELECT make_timestamptz(2023, 12, 25, 10, 30, 45.5);
- Result includes timezone information unlike the plain make_timestamp function