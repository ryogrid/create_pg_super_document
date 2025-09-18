# make_timestamptz_at_timezone

## Location
src/backend/utils/adt/timestamp.c: 695 - 734

## Overview
PostgreSQL SQL function constructor that creates a timestamp with timezone from individual date and time components using a specified timezone rather than the session timezone.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL-callable function that constructs a timestamp with timezone value from separate year, month, day, hour, minute, second, and timezone components. Unlike  which uses the session timezone, this function accepts an explicit timezone as the seventh parameter.

The function works by:
1. First constructing a plain timestamp using 
2. Converting the timestamp to a broken-down time structure using 
3. Parsing the provided timezone string using 
4. Converting the timestamp to the specified timezone using 
5. Performing final validation before returning the timestamptz result

This provides more precise control over timezone handling than the session-based  function.

## Parameters / Member Variables
- Function takes 7 PostgreSQL function arguments accessed via PG_GETARG macros:
  -  (int32): The year component
  -  (int32): The month component (1-12)
  -  (int32): The day component (1-31)
  -  (int32): The hour component (0-23)
  -  (int32): The minute component (0-59)
  -  (float8): The second component with fractional precision
  -  (text): The timezone specification (e.g., 'UTC', 'America/New_York')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro)
  - PG_GETARG_FLOAT8 (macro)
  - PG_GETARG_TEXT_PP (macro)
  - [make_timestamp_internal](make_timestamp_internal.md)
  - [timestamp2tm](../t/timestamp2tm.md)
  - [parse_sane_timezone](../p/parse_sane_timezone.md)
  - [dt2local](../d/dt2local.md)
  - IS_VALID_TIMESTAMP
  - PG_RETURN_TIMESTAMPTZ (macro)
- Called from:
  - SQL queries (via function call mechanism)

## Notes and Other Information
- This is a PostgreSQL built-in SQL function accessible from SQL statements
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- The input date/time components are interpreted in the explicitly specified timezone
- Provides more precise timezone control than make_timestamptz which uses session timezone
- All validation and error handling for date/time components is performed by make_timestamp_internal
- Additional validation occurs after timezone conversion to ensure the result remains valid
- The timezone parameter accepts various formats handled by parse_sane_timezone
- Can be called from SQL as: SELECT make_timestamptz_at_timezone(2023, 12, 25, 10, 30, 45.5, 'UTC');
- Throws ERRCODE_DATETIME_VALUE_OUT_OF_RANGE errors for various validation failures