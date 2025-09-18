# timestamp_out

## Location
src/backend/utils/adt/timestamp.c: 232 - 257

## Overview
A PostgreSQL output function that converts internal timestamp values to their external string representation, handling both finite timestamps and special values like infinity.

## Definition


## Detailed Description
This function implements the output conversion for the TIMESTAMP data type (without timezone). It takes PostgreSQL's internal timestamp representation and converts it to a human-readable string format. The function handles both regular timestamp values and special values like 'infinity' and '-infinity'.

The conversion process involves checking for special values first, then breaking down finite timestamps into their component parts (year, month, day, hour, minute, second, microseconds) and formatting them according to the current DateStyle setting. The output format respects PostgreSQL's configuration settings for date and time display.

## Parameters / Member Variables
- Function follows PostgreSQL's fmgr calling convention (PG_FUNCTION_ARGS)
-  (arg 0): Internal timestamp value to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP: Macro to extract timestamp argument
  - TIMESTAMP_NOT_FINITE: Macro to check for infinity values
  - EncodeSpecialTimestamp: Handles formatting of special values (infinity, -infinity)
  - timestamp2tm: Converts internal timestamp to broken-down time structure
  - EncodeDateTime: Formats broken-down time into string representation
  - pstrdup: Creates a copy of the formatted string for return
  - PG_RETURN_CSTRING: Return value macro
- Called from:
  - ExecGetJsonValueItemString (src/backend/executor/execExprInterp.c:4522)
  - Used as output function for TIMESTAMP type (registered in pg_type catalog)

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_NOT_FINITE
  - EncodeSpecialTimestamp
  - timestamp2tm
  - EncodeDateTime
  - DateStyle (global variable)
  - ereport (for error reporting)
- Called from:
  - ExecGetJsonValueItemString (src/backend/executor/execExprInterp.c:4522)

## Notes and Other Information
- Handles special timestamp values: 'infinity' and '-infinity' are formatted as special strings
- Output format depends on the DateStyle setting (ISO, Postgres, SQL, German styles)
- Performs range checking and reports errors for timestamps that cannot be represented
- Returns a newly allocated string that the caller is responsible for freeing
- The function never applies timezone conversion since TIMESTAMP is timezone-naive
- Error handling includes specific error codes for out-of-range values
- Part of PostgreSQL's type system infrastructure for displaying timestamp values