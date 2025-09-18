# timestamptz_at_local

## Location
src/backend/utils/adt/timestamp.c: 6698 - 6701

## Overview
A wrapper function for converting timestamp with timezone to timestamp without timezone using the session timezone, supporting the SQL AT LOCAL syntax.

## Definition


## Detailed Description
This function serves as a simple wrapper around timestamptz_timestamp to support PostgreSQL's AT LOCAL grammar syntax for timestamp conversion. It exists primarily to provide an overloaded function name that the SQL parser can use to distinguish between timestamp and timestamptz variants of the AT LOCAL operation.

The function converts a timestamp with timezone to a timestamp without timezone by converting the timestamptz value to the equivalent time in the session's local timezone and then removing the timezone information. This is a thin wrapper that delegates all actual conversion work to the timestamptz_timestamp function.

Like its counterpart timestamp_at_local, this wrapper exists to support the grammar requirements for AT LOCAL syntax, allowing the same SQL syntax to work with both timestamp and timestamptz input types while calling the appropriate underlying conversion functions.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Input timestamp with timezone (TimestampTz)

## Dependencies
- Functions called/Symbols referenced:
  - timestamptz_timestamp (actual conversion function)
- Called from:
  - SQL queries using the AT LOCAL syntax with timestamptz input

## Notes and Other Information
- This is a simple wrapper function created specifically for grammar overloading support
- Companion function to timestamp_at_local, handling the reverse conversion direction
- Converts timestamptz to timestamp by converting to session timezone and removing timezone info
- Part of PostgreSQL's AT LOCAL syntax implementation
- Does not perform any validation or processing itself, purely delegates to timestamptz_timestamp
- Function is registered in PostgreSQL's system catalogs to support AT LOCAL grammar parsing
- The conversion uses session_timezone for determining the local timezone context