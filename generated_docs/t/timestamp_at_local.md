# timestamp_at_local

## Location
src/backend/utils/adt/timestamp.c: 6692 - 6697

## Overview
A wrapper function for converting timestamp without timezone to timestamp with timezone using the session timezone, supporting the SQL AT LOCAL syntax.

## Definition


## Detailed Description
This function serves as a simple wrapper around timestamp_timestamptz to support PostgreSQL's AT LOCAL grammar syntax for timestamp conversion. It exists primarily to provide an overloaded function name that the SQL parser can use to distinguish between timestamp and timestamptz variants of the AT LOCAL operation.

The function converts a timestamp without timezone to a timestamp with timezone by assuming the input timestamp represents a time in the session's local timezone. This is a thin wrapper that delegates all actual conversion work to the timestamp_timestamptz function.

The existence of this wrapper is necessitated by PostgreSQL's grammar requirements for the AT LOCAL syntax, which needs to handle both timestamp and timestamptz input types with the same syntax but different underlying function implementations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Input timestamp without timezone (Timestamp)

## Dependencies
- Functions called/Symbols referenced:
  - timestamp_timestamptz (actual conversion function)
- Called from:
  - SQL queries using the AT LOCAL syntax with timestamp input

## Notes and Other Information
- This is a simple wrapper function created specifically for grammar overloading support
- The comment indicates this design was chosen because regression tests don't handle multiple functions with identical proargs/prosrc but different names
- Converts timestamp to timestamptz assuming the input is in session timezone
- Part of PostgreSQL's AT LOCAL syntax implementation
- Does not perform any validation or processing itself, purely delegates to timestamp_timestamptz
- Function is registered in PostgreSQL's system catalogs to support AT LOCAL grammar parsing