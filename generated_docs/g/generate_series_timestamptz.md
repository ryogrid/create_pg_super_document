# generate_series_timestamptz

## Location
src/backend/utils/adt/timestamp.c: 6672 - 6677

## Overview
A PostgreSQL built-in function that generates a series of timestamp with timezone values using the session timezone for arithmetic operations.

## Definition


## Detailed Description
This function serves as the public SQL-callable wrapper for generating timestamp series with timezone support. It delegates all actual work to the internal function generate_series_timestamptz_internal, using the session timezone for interval arithmetic operations. This function corresponds to the SQL function generate_series(timestamptz, timestamptz, interval) that can be called directly from SQL queries.

The function takes three arguments: start timestamp, end timestamp, and step interval, and returns a set of timestamp values with timezone information. All arithmetic operations are performed in the session's current timezone context.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Start timestamp with timezone (TimestampTz)
  - Finish timestamp with timezone (TimestampTz)
  - Step interval (Interval)

## Dependencies
- Functions called/Symbols referenced:
  - generate_series_timestamptz_internal (core implementation function)
- Called from:
  - SQL queries using generate_series(timestamptz, timestamptz, interval) syntax

## Notes and Other Information
- This is a thin wrapper function that provides the SQL-callable interface
- Uses session_timezone for all interval arithmetic operations
- For timezone-specific operations, use generate_series_timestamptz_at_zone instead
- Function is registered in PostgreSQL's system catalogs as a built-in function
- Returns a set of timestamptz values, making it usable in FROM clauses and other set-returning contexts