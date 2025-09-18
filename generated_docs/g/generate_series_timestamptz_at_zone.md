# generate_series_timestamptz_at_zone

## Location
src/backend/utils/adt/timestamp.c: 6678 - 6691

## Overview
A PostgreSQL built-in function that generates a series of timestamp with timezone values using a specified timezone for arithmetic operations.

## Definition


## Detailed Description
This function serves as the public SQL-callable wrapper for generating timestamp series with timezone support using a user-specified timezone. It delegates all actual work to the internal function generate_series_timestamptz_internal, but allows the caller to specify a particular timezone for performing interval arithmetic operations rather than using the session timezone.

The function takes four arguments: start timestamp, end timestamp, step interval, and timezone specification. This provides more control over timezone handling compared to the basic generate_series_timestamptz function, which always uses the session timezone.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Start timestamp with timezone (TimestampTz)
  - Finish timestamp with timezone (TimestampTz) 
  - Step interval (Interval)
  - Timezone specification (text) - timezone name or abbreviation

## Dependencies
- Functions called/Symbols referenced:
  - generate_series_timestamptz_internal (core implementation function)
- Called from:
  - SQL queries using generate_series(timestamptz, timestamptz, interval, text) syntax

## Notes and Other Information
- This is a thin wrapper function that provides the SQL-callable interface with timezone parameter
- Allows explicit timezone specification for interval arithmetic operations
- The timezone parameter is resolved by lookup_timezone() within the internal function
- More flexible than generate_series_timestamptz which always uses session_timezone
- Function is registered in PostgreSQL's system catalogs as a built-in function
- Returns a set of timestamptz values, making it usable in FROM clauses and other set-returning contexts
- Useful for generating timestamp series that need consistent timezone handling regardless of session settings