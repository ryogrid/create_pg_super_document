# generate_series_timestamptz_at_zone

## Location
[src/backend/utils/adt/timestamp.c:6678-6691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6678-L6691)

## Overview
A PostgreSQL built-in function that generates a series of timestamp with timezone values using a specified timezone for arithmetic operations.

## Definition

```c
Datum
generate_series_timestamptz_at_zone(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the public SQL-callable wrapper for generating timestamp series with timezone support using a user-specified timezone. It delegates all actual work to the internal function generate_series_timestamptz_internal, but allows the caller to specify a particular timezone for performing interval arithmetic operations rather than using the session timezone.

The function takes four arguments: start timestamp, end timestamp, step interval, and timezone specification. This provides more control over timezone handling compared to the basic generate_series_timestamptz function, which always uses the session timezone.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
## Dependencies
- Functions called/Symbols referenced:
  - [generate_series_timestamptz_internal](generate_series_timestamptz_internal.md) (core implementation function)
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

## Simplified Source

```c
Datum
generate_series_timestamptz_at_zone(PG_FUNCTION_ARGS)
{
    // Simple wrapper that delegates to internal implementation
    // Fourth parameter will be the timezone specification
    return generate_series_timestamptz_internal(fcinfo);
}
```