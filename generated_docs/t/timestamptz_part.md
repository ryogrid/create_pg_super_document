# timestamptz_part

## Location
src/backend/utils/adt/timestamp.c: 5883 - 5888

## Overview
A PostgreSQL function that extracts specified date/time fields from timestamp with time zone values, serving as a wrapper around the internal `timestamptz_part_common` function.

## Definition
```c
Datum timestamptz_part(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamptz_part` function is a PostgreSQL SQL-callable function that extracts specific date/time components from timestamp with time zone (timestamptz) values. It serves as the backend implementation for the `date_part()` SQL function when operating on timestamptz values. The function acts as a simple wrapper that calls the shared implementation `timestamptz_part_common` with the `retnumeric` parameter set to `false`, indicating that results should be returned as floating-point numbers (float8) rather than numeric types.

Unlike its timestamp counterpart, this function handles timezone-aware timestamps and can extract timezone-specific fields such as timezone offset in hours, minutes, or seconds.

## Parameters / Member Variables
- Uses standard PostgreSQL function calling convention (`PG_FUNCTION_ARGS`)
- Arguments are accessed through the `fcinfo` structure:
  - Argument 0: Text field name (e.g., 'year', 'month', 'day', 'timezone', 'timezone_hour', etc.)
  - Argument 1: TimestampTz value to extract from

## Dependencies
- Functions called/Symbols referenced:
  - [timestamptz_part_common](timestamptz_part_common.md) (shared implementation for timestamptz field extraction)
- Called from (representative examples):
  - SQL function `date_part()` when used with timestamptz arguments
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This function is the non-numeric variant of timestamptz field extraction (returns float8)
- Counterpart to `extract_timestamptz()` which returns numeric values  
- Supports timezone-specific extractions not available in the plain timestamp variant
- Part of PostgreSQL's date/time function family alongside `timestamp_part()` for timezone-unaware timestamps
- Located in `src/backend/utils/adt/timestamp.c:5883-5886`