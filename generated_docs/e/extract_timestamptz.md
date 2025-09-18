# extract_timestamptz

## Location
src/backend/utils/adt/timestamp.c: 5889 - 5905

## Overview
A PostgreSQL function that extracts specified date/time fields from timestamp with time zone values, returning results as numeric types for higher precision.

## Definition
```c
Datum extract_timestamptz(PG_FUNCTION_ARGS)
```

## Detailed Description
The `extract_timestamptz` function is a PostgreSQL SQL-callable function that extracts specific date/time components from timestamp with time zone (timestamptz) values. It serves as the backend implementation for the `EXTRACT()` SQL function when operating on timestamptz values. The function acts as a wrapper that calls the shared implementation `timestamptz_part_common` with the `retnumeric` parameter set to `true`, indicating that results should be returned as numeric types for higher precision rather than floating-point numbers.

Like `timestamptz_part`, this function handles timezone-aware timestamps and can extract timezone-specific fields, but provides numeric precision for applications requiring exact decimal representations, particularly important for fractional seconds and epoch time calculations.

## Parameters / Member Variables
- Uses standard PostgreSQL function calling convention (`PG_FUNCTION_ARGS`)
- Arguments are accessed through the `fcinfo` structure:
  - Argument 0: Text field name (e.g., 'year', 'month', 'day', 'timezone', 'epoch', etc.)
  - Argument 1: TimestampTz value to extract from

## Dependencies
- Functions called/Symbols referenced:
  - `[timestamptz_part_common](../t/timestamptz_part_common.md)` (shared implementation for timestamptz field extraction)
- Called from (representative examples):
  - SQL function `EXTRACT()` when used with timestamptz arguments
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This function is the numeric variant of timestamptz field extraction (returns numeric type)
- Counterpart to `timestamptz_part()` which returns float8 values
- The numeric return type provides higher precision for fractional seconds, epoch calculations, and timezone offsets
- Supports timezone-specific extractions not available in the plain timestamp variant
- Particularly useful for applications requiring exact decimal precision in temporal calculations
- Part of PostgreSQL's date/time function family alongside `extract_timestamp()` for timezone-unaware timestamps
- Located in `src/backend/utils/adt/timestamp.c:5889-5892`