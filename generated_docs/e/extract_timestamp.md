# extract_timestamp

## Location
[src/backend/utils/adt/timestamp.c:5617-5625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5617-L5625)

## Overview
A PostgreSQL function that extracts specified date/time fields from timestamp values, returning results as numeric types rather than floating-point values.

## Definition
```c
Datum extract_timestamp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `extract_timestamp` function is a PostgreSQL SQL-callable function that extracts specific date/time components from timestamp values. It serves as the backend implementation for the `EXTRACT()` SQL function when operating on timestamp (without time zone) values. The function acts as a wrapper that calls the shared implementation `timestamp_part_common` with the `retnumeric` parameter set to `true`, indicating that results should be returned as numeric types for higher precision rather than floating-point numbers.

## Parameters / Member Variables
- Arguments are accessed through the `fcinfo` structure:
  - Argument 0: Text field name (e.g., 'year', 'month', 'day', 'hour', etc.)
  - Argument 1: Timestamp value to extract from

## Dependencies
- Functions called/Symbols referenced:
  - [timestamp_part_common](../t/timestamp_part_common.md) (shared implementation for timestamp field extraction)
- Called from (representative examples):
  - SQL function `EXTRACT()` when used with timestamp arguments
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This function is the numeric variant of timestamp field extraction (returns numeric type)
- Counterpart to `timestamp_part()` which returns float8 values
- The numeric return type provides higher precision for fractional seconds and other precise measurements
- Part of PostgreSQL's date/time function family alongside `extract_timestamptz()` for timezone-aware timestamps
- Located in `src/backend/utils/adt/timestamp.c:5617-5620`

## Simplified Source

```c
Datum extract_timestamp(PG_FUNCTION_ARGS) {
    // Delegate to common implementation with numeric return type for precision
    return timestamp_part_common(fcinfo, true);
}
```