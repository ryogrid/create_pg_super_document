# timestamp_cmp_date

## Location
src/backend/utils/adt/date.c: 961 - 969

## Overview
Compares a timestamp value with a date value and returns an integer indicating their relative ordering.

## Definition
```c
Datum timestamp_cmp_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the comparison operator between a timestamp and a date, returning a three-way comparison result. It extracts a timestamp and a date from the function arguments, then uses the internal comparison function `date_cmp_timestamp_internal` to perform the comparison. The function returns an integer value indicating the relative ordering of the two values.

The comparison is performed by delegating to `date_cmp_timestamp_internal(dateVal, dt1)` and negating the result. This negation is necessary because the internal function compares date to timestamp, but this function needs to return the result from the timestamp's perspective relative to the date.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - Argument 0: `Timestamp dt1` - The timestamp value to compare
  - Argument 1: `DateADT dateVal` - The date value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMESTAMP` - Extracts timestamp argument
  - `PG_GETARG_DATEADT` - Extracts date argument  
  - `[date_cmp_timestamp_internal](../d/date_cmp_timestamp_internal.md)` - Performs the actual comparison
  - `PG_RETURN_INT32` - Returns integer result
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This function is typically used by PostgreSQL's operator system for three-way comparisons between timestamp and date types
- Returns < 0 if timestamp < date, 0 if timestamp = date, > 0 if timestamp > date
- The result negation (`-date_cmp_timestamp_internal`) converts the date-to-timestamp comparison result to a timestamp-to-date comparison result
- Part of PostgreSQL's date/time ADT (Abstract Data Type) implementation in src/backend/utils/adt/date.c