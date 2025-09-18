# timestamp_ge_date

## Location
src/backend/utils/adt/date.c: 952 - 960

## Overview
Compares a timestamp value with a date value to determine if the timestamp is greater than or equal to the date.

## Definition
```c
Datum timestamp_ge_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the greater-than-or-equal-to comparison operator between a timestamp and a date. It extracts a timestamp and a date from the function arguments, then uses the internal comparison function `date_cmp_timestamp_internal` to perform the comparison. The function returns true if the timestamp is greater than or equal to the date, false otherwise.

The comparison is performed by delegating to `date_cmp_timestamp_internal(dateVal, dt1)` and checking if the result is less than or equal to 0, which indicates that the date is less than or equal to the timestamp (i.e., timestamp >= date).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - Argument 0: `Timestamp dt1` - The timestamp value to compare
  - Argument 1: `DateADT dateVal` - The date value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TIMESTAMP` - Extracts timestamp argument
  - `PG_GETARG_DATEADT` - Extracts date argument  
  - [date_cmp_timestamp_internal](../d/date_cmp_timestamp_internal.md) - Performs the actual comparison
  - `PG_RETURN_BOOL` - Returns boolean result
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's SQL operator system when using the '>=' operator between timestamp and date types
- The comparison logic is implemented in `date_cmp_timestamp_internal` which handles the conversion and comparison details
- Part of PostgreSQL's date/time ADT (Abstract Data Type) implementation in src/backend/utils/adt/date.c