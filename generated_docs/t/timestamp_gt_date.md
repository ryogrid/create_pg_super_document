# timestamp_gt_date

## Location
[src/backend/utils/adt/date.c:934-942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L934-L942)

## Overview
Compares a timestamp value with a date value to determine if the timestamp is greater than the date.

## Definition

```c
Datum
timestamp_gt_date(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the greater-than comparison operator between a timestamp and a date. It extracts a timestamp and a date from the function arguments, then uses the internal comparison function  to perform the comparison. The function returns true if the timestamp is greater than the date, false otherwise.

The comparison is performed by delegating to  and checking if the result is less than 0, which indicates that the date is less than the timestamp (i.e., timestamp > date).

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0:  - The timestamp value to compare
  - Argument 1:  - The date value to compare against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts timestamp argument
  -  - Extracts date argument  
  -  - Performs the actual comparison
  -  - Returns boolean result
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's SQL operator system when using the '>' operator between timestamp and date types
- The comparison logic is implemented in  which handles the conversion and comparison details
- Part of PostgreSQL's date/time ADT (Abstract Data Type) implementation in src/backend/utils/adt/date.c

## Simplified Source

```c
Datum timestamp_gt_date(PG_FUNCTION_ARGS) {
    // Extract timestamp and date arguments
    Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
    DateADT date = PG_GETARG_DATEADT(1);

    // Return true if timestamp > date (comparison result < 0)
    return PG_RETURN_BOOL(date_cmp_timestamp_internal(date, timestamp) < 0);
}
```