# timetz_ge

## Location
[src/backend/utils/adt/date.c:2515-2523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2515-L2523)

## Overview
The timetz_ge function compares two time with time zone values and returns true if the first time is greater than or equal to the second time.

## Definition
Datum timetz_ge(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the greater-than-or-equal-to comparison operator for the TimeTzADT (time with time zone) data type. It extracts two TimeTzADT values from the function arguments and uses the internal comparison function timetz_cmp_internal to determine their relative ordering. The function returns true if the first time value is considered greater than or equal to the second.

Like timetz_gt, the comparison is performed by first converting both times to GMT-equivalent values (adding the timezone offset) and comparing those. If the GMT times are equal, the comparison falls back to comparing the timezone values themselves. The function returns true for both greater-than and equal-to cases.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function argument structure containing:
  - Argument 0: First TimeTzADT value (time1)
  - Argument 1: Second TimeTzADT value (time2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P: Extracts TimeTzADT arguments from function call
  - [timetz_cmp_internal](timetz_cmp_internal.md): Internal comparison function that performs the actual comparison logic
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL function call framework
- Data types used:
  - TimeTzADT: Structure containing time (TimeADT) and zone (int32) fields
- Called from (representative examples):
  - SQL greater-than-or-equal-to operator (>=) for timetz data type

## Notes and Other Information
- This function is part of PostgreSQL's type system infrastructure and is typically called through SQL operators rather than directly
- The comparison logic ensures proper handling of timezone differences by normalizing both times to GMT before comparison
- Returns a PostgreSQL Datum containing a boolean value indicating whether time1 >= time2
- Located in src/backend/utils/adt/date.c:2515-2523
- Very similar to timetz_gt but includes equality in the comparison (>= 0 instead of > 0)

## Simplified Source

```c
Datum timetz_ge(PG_FUNCTION_ARGS) {
    // Extract two time-with-timezone values from arguments
    TimeTzADT *time1 = PG_GETARG_TIMETZADT_P(0);
    TimeTzADT *time2 = PG_GETARG_TIMETZADT_P(1);

    // Compare times using internal comparison function
    // Returns true if time1 >= time2 (accounting for timezone differences)
    PG_RETURN_BOOL(timetz_cmp_internal(time1, time2) >= 0);
}
```