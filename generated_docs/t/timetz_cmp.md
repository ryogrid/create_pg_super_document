# timetz_cmp

## Location
[src/backend/utils/adt/date.c:2524-2532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2524-L2532)

## Overview
The timetz_cmp function provides a three-way comparison of two time with time zone values, returning an integer indicating their relative ordering.

## Definition
Datum timetz_cmp(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the comparison operator for the TimeTzADT (time with time zone) data type that returns a signed integer result instead of a boolean. It extracts two TimeTzADT values from the function arguments and delegates to the internal comparison function timetz_cmp_internal to determine their relative ordering. The function returns:
- A negative value if time1 < time2
- Zero if time1 = time2  
- A positive value if time1 > time2

This function serves as the foundation for all other timetz comparison operations and is also used by PostgreSQL's sorting and indexing mechanisms. The comparison logic first converts both times to GMT-equivalent values by adding their respective timezone offsets, then compares those normalized times. If the GMT times are equal, it falls back to comparing the timezone values to ensure complete ordering.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function argument structure containing:
  - Argument 0: First TimeTzADT value (time1)
  - Argument 1: Second TimeTzADT value (time2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P: Extracts TimeTzADT arguments from function call
  - [timetz_cmp_internal](timetz_cmp_internal.md): Internal comparison function that performs the actual comparison logic
  - PG_RETURN_INT32: Returns integer result to PostgreSQL function call framework
- Data types used:
  - TimeTzADT: Structure containing time (TimeADT) and zone (int32) fields
- Called from (representative examples):
  - [compareDatetime](../c/compareDatetime.md): Used in JSON path execution for datetime comparisons
  - PostgreSQL sorting and indexing operations
  - Other timetz comparison functions (timetz_gt, timetz_ge, etc.)

## Notes and Other Information
- This is the core comparison function for timetz data type, used by PostgreSQL's internal sorting and comparison infrastructure
- Unlike the boolean comparison functions (timetz_gt, timetz_ge, etc.), this returns the actual comparison result as an integer
- The function ensures proper timezone handling by normalizing times to GMT before comparison
- Returns a PostgreSQL Datum containing a signed 32-bit integer
- Located in src/backend/utils/adt/date.c:2524-2532
- Essential for B-tree indexing and ORDER BY operations on timetz columns

## Simplified Source

```c
Datum timetz_cmp(PG_FUNCTION_ARGS) {
    TimeTzADT *time1 = PG_GETARG_TIMETZADT_P(0);
    TimeTzADT *time2 = PG_GETARG_TIMETZADT_P(1);

    PG_RETURN_INT32(timetz_cmp_internal(time1, time2));
}
```