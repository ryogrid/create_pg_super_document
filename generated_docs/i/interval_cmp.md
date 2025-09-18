# interval_cmp

## Location
src/backend/utils/adt/timestamp.c: 2577 - 2592

## Overview
The interval_cmp function implements the three-way comparison function for PostgreSQL Interval data types, returning an integer indicating the relative ordering of two intervals.

## Definition
```c
Datum interval_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function compares two Interval values and returns an integer result indicating their relative ordering: negative if the first interval is less than the second, zero if they are equal, and positive if the first is greater than the second. This function serves as the basis for all interval comparison operations in PostgreSQL and is used by the SQL comparison operators as well as internal sorting and indexing operations. The function extracts two Interval arguments and delegates the comparison logic to `interval_cmp_internal`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First Interval pointer (interval1)
  - Argument 1: Second Interval pointer (interval2)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INTERVAL_P`: Extracts Interval arguments from function call context
  - [interval_cmp_internal](interval_cmp_internal.md): Performs the actual interval comparison logic
  - `PG_RETURN_INT32`: Returns integer result to PostgreSQL function call context
  - `Interval`: PostgreSQL interval data type structure

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:2577-2592
- Returns negative value when interval1 < interval2, zero when equal, positive when interval1 > interval2
- This is the primary comparison function used by PostgreSQL for interval ordering operations
- Used internally for sorting intervals and implementing B-tree index operations
- Standard PostgreSQL V1 function following the PG_FUNCTION_ARGS calling convention