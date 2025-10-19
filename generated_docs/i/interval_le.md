# interval_le

## Location
[src/backend/utils/adt/timestamp.c:2559-2567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2559-L2567)

## Overview
The interval_le function implements the "less than or equal to" comparison operator (<=) for PostgreSQL Interval data types.

## Definition
```c
Datum interval_le(PG_FUNCTION_ARGS)
```

## Detailed Description
This function compares two Interval values and returns true if the first interval is less than or equal to the second interval. It serves as a PostgreSQL function implementation that can be called from SQL queries using the `<=` operator on interval data types. The function extracts two Interval arguments from the PostgreSQL function call context and delegates the actual comparison logic to the internal `interval_cmp_internal` function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First Interval pointer (interval1)
  - Argument 1: Second Interval pointer (interval2)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INTERVAL_P`: Extracts Interval arguments from function call context
  - [interval_cmp_internal](interval_cmp_internal.md): Performs the actual interval comparison logic
  - `PG_RETURN_BOOL`: Returns boolean result to PostgreSQL function call context
  - `Interval`: PostgreSQL interval data type structure

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:2559-2567
- The function returns true (<=) when interval_cmp_internal returns a value <= 0
- This is a standard PostgreSQL V1 function that follows the PG_FUNCTION_ARGS calling convention
- Used internally by PostgreSQL to implement the <= operator for interval comparisons in SQL queries

## Simplified Source

```c
Datum interval_le(PG_FUNCTION_ARGS) {
    // Extract two interval arguments
    Interval *interval1 = PG_GETARG_INTERVAL_P(0);
    Interval *interval2 = PG_GETARG_INTERVAL_P(1);

    // Compare intervals and return true if first is less than or equal to second
    PG_RETURN_BOOL(interval_cmp_internal(interval1, interval2) <= 0);
}
```