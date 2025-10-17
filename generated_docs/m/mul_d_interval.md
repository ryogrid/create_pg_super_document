# mul_d_interval

## Location
[src/backend/utils/adt/timestamp.c:3687-3696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L3687-L3696)

## Overview
A PostgreSQL function that provides a wrapper for interval multiplication with reversed argument order (factor * interval).

## Definition

```c
Datum
mul_d_interval(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a simple wrapper around  with the argument order reversed. It takes a floating-point factor as the first argument and an interval as the second argument, then delegates to  by swapping the argument order. This allows PostgreSQL to support both  and 0:
1:
5: 5
6: 2 3 syntax in SQL.

The function uses  to directly invoke  with the arguments reordered, avoiding the overhead of a full function call through the fmgr interface.

## Parameters / Member Variables
- Function uses  calling convention:
  - Argument 0: Floating-point multiplication factor (float8)
  - Argument 1: Interval to multiply
- Returns: Datum containing the resulting interval (same as interval_mul)

## Dependencies
- Functions called/Symbols referenced:
  -  (extract generic arguments)
  -  (direct function call mechanism)
  -  (the actual multiplication implementation)
- Called from (representative examples):
  - No direct references found (likely used through SQL operator system)

## Notes and Other Information
- This is a PostgreSQL V1 calling convention function, accessible from SQL to support commutative multiplication syntax
- Acts as a thin wrapper to enable 0:
1:
5: 5
6: 2 3 syntax alongside 
- Uses generic Datum arguments to avoid type-specific argument extraction
- Directly delegates all actual computation to 
- Part of PostgreSQL's operator system for providing symmetric multiplication operations
- Located in src/backend/utils/adt/timestamp.c:3687-3696

## Simplified Source

```c
Datum mul_d_interval(PG_FUNCTION_ARGS) {
    // Extract arguments as generic Datums
    Datum factor = PG_GETARG_DATUM(0);  // float8
    Datum span = PG_GETARG_DATUM(1);    // Interval

    // Call interval_mul with arguments swapped
    return DirectFunctionCall2(interval_mul, span, factor);
}
```