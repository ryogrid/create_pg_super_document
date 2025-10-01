# numeric_is_inf

## Location
[src/backend/utils/adt/numeric.c:860-870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L860-L870)

## Overview
A utility function that tests whether a Numeric value represents positive or negative infinity.

## Definition
```c
bool numeric_is_inf(Numeric num)
```

## Detailed Description
The `numeric_is_inf` function provides a simple boolean test to determine if a Numeric value is infinity (either positive or negative). This is a lightweight wrapper around the `NUMERIC_IS_INF` macro that offers a function interface for checking infinity status.

Infinity values in PostgreSQL's numeric type represent mathematical concepts of positive and negative infinity, typically resulting from operations like division by zero (1/0 = +∞, -1/0 = -∞) or overflow conditions. This function is essential for:

1. **Mathematical validation**: Ensuring finite numeric values before processing
2. **JSON operations**: Proper handling of infinity values in JSON contexts where they may need special treatment
3. **Arithmetic operations**: Detecting overflow or division-by-zero results
4. **Range checking**: Validating that values fall within acceptable finite ranges

## Parameters / Member Variables
- `num`: The Numeric value to test for infinity status

## Dependencies
- Functions called/Symbols referenced:
  - `NUMERIC_IS_INF`: Macro that performs the actual infinity check on the Numeric structure
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md): JSON path execution with infinity handling
  - Various numeric utility functions via `PG_RETURN_NUMERIC` header

## Notes and Other Information
- Simple wrapper function providing a clean API for infinity detection
- Returns `true` if the value is either positive or negative infinity, `false` otherwise
- Does not distinguish between positive and negative infinity - use `NUMERIC_IS_PINF` and `NUMERIC_IS_NINF` macros for that
- Used primarily in JSON operations where infinity values need special handling
- Part of PostgreSQL's comprehensive support for IEEE 754-style special numeric values
- The function is inline-friendly and very lightweight, essentially just a macro call

## Simplified Source

```c
bool
numeric_is_inf(Numeric num)
{
    return NUMERIC_IS_INF(num);
}
```