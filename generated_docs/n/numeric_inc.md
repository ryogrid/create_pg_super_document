# numeric_inc

## Location
src/backend/utils/adt/numeric.c: 3453 - 3485

## Overview
PostgreSQL function that increments a numeric value by one, providing a simple and efficient way to add 1 to any Numeric type value.

## Definition
```c
Datum numeric_inc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements increment operation for PostgreSQL's Numeric data type by adding one to the input value. It handles special numeric values (NaN, infinity) by returning them unchanged, and performs regular addition with the constant value 1 for finite numbers. The function uses PostgreSQL's internal arithmetic functions for precise numeric computation.

Key behaviors:
- Adds exactly 1 to the input numeric value
- Preserves special values (NaN, positive/negative infinity) unchanged
- Uses high-precision arithmetic for accurate increment operation
- Returns result as Numeric type
- Uses PostgreSQL's function calling convention (PG_FUNCTION_ARGS)

## Parameters / Member Variables
- Function arguments accessed via PG_GETARG_NUMERIC():
  - Argument 0: The Numeric value to increment by one

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC, PG_RETURN_NUMERIC
  - NUMERIC_IS_SPECIAL, duplicate_numeric
  - [init_var_from_num](../i/init_var_from_num.md), free_var
  - [add_var](../a/add_var.md) (with const_one constant)
  - [make_result](../m/make_result.md)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function dispatch)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL
- Provides an optimized way to increment numeric values without explicit addition
- Special value handling ensures mathematical consistency (NaN + 1 = NaN, Inf + 1 = Inf)
- Uses the internal const_one constant for efficient addition
- Part of PostgreSQL's comprehensive numeric arithmetic function suite
- Commonly used in SQL expressions where incrementing is needed