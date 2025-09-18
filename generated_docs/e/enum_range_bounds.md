# enum_range_bounds

## Location
src/backend/utils/adt/enum.c: 496 - 526

## Overview
A PostgreSQL built-in function that returns an array of enum values between specified lower and upper bounds (inclusive).

## Definition
```c
Datum enum_range_bounds(PG_FUNCTION_ARGS)
```

## Detailed Description
The `enum_range_bounds` function implements the two-argument variant of the `enum_range` SQL function. It accepts two enum values as bounds and returns an array containing all enum values between and including those bounds, ordered according to the enum's sort order. The function allows NULL arguments, where NULL lower bound means "start from the beginning" and NULL upper bound means "go to the end". The actual range generation logic is delegated to `enum_range_internal`.

Like other enum functions, it determines the enum type from the expression tree rather than examining the argument values directly, ensuring type safety through PostgreSQL's type system.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the two enum bound arguments

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL
  - PG_GETARG_OID
  - get_fn_expr_argtype
  - enum_range_internal
  - ereport
  - PG_RETURN_ARRAYTYPE_P
- Called from:
  - SQL queries using enum_range(lower_bound, upper_bound) function

## Notes and Other Information
- This is the two-argument variant of enum_range, complementing the single-argument `enum_range_all` function
- Supports NULL arguments for open-ended ranges
- The PostgreSQL type system ensures both arguments are of the same enum type
- Returns an array of enum values, not individual values
- Raises an error if the enum type cannot be determined from the calling context
- The actual range generation logic is implemented in the shared `enum_range_internal` function