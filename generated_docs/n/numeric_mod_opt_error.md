# numeric_mod_opt_error

## Location
src/backend/utils/adt/numeric.c: 3384 - 3452

## Overview
Internal version of numeric modulo operation that provides optional error handling, allowing callers to handle modulo errors (like division by zero) without raising exceptions.

## Definition
```c
Numeric numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error)
```

## Detailed Description
This function performs modulo (remainder) operation on two PostgreSQL Numeric values with optional error handling. Unlike the standard `numeric_mod()` function, this variant allows the caller to handle errors gracefully by setting a flag instead of throwing exceptions. The function implements modulo semantics similar to POSIX fmod() with PostgreSQL-specific handling for special numeric values.

Key behaviors:
- Returns NULL and sets `*have_error = true` when division by zero occurs (if error handling is enabled)
- Handles special cases: NaN propagation, infinity modulo rules following POSIX fmod()
- Returns NaN for infinity % finite_nonzero cases
- Returns original dividend when divisor is infinity
- Uses mod_var for the core modulo computation

## Parameters / Member Variables
- `num1`: The dividend (numerator) - the Numeric value to find remainder for
- `num2`: The divisor (denominator) - the Numeric value to divide by for remainder
- `have_error`: Optional pointer to bool flag; if provided, set to true on error instead of throwing exception

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN, NUMERIC_IS_INF
  - make_result, make_result_opt_error
  - numeric_sign_internal, duplicate_numeric
  - init_var_from_num, init_var, free_var
  - mod_var (core modulo implementation)
- Called from (representative examples):
  - numeric_mod
  - executeItemOptUnwrapTarget (JSON path execution)

## Notes and Other Information
- This is the core implementation function for numeric modulo operations in PostgreSQL
- Follows POSIX fmod() semantics for special value handling with PostgreSQL adaptations
- Differs from POSIX by only raising division by zero error for y-is-zero, not x-is-infinite
- Error handling mechanism allows for more robust numeric computations in complex expressions
- Used internally by higher-level modulo functions and JSON path operations