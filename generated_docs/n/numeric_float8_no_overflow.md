# numeric_float8_no_overflow

## Location
[src/backend/utils/adt/numeric.c:4677-4702](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4677-L4702)

## Overview
Internal helper function that converts a Numeric value to float8 with overflow protection, returning HUGE_VAL instead of throwing errors for out-of-range values.

## Definition
```c
Datum numeric_float8_no_overflow(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_float8_no_overflow` function is an internal helper that converts a PostgreSQL `Numeric` value to a `float8` (double precision) type with special overflow handling. Unlike the standard `numeric_float8` function, this variant does not throw errors for out-of-range values. Instead, it returns `HUGE_VAL` (positive or negative) when the numeric value exceeds the representable range of double precision floating-point numbers. For special numeric values, it maps positive infinity to `HUGE_VAL`, negative infinity to `-HUGE_VAL`, and NaN to the appropriate floating-point NaN. This function is designed for internal use where graceful overflow handling is preferred over error reporting.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function parameter macro that provides access to function arguments
  - Argument 0: `Numeric` input value to be converted to float8

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC`: Retrieves the numeric argument from function parameters
  - `NUMERIC_IS_SPECIAL`: Checks if the numeric value is special (NaN or infinity)
  - `NUMERIC_IS_PINF`: Checks for positive infinity
  - `NUMERIC_IS_NINF`: Checks for negative infinity
  - `[get_float8_nan](../g/get_float8_nan.md)`: Returns the IEEE 754 NaN value
  - [init_var_from_num](../i/init_var_from_num.md): Initializes a NumericVar from a Numeric value
  - [numericvar_to_double_no_overflow](numericvar_to_double_no_overflow.md): Converts NumericVar to double with overflow protection
  - `PG_RETURN_FLOAT8`: Returns the float8 result
- Called from (representative examples):
  - `[convert_numeric_to_scalar](../c/convert_numeric_to_scalar.md)`: Selectivity estimation functions
  - `TextDatumGetCString`: Text conversion utilities

## Notes and Other Information
- Internal helper function not directly callable from SQL
- Uses `HUGE_VAL` constant for overflow representation instead of throwing errors
- Provides graceful degradation for numeric values that exceed float8 range
- Uses direct numeric variable conversion via `numericvar_to_double_no_overflow` for finite values
- Part of PostgreSQL's numeric type conversion system in `src/backend/utils/adt/numeric.c`
- Primarily used in contexts where statistical calculations need to continue despite overflow conditions
- The no-overflow behavior makes it suitable for selectivity estimation and other statistical functions