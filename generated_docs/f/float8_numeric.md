# float8_numeric

## Location
src/backend/utils/adt/numeric.c: 4609 - 4643

## Overview
Converts a PostgreSQL float8 (double precision) value to a Numeric type, handling special floating-point values like NaN and infinity.

## Definition
```c
Datum float8_numeric(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float8_numeric` function converts a PostgreSQL `float8` (double precision floating-point) value to a `Numeric` type. It handles special IEEE 754 floating-point values by mapping NaN to numeric NaN and positive/negative infinity to their corresponding numeric representations. For finite values, it converts the float8 to a string representation using `snprintf` with `DBL_DIG` precision, then parses this string into a numeric value. This approach ensures accurate conversion while preserving the precision characteristics of the original floating-point value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function parameter macro that provides access to function arguments
  - Argument 0: `float8` input value to be converted to Numeric

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT8`: Retrieves the float8 argument from function parameters
  - `isnan`: Checks if the float8 value is NaN (Not a Number)
  - `isinf`: Checks if the float8 value is infinite
  - `[make_result](../m/make_result.md)`: Creates a Numeric result from a NumericVar
  - `init_var`: Initializes a NumericVar structure
  - `[set_var_from_str](../s/set_var_from_str.md)`: Parses a string representation into a NumericVar
  - `[free_var](free_var.md)`: Frees memory allocated for a NumericVar
  - `PG_RETURN_NUMERIC`: Returns the Numeric result
- Called from (representative examples):
  - `[executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)`: JSON path execution
  - `[JsonItemFromDatum](../J/JsonItemFromDatum.md)`: JSON item conversion

## Notes and Other Information
- Uses `DBL_DIG` constant to determine appropriate precision for string conversion
- Handles IEEE 754 special values by mapping to corresponding numeric constants (`const_nan`, `const_pinf`, `const_ninf`)
- Converts through string representation to ensure accurate decimal conversion
- Buffer size is `DBL_DIG + 100` to accommodate the formatted floating-point string
- Part of PostgreSQL's numeric type conversion system in `src/backend/utils/adt/numeric.c`
- The string-based conversion approach avoids precision issues that could occur with direct binary conversion