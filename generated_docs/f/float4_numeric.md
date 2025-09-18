# float4_numeric

## Location
[src/backend/utils/adt/numeric.c:4703-4737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4703-L4737)

## Overview
Converts a PostgreSQL float4 (single precision) value to a Numeric type, handling special floating-point values like NaN and infinity.

## Definition
```c
Datum float4_numeric(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float4_numeric` function converts a PostgreSQL `float4` (single precision floating-point) value to a `Numeric` type. It follows the same conversion strategy as `float8_numeric` but operates on single precision values. The function handles special IEEE 754 floating-point values by mapping NaN to numeric NaN and positive/negative infinity to their corresponding numeric representations. For finite values, it converts the float4 to a string representation using `snprintf` with `FLT_DIG` precision, then parses this string into a numeric value. This approach ensures accurate conversion while preserving the precision characteristics appropriate for single precision floating-point values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function parameter macro that provides access to function arguments
  - Argument 0: `float4` input value to be converted to Numeric

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT4`: Retrieves the float4 argument from function parameters
  - `isnan`: Checks if the float4 value is NaN (Not a Number)
  - `isinf`: Checks if the float4 value is infinite
  - [make_result](../m/make_result.md): Creates a Numeric result from a NumericVar
  - `init_var`: Initializes a NumericVar structure
  - [set_var_from_str](../s/set_var_from_str.md): Parses a string representation into a NumericVar
  - [free_var](free_var.md): Frees memory allocated for a NumericVar
  - `PG_RETURN_NUMERIC`: Returns the Numeric result
- Called from (representative examples):
  - [JsonItemFromDatum](../J/JsonItemFromDatum.md): JSON item conversion from various data types

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_FLOAT4`: Retrieves the float4 argument from function parameters
  - `isnan`: Checks if the float4 value is NaN (Not a Number)
  - `isinf`: Checks if the float4 value is infinite
  - [make_result](../m/make_result.md): Creates a Numeric result from a NumericVar
  - `init_var`: Initializes a NumericVar structure
  - [set_var_from_str](../s/set_var_from_str.md): Parses a string representation into a NumericVar
  - [free_var](free_var.md): Frees memory allocated for a NumericVar
  - `PG_RETURN_NUMERIC`: Returns the Numeric result
- Called from (representative examples):
  - [JsonItemFromDatum](../J/JsonItemFromDatum.md): JSON item conversion from various data types

## Notes and Other Information
- Uses `FLT_DIG` constant to determine appropriate precision for single precision float string conversion
- Handles IEEE 754 special values by mapping to corresponding numeric constants (`const_nan`, `const_pinf`, `const_ninf`)
- Buffer size is `FLT_DIG + 100` to accommodate the formatted single precision floating-point string
- Converts through string representation to ensure accurate decimal conversion, similar to `float8_numeric`
- Part of PostgreSQL's numeric type conversion system in `src/backend/utils/adt/numeric.c`
- The string-based conversion approach avoids precision issues that could occur with direct binary conversion
- Precision is limited by the single precision format (typically 6-7 significant decimal digits)