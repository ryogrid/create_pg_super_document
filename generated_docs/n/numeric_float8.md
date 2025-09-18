# numeric_float8

## Location
src/backend/utils/adt/numeric.c: 4644 - 4676

## Overview
Converts a PostgreSQL Numeric value to a float8 (double precision) type, handling special numeric values and using string-based conversion for accuracy.

## Definition
```c
Datum numeric_float8(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_float8` function converts a PostgreSQL `Numeric` value to a `float8` (double precision floating-point) type. It first handles special numeric values by mapping positive infinity, negative infinity, and NaN to their corresponding IEEE 754 floating-point representations. For finite numeric values, it uses a two-step string-based conversion process: first converting the numeric to its string representation using `numeric_out`, then parsing that string as a float8 using `float8in`. This approach ensures accurate conversion while properly handling the full range of numeric precision.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function parameter macro that provides access to function arguments
  - Argument 0: `Numeric` input value to be converted to float8

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC`: Retrieves the numeric argument from function parameters
  - `NUMERIC_IS_SPECIAL`: Checks if the numeric value is special (NaN or infinity)
  - `NUMERIC_IS_PINF`: Checks for positive infinity
  - `NUMERIC_IS_NINF`: Checks for negative infinity
  - `get_float8_infinity`: Returns the IEEE 754 positive infinity value
  - `get_float8_nan`: Returns the IEEE 754 NaN value
  - [numeric_out](numeric_out.md): Converts numeric to string representation
  - [float8in](../f/float8in.md): Parses string into float8 value
  - `DirectFunctionCall1`: Directly calls PostgreSQL functions
  - [NumericGetDatum](../N/NumericGetDatum.md)/`DatumGetCString`/`CStringGetDatum`: Type conversion utilities
  - [pfree](../p/pfree.md): Frees allocated memory
  - `PG_RETURN_DATUM`: Returns the float8 result
- Called from (representative examples):
  - [brin_minmax_multi_distance_numeric](../b/brin_minmax_multi_distance_numeric.md): BRIN index distance calculation
  - [jsonb_float8](../j/jsonb_float8.md): JSONB to float8 conversion
  - [numrange_subdiff](numrange_subdiff.md): Numeric range subdifference calculation

## Notes and Other Information
- Uses string-based conversion via `numeric_out` and `float8in` to ensure accurate decimal-to-binary conversion
- Properly handles PostgreSQL's extended numeric values (positive/negative infinity, NaN)
- Memory management includes `pfree` call to release the temporary string buffer
- Part of PostgreSQL's numeric type conversion system in `src/backend/utils/adt/numeric.c`
- The two-step conversion approach preserves precision better than direct binary conversion
- Special value handling ensures IEEE 754 compliance for floating-point results