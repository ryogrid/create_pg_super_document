# numeric_int2

## Location
src/backend/utils/adt/numeric.c: 4569 - 4608

## Overview
Converts a PostgreSQL numeric value to a 16-bit signed integer (smallint), performing range validation and error handling for special numeric values.

## Definition


## Detailed Description
The `numeric_int2` function converts a PostgreSQL `Numeric` type to a 16-bit signed integer (`int16`). It handles the conversion by first checking for special numeric values (NaN and infinity), then converting to an intermediate 64-bit integer representation, and finally performing range validation to ensure the result fits within the smallint range (-32768 to 32767). The function follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function parameter macro that provides access to function arguments
  - Argument 0: `Numeric` input value to be converted to smallint

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC`: Retrieves the numeric argument from function parameters
  - `NUMERIC_IS_SPECIAL`: Checks if the numeric value is special (NaN or infinity)
  - `NUMERIC_IS_NAN`: Checks specifically for NaN values
  - `[init_var_from_num](../i/init_var_from_num.md)`: Initializes a NumericVar from a Numeric value
  - `[numericvar_to_int64](numericvar_to_int64.md)`: Converts NumericVar to 64-bit integer
  - `PG_RETURN_INT16`: Returns the 16-bit integer result
- Called from (representative examples):
  - `[jsonb_int2](../j/jsonb_int2.md)`: JSONB to smallint conversion

## Notes and Other Information
- Throws `ERRCODE_FEATURE_NOT_SUPPORTED` error for NaN and infinity inputs
- Throws `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE` error when the numeric value exceeds smallint range
- Uses intermediate 64-bit integer conversion to ensure precision during range checking
- Range validation uses `PG_INT16_MIN` and `PG_INT16_MAX` constants for boundary checking
- Part of PostgreSQL's numeric type conversion system in `src/backend/utils/adt/numeric.c`