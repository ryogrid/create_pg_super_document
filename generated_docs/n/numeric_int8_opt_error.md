# numeric_int8_opt_error

## Location
[src/backend/utils/adt/numeric.c:4501-4550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4501-L4550)

## Overview
Converts a PostgreSQL numeric value to a 64-bit signed integer with optional error handling instead of throwing exceptions.

## Definition
```c
int64 numeric_int8_opt_error(Numeric num, bool *have_error)
```

## Detailed Description
This function provides a safe conversion from PostgreSQL's numeric data type to a 64-bit signed integer (int64/bigint) with flexible error handling. Unlike the standard conversion functions that throw errors on failure, this function can optionally return error status through a boolean parameter. It handles special numeric values (NaN and infinity) and range overflow conditions. When have_error is provided, conversion failures set the error flag and return 0; when have_error is NULL, the function throws standard PostgreSQL errors. The conversion process first converts the numeric to NumericVar format, then uses numericvar_to_int64 for the actual conversion.

## Parameters / Member Variables
- `num`: Input Numeric value to be converted
- `have_error`: Optional pointer to boolean flag; if provided, conversion errors set this to true instead of throwing exceptions

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL (macro to check for special values)
  - NUMERIC_IS_NAN (macro to check for NaN)
  - [init_var_from_num](../i/init_var_from_num.md) (converts Numeric to NumericVar)
  - [numericvar_to_int64](numericvar_to_int64.md) (performs the actual conversion)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (in JSON path execution)
  - [numeric_int8](numeric_int8.md) (standard int8 conversion function)
  - PG_RETURN_NUMERIC (in numeric utilities)

## Notes and Other Information
- Provides two error handling modes: exception-based (when have_error is NULL) or flag-based (when have_error is provided)
- Special values (NaN, infinity) always result in errors as they cannot be represented as integers
- [Range](../R/Range.md) overflow occurs when the numeric value exceeds the bounds of int64 (-2^63 to 2^63-1)
- Returns 0 on error when using flag-based error handling
- Part of PostgreSQL's type conversion system and used extensively in JSON path operations
- Error codes used: ERRCODE_FEATURE_NOT_SUPPORTED (for special values), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE (for overflow)