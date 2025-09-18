# numeric_cash

## Location
src/backend/utils/adt/cash.c: 1102 - 1135

## Overview
Converts a PostgreSQL numeric data type to a cash (money) data type, handling decimal precision and locale-specific formatting.

## Definition
```c
Datum numeric_cash(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_cash` function converts a PostgreSQL numeric value to the cash data type. It handles the conversion by:

1. Determining the appropriate decimal precision (fractional digits) from the current locale
2. Computing a scale factor based on the fractional digits (10^frac_digits)
3. Multiplying the input numeric value by the scale factor to convert to the integer representation used by cash
4. Converting the scaled result to an int64 and returning it as a Cash value

The function uses the locale-specific `frac_digits` setting to determine how many decimal places the cash value should have, defaulting to 2 if the locale setting is invalid (< 0 or > 10).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the numeric input value

## Dependencies
- Functions called/Symbols referenced:
  - `[PGLC_localeconv](../P/PGLC_localeconv.md)`: Get locale conversion information
  - `[int64_to_numeric](../i/int64_to_numeric.md)`: Convert int64 to numeric type
  - `[NumericGetDatum](../N/NumericGetDatum.md)`: Convert numeric to Datum
  - `[numeric_mul](numeric_mul.md)`: Multiply two numeric values
  - `DirectFunctionCall2`: Call a 2-argument PostgreSQL function
  - `[numeric_int8](numeric_int8.md)`: Convert numeric to int8 (with rounding)
  - `DirectFunctionCall1`: Call a 1-argument PostgreSQL function
  - `[DatumGetInt64](../D/DatumGetInt64.md)`: Extract int64 from Datum
  - `PG_RETURN_CASH`: Return a Cash value
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function automatically rounds the result to the nearest integer using `numeric_int8`
- Locale settings are validated to ensure fractional digits are within reasonable bounds (0-10)
- The conversion process preserves the monetary precision defined by the locale
- Located in src/backend/utils/adt/cash.c:1102-1135