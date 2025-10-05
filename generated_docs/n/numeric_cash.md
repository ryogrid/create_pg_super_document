# numeric_cash

## Location
[src/backend/utils/adt/cash.c:1102-1135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L1102-L1135)

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
  - [PGLC_localeconv](../P/PGLC_localeconv.md): Get locale conversion information
  - [int64_to_numeric](../i/int64_to_numeric.md): Convert int64 to numeric type
  - [NumericGetDatum](../N/NumericGetDatum.md): Convert numeric to Datum
  - [numeric_mul](numeric_mul.md): Multiply two numeric values
  - `DirectFunctionCall2`: Call a 2-argument PostgreSQL function
  - [numeric_int8](numeric_int8.md): Convert numeric to int8 (with rounding)
  - `DirectFunctionCall1`: Call a 1-argument PostgreSQL function
  - [DatumGetInt64](../D/DatumGetInt64.md): Extract int64 from Datum
  - `PG_RETURN_CASH`: Return a Cash value
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function automatically rounds the result to the nearest integer using `numeric_int8`
- Locale settings are validated to ensure fractional digits are within reasonable bounds (0-10)
- The conversion process preserves the monetary precision defined by the locale
- Located in src/backend/utils/adt/cash.c:1102-1135

## Simplified Source

```c
Datum numeric_cash(PG_FUNCTION_ARGS) {
    Datum amount = PG_GETARG_DATUM(0);
    Cash result;
    int fpoint;
    int64 scale;
    int i;
    Datum numeric_scale;
    struct lconv *lconvert = PGLC_localeconv();

    // Get fractional digits from locale, default to 2 if invalid
    fpoint = lconvert->frac_digits;
    if (fpoint < 0 || fpoint > 10) {
        fpoint = 2;
    }

    // Compute scale factor (10^fpoint)
    scale = 1;
    for (i = 0; i < fpoint; i++) {
        scale *= 10;
    }

    // Multiply input by scale factor to convert to cash internal format
    numeric_scale = NumericGetDatum(int64_to_numeric(scale));
    amount = DirectFunctionCall2(numeric_mul, amount, numeric_scale);

    // Convert to int64 (with rounding) and return as cash
    result = DatumGetInt64(DirectFunctionCall1(numeric_int8, amount));

    PG_RETURN_CASH(result);
}
```