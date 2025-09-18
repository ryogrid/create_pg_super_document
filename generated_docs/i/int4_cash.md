# int4_cash

## Location
src/backend/utils/adt/cash.c: 1136 - 1165

## Overview
Converts a PostgreSQL int4 (32-bit integer) data type to a cash (money) data type, scaling the value according to locale-specific decimal precision.

## Definition
```c
Datum int4_cash(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4_cash` function converts a PostgreSQL int4 (32-bit integer) value to the cash data type. The conversion process involves:

1. Retrieving the input int32 value from the function arguments
2. Determining the decimal precision (fractional digits) from the current locale settings
3. Computing a scale factor based on the fractional digits (10^frac_digits)
4. Multiplying the input integer by the scale factor with overflow checking
5. Returning the scaled result as a Cash value

The function ensures that the multiplication does not cause integer overflow by using the `int8mul` function which includes overflow detection and error handling.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the int4 input value

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Extract int32 from function arguments
  - `[PGLC_localeconv](../P/PGLC_localeconv.md)`: Get locale conversion information
  - `[int8mul](int8mul.md)`: Multiply two int64 values with overflow checking
  - `DirectFunctionCall2`: Call a 2-argument PostgreSQL function
  - `[Int64GetDatum](../I/Int64GetDatum.md)`: Convert int64 to Datum
  - `[DatumGetInt64](../D/DatumGetInt64.md)`: Extract int64 from Datum
  - `PG_RETURN_CASH`: Return a Cash value
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function validates locale fractional digits settings (0-10 range) and defaults to 2 if invalid
- Overflow protection is provided through the use of `int8mul` function
- The scaling preserves the monetary precision defined by the current locale
- Input values are treated as whole currency units before scaling to the internal representation
- Located in src/backend/utils/adt/cash.c:1136-1165