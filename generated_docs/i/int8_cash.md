# int8_cash

## Location
[src/backend/utils/adt/cash.c:1166-1190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L1166-L1190)

## Overview
Converts a PostgreSQL int8 (64-bit integer/bigint) data type to a cash (money) data type, scaling the value according to locale-specific decimal precision.

## Definition
```c
Datum int8_cash(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int8_cash` function converts a PostgreSQL int8 (64-bit integer/bigint) value to the cash data type. The conversion process follows these steps:

1. Extracting the input int64 value from the function arguments
2. Determining the decimal precision (fractional digits) from the current locale settings
3. Computing a scale factor based on the fractional digits (10^frac_digits)
4. Multiplying the input bigint by the scale factor with overflow protection
5. Returning the scaled result as a Cash value

Similar to `int4_cash`, this function uses `int8mul` to perform the multiplication with built-in overflow detection, ensuring that large input values do not cause integer overflow during the scaling operation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the int8 input value

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT64`: Extract int64 from function arguments
  - [PGLC_localeconv](../P/PGLC_localeconv.md): Get locale conversion information
  - [int8mul](int8mul.md): Multiply two int64 values with overflow checking
  - `DirectFunctionCall2`: Call a 2-argument PostgreSQL function
  - [Int64GetDatum](../I/Int64GetDatum.md): Convert int64 to Datum
  - [DatumGetInt64](../D/DatumGetInt64.md): Extract int64 from Datum
  - `PG_RETURN_CASH`: Return a Cash value
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- Validates locale fractional digits settings within the 0-10 range, defaulting to 2 if out of bounds
- Provides overflow protection through the `int8mul` function for safe arithmetic operations
- Maintains monetary precision as defined by the current locale configuration
- Handles larger integer values than `int4_cash` due to 64-bit input capacity
- Input values are interpreted as whole currency units before scaling to internal cash representation
- Located in src/backend/utils/adt/cash.c:1166-1190