# numeric_abs

## Location
[src/backend/utils/adt/numeric.c:1391-1417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1391-L1417)

## Overview
The  function computes the absolute value of a NUMERIC data type, removing the sign to always return a non-negative result.

## Definition

```c
Datum
numeric_abs(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the absolute value operation for PostgreSQL's NUMERIC type. It efficiently computes the absolute value by directly manipulating the sign bits in the packed numeric format, avoiding the overhead of unpacking and repacking the numeric value. The function handles all numeric representations including short form, long form, and special values (NaN and infinity). For negative infinity, it converts it to positive infinity, while NaN remains unchanged.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The input NUMERIC value to compute absolute value for (PG_GETARG_NUMERIC(0))
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extracts numeric argument from function call
  - [duplicate_numeric](../d/duplicate_numeric.md): Creates a copy of the numeric value
  - NUMERIC_IS_SHORT: Checks if numeric uses short representation
  - NUMERIC_SHORT_SIGN_MASK: Mask for sign bit in short format
  - NUMERIC_IS_SPECIAL: Checks if numeric is NaN or infinity
  - NUMERIC_INF_SIGN_MASK: Mask for infinity sign bit
  - NUMERIC_POS: Constant for positive sign
  - NUMERIC_DSCALE: Extracts display scale from numeric
  - PG_RETURN_NUMERIC: Returns numeric result

- Called from (representative examples):
  - [numeric_absolute](numeric_absolute.md): Database size calculation function
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md): JSON path execution context

## Notes and Other Information
- Implements direct bit manipulation for optimal performance
- Handles both short and long numeric format representations
- Special case handling for infinity values: -Inf becomes +Inf
- NaN values remain unchanged (absolute value of NaN is NaN)
- Does not require unpacking the numeric to NumericVar format for efficiency
- Part of the mathematical functions section in numeric.c
- Located in src/backend/utils/adt/numeric.c:1391-1417

## Simplified Source

```c
Datum
numeric_abs(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric result;

    // Create a copy of the input numeric
    result = duplicate_numeric(num);

    // Remove the sign bit based on numeric format
    if (NUMERIC_IS_SHORT(num)) {
        // For short format: clear the sign bit
        result->choice.n_short.n_header =
            num->choice.n_short.n_header & ~NUMERIC_SHORT_SIGN_MASK;
    }
    else if (NUMERIC_IS_SPECIAL(num)) {
        // For special values (NaN/Inf): clear infinity sign bit
        // This converts -Inf to +Inf, NaN remains unchanged
        result->choice.n_short.n_header =
            num->choice.n_short.n_header & ~NUMERIC_INF_SIGN_MASK;
    }
    else {
        // For long format: set positive sign while preserving scale
        result->choice.n_long.n_sign_dscale = NUMERIC_POS | NUMERIC_DSCALE(num);
    }

    PG_RETURN_NUMERIC(result);
}
```