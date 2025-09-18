# numeric_uminus

## Location
[src/backend/utils/adt/numeric.c:1418-1459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1418-L1459)

## Overview
The  function implements the unary minus operation for NUMERIC data types, negating the sign of the input value.

## Definition


## Detailed Description
This function performs sign negation on PostgreSQL NUMERIC values by directly manipulating the sign bits in the packed format for efficiency. It handles all numeric representations including special values (NaN, positive and negative infinity) and zero values. The function implements the mathematical unary minus operator (-x) where positive values become negative and vice versa. Zero values remain zero since -0 = 0. The function optimizes by avoiding unpacking and repacking of the numeric value.

## Parameters / Member Variables
- : The input NUMERIC value to negate (PG_GETARG_NUMERIC(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extracts numeric argument from function call
  - [duplicate_numeric](../d/duplicate_numeric.md): Creates a copy of the numeric value
  - NUMERIC_IS_SPECIAL: Checks if numeric is NaN or infinity
  - NUMERIC_IS_NAN: Checks if numeric is NaN (Not a Number)
  - NUMERIC_INF_SIGN_MASK: Mask for infinity sign bit
  - NUMERIC_NDIGITS: Returns number of digits in numeric
  - NUMERIC_IS_SHORT: Checks if numeric uses short representation
  - NUMERIC_SHORT_SIGN_MASK: Mask for sign bit in short format
  - NUMERIC_SIGN: Extracts sign from numeric
  - NUMERIC_POS: Constant for positive sign
  - NUMERIC_NEG: Constant for negative sign
  - NUMERIC_DSCALE: Extracts display scale from numeric
  - PG_RETURN_NUMERIC: Returns numeric result

- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md): JSON path execution context

## Notes and Other Information
- Implements direct bit manipulation for optimal performance
- Special handling for infinity: +Inf becomes -Inf and vice versa
- NaN values remain unchanged (unary minus of NaN is NaN)
- Zero values are detected by checking if NUMERIC_NDIGITS == 0 and remain unchanged
- Handles both short and long format numeric representations
- Does not require unpacking to NumericVar format, providing better performance
- Part of the sign manipulation functions in numeric.c
- Located in src/backend/utils/adt/numeric.c:1418-1459