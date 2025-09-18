# cash_div_cash

## Location
src/backend/utils/adt/cash.c: 714 - 733

## Overview
Performs division of two PostgreSQL Cash values, returning the result as a double precision floating-point number (float8).

## Definition


## Detailed Description
This function divides one Cash value by another and returns the quotient as a float8. It implements the PostgreSQL SQL operator for dividing money amounts, handling division by zero by raising an appropriate error. The function converts both Cash operands to float8 before performing the division to ensure precision in the result.

## Parameters / Member Variables
- : The Cash value to be divided (first argument)
- : The Cash value to divide by (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH
  - PG_RETURN_FLOAT8
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- Raises ERRCODE_DIVISION_BY_ZERO error when divisor is zero
- [Result](../R/Result.md) is always returned as float8, not Cash, which allows for fractional results
- Part of PostgreSQL's money data type implementation in src/backend/utils/adt/cash.c