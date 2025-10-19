# cash_div_cash

## Location
[src/backend/utils/adt/cash.c:714-733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L714-L733)

## Overview
Performs division of two PostgreSQL Cash values, returning the result as a double precision floating-point number (float8).

## Definition

```c
Datum
cash_div_cash(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum cash_div_cash(PG_FUNCTION_ARGS) {
    // Extract dividend and divisor cash values
    Cash dividend = PG_GETARG_CASH(0);
    Cash divisor = PG_GETARG_CASH(1);

    // Check for division by zero
    if (divisor == 0)
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                       errmsg("division by zero")));

    // Perform division and return as float8
    float8 quotient = (float8) dividend / (float8) divisor;
    PG_RETURN_FLOAT8(quotient);
}
```