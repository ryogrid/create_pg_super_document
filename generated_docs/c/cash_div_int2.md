# cash_div_int2

## Location
[src/backend/utils/adt/cash.c:916-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L916-L927)

## Overview
A PostgreSQL function that divides a Cash value by a 16-bit integer, providing division operations for the cash data type with int2 operands.

## Definition

```c
Datum
cash_div_int2(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the division operation between a Cash data type value and a 16-bit signed integer (int2). It serves as a PostgreSQL SQL function that can be called to perform cash/integer division operations. The function acts as a wrapper that converts the int2 parameter to int64 and delegates the actual division logic to the cash_div_int64 helper function, ensuring consistent division behavior across different integer types.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: PostgreSQL's standard function argument structure containing:
  - **c (Cash)**: The cash value to be divided (dividend)
  - **s (int16)**: The 16-bit integer divisor

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Extracts Cash argument from function call
  - PG_GETARG_INT16: Extracts int16 argument from function call  
  - [cash_div_int64](cash_div_int64.md): Performs the actual division operation
  - PG_RETURN_CASH: Returns Cash result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/cash.c:916-927
- This function is part of PostgreSQL's cash data type arithmetic operations
- Uses safe division through cash_div_int64 which likely includes division-by-zero checks
- The int2 parameter is automatically promoted to int64 for the underlying division operation