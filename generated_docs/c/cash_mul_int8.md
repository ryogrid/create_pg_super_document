# cash_mul_int8

## Location
[src/backend/utils/adt/cash.c:813-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L813-L825)

## Overview
A PostgreSQL function that performs multiplication of a Cash value by a 64-bit integer (int8), returning the result as a Cash type.

## Definition
```c
Datum cash_mul_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the multiplication operation between a PostgreSQL Cash value and a 64-bit integer (int8/bigint). It serves as a wrapper function that extracts the arguments and delegates the actual computation to the `cash_mul_int64` helper function. This design provides a clean interface for SQL-level multiplication operations while leveraging the robust integer arithmetic implementation in the helper function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Cash value to be multiplied
  - Argument 1: int64 value (64-bit integer multiplier)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Extracts Cash argument from function call context
  - PG_GETARG_INT64: Extracts int64 argument from function call context
  - [cash_mul_int64](cash_mul_int64.md): Performs the actual safe multiplication with overflow checking
  - PG_RETURN_CASH: Returns the computed Cash result
- Called from:
  - SQL operator implementations for money * bigint operations

## Notes and Other Information
- Located in src/backend/utils/adt/cash.c:813-825
- Part of PostgreSQL's monetary data type arithmetic operations
- Leverages the cash_mul_int64 helper function which provides overflow protection and safe arithmetic
- Handles large integer multipliers efficiently through direct 64-bit integer arithmetic
- Related to the already processed cash_mul_int64 helper function that provides safe multiplication with overflow detection