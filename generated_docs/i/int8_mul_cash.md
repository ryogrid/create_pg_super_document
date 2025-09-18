# int8_mul_cash

## Location
[src/backend/utils/adt/cash.c:826-837](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L826-L837)

## Overview
A PostgreSQL function that performs multiplication of a 64-bit integer (int8) by a Cash value, returning the result as a Cash type.

## Definition
```c
Datum int8_mul_cash(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the multiplication operation between a 64-bit integer (int8/bigint) and a PostgreSQL Cash value. It serves as a wrapper function that extracts the arguments and delegates the actual computation to the `cash_mul_int64` helper function. This function provides the commutative counterpart to `cash_mul_int8`, allowing integer multiplication to be performed regardless of operand order (int8 * money vs money * int8).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: int64 value (64-bit integer multiplier)
  - Argument 1: Cash value to be multiplied

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Extracts int64 argument from function call context
  - PG_GETARG_CASH: Extracts Cash argument from function call context
  - [cash_mul_int64](../c/cash_mul_int64.md): Performs the actual safe multiplication with overflow checking
  - PG_RETURN_CASH: Returns the computed Cash result
- Called from:
  - SQL operator implementations for bigint * money operations

## Notes and Other Information
- Located in src/backend/utils/adt/cash.c:826-837
- Part of PostgreSQL's monetary data type arithmetic operations
- Provides commutativity for integer-Cash multiplication (complements cash_mul_int8)
- Uses the same underlying cash_mul_int64 helper function as cash_mul_int8, ensuring consistent behavior
- Leverages safe arithmetic with overflow protection through the helper function
- Related to the already processed cash_mul_int64 helper function that provides safe multiplication with overflow detection