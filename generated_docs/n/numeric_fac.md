# numeric_fac

## Location
[src/backend/utils/adt/numeric.c:3640-3691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3640-L3691)

## Overview
Computes the factorial of a non-negative integer, with overflow protection and interrupt handling for large computations.

## Definition
```c
Datum numeric_fac(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_fac` function implements the mathematical factorial operation (n!) for PostgreSQL. It takes a 64-bit integer argument and computes n! = n × (n-1) × (n-2) × ... × 2 × 1. The function includes comprehensive error handling for negative inputs and overflow prevention by limiting input to values ≤ 32177. For efficiency, it handles the base cases (n ≤ 1) by returning 1 immediately. The computation uses a simple iterative approach with interrupt checking to allow cancellation of long-running calculations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `num` (int64): Non-negative integer value for which to compute factorial

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT64`: Extracts 64-bit integer argument from function call context
  - `ereport`: Reports errors for negative inputs and overflow conditions
  - [make_result](../m/make_result.md): Converts NumericVar to Numeric result (for base case and final result)
  - `init_var`: Initializes NumericVar structures for computation
  - [int64_to_numericvar](../i/int64_to_numericvar.md): Converts int64 values to NumericVar for arithmetic
  - [mul_var](../m/mul_var.md): Performs multiplication in the factorial loop
  - `CHECK_FOR_INTERRUPTS`: Allows query cancellation during long computations
  - [free_var](../f/free_var.md): Releases memory allocated for NumericVar structures
  - `PG_RETURN_NUMERIC`: Returns numeric result to caller
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- Input validation: rejects negative numbers with "factorial of a negative number is undefined"
- Overflow protection: limits input to n ≤ 32177 to prevent numeric format overflow
- Base case optimization: returns 1 immediately for n ≤ 1 (mathematically correct: 0! = 1! = 1)
- Interruptible computation: includes CHECK_FOR_INTERRUPTS() in the multiplication loop
- Uses iterative rather than recursive approach to avoid stack overflow
- Part of the PostgreSQL numeric type system in src/backend/utils/adt/numeric.c
- Memory management includes proper cleanup of temporary NumericVar structures
- The 32177 limit ensures the result fits within PostgreSQL numeric format constraints