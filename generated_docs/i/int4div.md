# int4div

## Location
[src/backend/utils/adt/int.c:833-871](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L833-L871)

## Overview
Performs division of two 32-bit integers with comprehensive error checking for division by zero and overflow conditions, implementing the PostgreSQL SQL function for integer division.

## Definition
```c
Datum int4div(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4div` function implements the division operation for PostgreSQL 32-bit integers (int4 type). It performs integer division with careful handling of edge cases including division by zero and the special case of dividing the minimum integer value by -1, which would cause overflow in twos complement arithmetic. The function includes detailed error checking and special case handling to ensure mathematical correctness and prevent undefined behavior.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call context containing:
  - `arg1` (int32): The dividend - the number to be divided
  - `arg2` (int32): The divisor - the number by which to divide

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Macro to extract 32-bit integer arguments from function context
  - `PG_INT32_MIN`: Constant representing the minimum value for 32-bit signed integers
  - `ereport`: PostgreSQL error reporting mechanism
  - `PG_RETURN_INT32`: Macro to return 32-bit integer result
  - `PG_RETURN_NULL`: Macro to return NULL result (used after division by zero error)
- Called from (representative examples):
  - This function is typically invoked through PostgreSQLs SQL function dispatch system when the division operator (/) is used with integer types

## Notes and Other Information
- Part of PostgreSQLs integer arithmetic operations located in `src/backend/utils/adt/int.c:833-871`
- Includes explicit division by zero checking with appropriate SQLSTATE error code (ERRCODE_DIVISION_BY_ZERO)
- Handles the special case of `INT_MIN / -1` which cannot be represented in twos complement arithmetic, raising a numeric value out of range error
- Recognizes that division by -1 is equivalent to negation for optimization and clarity
- Unlike other arithmetic operations, division generally cannot overflow except for the specific `INT_MIN / -1` case
- The function includes a compiler hint comment to ensure proper optimization and avoid potential gcc bugs related to unreachable code after the division by zero error
- Demonstrates PostgreSQLs defensive programming practices for handling edge cases in arithmetic operations

## Simplified Source

```c
Datum int4div(PG_FUNCTION_ARGS) {
    // Extract arguments
    int32 arg1 = PG_GETARG_INT32(0);  // dividend
    int32 arg2 = PG_GETARG_INT32(1);  // divisor

    // Check for division by zero
    if (arg2 == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                       errmsg("division by zero")));
        PG_RETURN_NULL();
    }

    // Handle special case: INT_MIN / -1 would overflow
    if (arg2 == -1) {
        if (unlikely(arg1 == PG_INT32_MIN)) {
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                           errmsg("integer out of range")));
        }
        // Division by -1 is just negation
        PG_RETURN_INT32(-arg1);
    }

    // Perform the division
    int32 result = arg1 / arg2;
    PG_RETURN_INT32(result);
}
```