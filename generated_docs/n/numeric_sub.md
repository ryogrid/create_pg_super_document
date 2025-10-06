# numeric_sub

## Location
[src/backend/utils/adt/numeric.c:2941-2960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2941-L2960)

## Overview
PostgreSQL function that performs subtraction of two numeric values, implementing the SQL minus operator (-) for the NUMERIC data type.

## Definition

```c
Datum
numeric_sub(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that subtracts one numeric value from another. It serves as the implementation for the SQL subtraction operator (-) when applied to NUMERIC data types. This function is a thin wrapper around , providing the standard PostgreSQL function interface for numeric subtraction operations.

The function extracts two NUMERIC arguments from the function call arguments, performs the subtraction operation by delegating to , and returns the result as a Datum. It handles all special cases including NaN and infinity values through the underlying implementation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0): The minuend (numeric value to subtract from)
  - Second argument (index 1): The subtrahend (numeric value to subtract)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts NUMERIC arguments from function call
  - : Performs the actual subtraction operation
  - : Returns the result as a PostgreSQL Datum
  - : PostgreSQL's internal numeric data type

- Called from (representative examples):
  - : BRIN index distance calculation
  - : Database size calculation utilities
  - : LSN (Log Sequence Number) operations
  - : Numeric range operations

## Notes and Other Information
- This function is registered in PostgreSQL's system catalogs and can be called directly from SQL
- Error handling is managed by the underlying  function
- The function handles all special numeric cases including NaN, positive/negative infinity
- Location: 
- Part of PostgreSQL's comprehensive numeric arithmetic implementation

## Simplified Source

```c
Datum numeric_sub(PG_FUNCTION_ARGS) {
    // Get the two numeric operands
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);

    // Perform subtraction using internal implementation
    Numeric res = numeric_sub_opt_error(num1, num2, NULL);

    // Return the result
    PG_RETURN_NUMERIC(res);
}
```