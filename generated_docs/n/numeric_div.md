# numeric_div

## Location
[src/backend/utils/adt/numeric.c:3140-3159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3140-L3159)

## Overview
PostgreSQL function that performs division of two numeric values, implementing the SQL division operator (/) for the NUMERIC data type.

## Definition

```c
Datum
numeric_div(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that divides one numeric value by another. It serves as the implementation for the SQL division operator (/) when applied to NUMERIC data types. This function is a thin wrapper around , providing the standard PostgreSQL function interface for numeric division operations.

The function extracts two NUMERIC arguments from the function call arguments, performs the division operation by delegating to , and returns the result as a Datum. It handles all special cases including NaN, infinity values, and division by zero through the underlying implementation.

Division is inherently more complex than other arithmetic operations due to precision considerations and the potential for division by zero errors.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0): The dividend (numeric value to be divided)
  - Second argument (index 1): The divisor (numeric value to divide by)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts NUMERIC arguments from function call
  - : Performs the actual division operation
  - : Returns the result as a PostgreSQL Datum
  - : PostgreSQL's internal numeric data type

- Called from (representative examples):
  - : Cash type conversion and calculations
  - : Polymorphic numeric averaging operations
  - : Standard numeric averaging aggregate function
  - : Integer averaging operations

## Notes and Other Information
- This function is registered in PostgreSQL's system catalogs and can be called directly from SQL
- Error handling including division by zero is managed by the underlying  function
- The function handles all special numeric cases including NaN, positive/negative infinity
- Division by zero raises an appropriate PostgreSQL error through the underlying implementation
- Location: 
- Part of PostgreSQL's comprehensive numeric arithmetic implementation
- Commonly used in aggregate functions, financial calculations, and statistical operations

## Simplified Source

```c
Datum
numeric_div(PG_FUNCTION_ARGS)
{
    // Extract two numeric arguments (dividend and divisor)
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);

    // Delegate to internal division function with error handling
    Numeric result = numeric_div_opt_error(num1, num2, NULL);

    // Return the division result
    PG_RETURN_NUMERIC(result);
}
```