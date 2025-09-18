# numeric_sub_opt_error

## Location
[src/backend/utils/adt/numeric.c:2961-3018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2961-L3018)

## Overview
Internal PostgreSQL function that performs numeric subtraction with optional error handling, providing the core implementation for numeric subtraction operations.

## Definition


## Detailed Description
The  function is the internal implementation of numeric subtraction in PostgreSQL. Unlike the public  function, this version provides optional error handling through the  parameter, allowing callers to handle arithmetic errors gracefully without throwing exceptions.

The function handles all special numeric cases including NaN (Not a Number) and infinity values according to IEEE 754-like semantics:
- Any operation involving NaN results in NaN
- Infinity minus infinity results in NaN
- Positive infinity minus any finite number results in positive infinity
- Negative infinity minus any finite number results in negative infinity
- Finite number minus positive infinity results in negative infinity
- Finite number minus negative infinity results in positive infinity

For finite numbers, the function converts the input numerics to internal NumericVar format, performs the subtraction using , and converts the result back to the external Numeric format.

## Parameters / Member Variables
- : The minuend (Numeric value to subtract from)
- : The subtrahend (Numeric value to subtract)
- : Optional pointer to boolean flag for error reporting. If provided and an error occurs, the flag is set to true and NULL is returned instead of throwing an exception

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if numeric value is NaN or infinity
  - : Checks if numeric value is NaN
  - : Checks if numeric value is positive infinity
  - : Checks if numeric value is negative infinity
  - : Creates result from constant numeric values
  - : Converts Numeric to NumericVar format
  - : Initializes NumericVar structure
  - : Performs actual subtraction on NumericVar values
  - : Creates result with optional error handling
  - : Frees NumericVar memory

- Called from (representative examples):
  - : Public numeric subtraction function
  - : JSON path execution
  - : Timestamp/timestamptz part extraction
  - Various internal numeric operations requiring error handling

## Notes and Other Information
- This function implements the core subtraction logic for PostgreSQL's NUMERIC data type
- The optional error handling makes it suitable for use in contexts where exceptions need to be avoided
- Special value handling follows mathematical conventions for infinity and NaN
- Memory management is handled through NumericVar lifecycle functions
- Location: 
- Part of PostgreSQL's internal numeric arithmetic implementation with enhanced error control