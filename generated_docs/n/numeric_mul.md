# numeric_mul

## Location
[src/backend/utils/adt/numeric.c:3019-3038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3019-L3038)

## Overview
PostgreSQL function that performs multiplication of two numeric values, implementing the SQL multiplication operator (*) for the NUMERIC data type.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that multiplies two numeric values together. It serves as the implementation for the SQL multiplication operator (*) when applied to NUMERIC data types. This function is a thin wrapper around , providing the standard PostgreSQL function interface for numeric multiplication operations.

The function extracts two NUMERIC arguments from the function call arguments, performs the multiplication operation by delegating to , and returns the result as a Datum. It handles all special cases including NaN and infinity values through the underlying implementation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0): The first multiplicand (numeric value to multiply)
  - Second argument (index 1): The second multiplicand (numeric value to multiply)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts NUMERIC arguments from function call
  - : Performs the actual multiplication operation
  - : Returns the result as a PostgreSQL Datum
  - : PostgreSQL's internal numeric data type

- Called from (representative examples):
  - : Cash type conversion and calculations
  - : Database size calculation utilities
  - : Numeric formatting operations
  - : Numeric to string conversion

## Notes and Other Information
- This function is registered in PostgreSQL's system catalogs and can be called directly from SQL
- Error handling is managed by the underlying  function
- The function handles all special numeric cases including NaN, positive/negative infinity
- Location: 
- Part of PostgreSQL's comprehensive numeric arithmetic implementation
- Used extensively in financial calculations, size computations, and numeric formatting operations