# numeric_mul_opt_error

## Location
src/backend/utils/adt/numeric.c: 3039 - 3139

## Overview
Internal PostgreSQL function that performs numeric multiplication with optional error handling, providing the core implementation for numeric multiplication operations with precise result calculation and rounding.

## Definition


## Detailed Description
The  function is the internal implementation of numeric multiplication in PostgreSQL. Unlike the public  function, this version provides optional error handling through the  parameter, allowing callers to handle arithmetic errors gracefully without throwing exceptions.

The function handles all special numeric cases including NaN and infinity values with careful attention to mathematical rules:
- Any operation involving NaN results in NaN
- Zero multiplied by infinity (positive or negative) results in NaN
- Infinity multiplied by positive numbers results in infinity with the same sign
- Infinity multiplied by negative numbers results in infinity with opposite sign

For finite numbers, the function performs exact multiplication by:
1. Converting inputs to internal NumericVar format
2. Computing the exact product using  with precise decimal scale calculation
3. Applying rounding only if the result exceeds maximum decimal scale
4. Converting back to external Numeric format

The multiplication preserves maximum precision by setting the result scale to the sum of input scales, ensuring mathematical accuracy before any necessary rounding.

## Parameters / Member Variables
- : The first multiplicand (Numeric value)
- : The second multiplicand (Numeric value)  
- : Optional pointer to boolean flag for error reporting. If provided and an error occurs, the flag is set to true and NULL is returned instead of throwing an exception

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if numeric value is NaN or infinity
  - : Checks if numeric value is NaN
  - : Checks if numeric value is positive infinity
  - : Checks if numeric value is negative infinity
  - : Determines sign of numeric value (0, 1, -1)
  - : Creates result from constant numeric values
  - : Converts Numeric to NumericVar format
  - : Initializes NumericVar structure
  - : Performs actual multiplication on NumericVar values with specified scale
  - : Rounds NumericVar to specified decimal places
  - : Creates result with optional error handling
  - : Frees NumericVar memory
  - : Maximum decimal scale constant

- Called from (representative examples):
  - : Public numeric multiplication function
  - : JSON path execution operations
  - Various internal numeric operations requiring error handling

## Notes and Other Information
- This function implements exact multiplication semantics, computing the full precision result before rounding
- The result scale is set to the sum of input scales (arg1.dscale + arg2.dscale) to preserve maximum precision
- Rounding only occurs if the result exceeds NUMERIC_DSCALE_MAX decimal places
- Special value handling follows IEEE 754-like semantics for infinity and NaN operations
- The function carefully handles zero-times-infinity cases which result in NaN
- Location: 
- Part of PostgreSQL's internal numeric arithmetic implementation with enhanced precision control and error handling