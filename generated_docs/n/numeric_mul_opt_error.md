# numeric_mul_opt_error

## Location
[src/backend/utils/adt/numeric.c:3039-3139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3039-L3139)

## Overview
Internal PostgreSQL function that performs numeric multiplication with optional error handling, providing the core implementation for numeric multiplication operations with precise result calculation and rounding.

## Definition

```c
Numeric
numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error)
```
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

## Simplified Source

```c
Numeric
numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
    // Handle special values (NaN, infinity)
    if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2)) {
        if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
            return make_result(&const_nan);

        // Handle infinity multiplication rules
        if (NUMERIC_IS_PINF(num1)) {
            int sign = numeric_sign_internal(num2);
            return (sign == 0) ? make_result(&const_nan) :  // Inf * 0
                   (sign > 0) ? make_result(&const_pinf) :   // Inf * positive
                                make_result(&const_ninf);    // Inf * negative
        }

        // Similar logic for negative infinity and finite numbers
        // ... (other infinity cases follow same pattern)
    }

    // Convert to internal format for computation
    NumericVar arg1, arg2, result;
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);

    // Perform multiplication with exact precision
    mul_var(&arg1, &arg2, &result, arg1.dscale + arg2.dscale);

    // Round if result exceeds maximum scale
    if (result.dscale > NUMERIC_DSCALE_MAX)
        round_var(&result, NUMERIC_DSCALE_MAX);

    // Convert back to external format
    Numeric res = make_result_opt_error(&result, have_error);
    free_var(&result);

    return res;
}
```