# float4_div

## Location
[src/include/utils/float.h:222-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L222-L237)

## Overview
Performs division of two single-precision floating-point numbers (float4) with comprehensive error checking for division by zero, overflow, and underflow conditions.

## Definition

```c
static inline float4
float4_div(const float4 val1, const float4 val2)
```
## Detailed Description
The  function divides two  (single-precision floating-point) values and returns the result with comprehensive error handling. This is an inline function defined in the header file for performance optimization. Unlike multiplication, division requires additional error checking for division by zero, which is a common source of mathematical errors in database operations.

The function performs the division operation but includes checks for division by zero, overflow (when the result becomes infinite), and underflow (when the result becomes zero despite having a non-zero dividend). The function uses the  macro to optimize branch prediction for error conditions.

## Parameters / Member Variables
- `val1`: The dividend (numerator) - the floating-point value to be divided
- `val2`: The divisor (denominator) - the floating-point value to divide by
## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function to check for NaN (Not a Number)
  - : Standard C library function to check for infinity
  - : PostgreSQL error handler for division by zero
  - : PostgreSQL error handler for floating-point overflow
  - : PostgreSQL error handler for floating-point underflow
  - : Type alias for single-precision floating-point (float)
- Called from (representative examples):
  - : Main SQL-callable division function in src/backend/utils/adt/float.c:753
  - : GiST index splitting algorithm in src/backend/access/gist/gistproc.c:382

## Notes and Other Information
- This is an inline function for performance, defined in src/include/utils/float.h:222-237
- The function first checks for division by zero (val2 == 0.0f) but only raises an error if val1 is not NaN
- Division by zero with NaN as dividend is allowed and follows IEEE 754 standards
- Overflow is detected when the result is infinite but the dividend was not infinite
- Underflow is detected when the result is zero but the dividend is non-zero and divisor is not infinite
- The  macro is used to hint to the compiler that error conditions are rare
- This function is part of PostgreSQL's type system implementation for the  SQL data type (float4)
- The function follows PostgreSQL's convention of throwing errors rather than returning special values for exceptional conditions
- More complex error handling than multiplication due to the additional division-by-zero case

## Simplified Source

```c
static inline float4 float4_div(const float4 val1, const float4 val2) {
    float4 result;

    // Check for division by zero (but allow NaN/0 per IEEE 754)
    if (unlikely(val2 == 0.0f) && !isnan(val1))
        float_zero_divide_error();

    // Perform division
    result = val1 / val2;

    // Check for overflow: result is infinite but dividend was not infinite
    if (unlikely(isinf(result)) && !isinf(val1))
        float_overflow_error();

    // Check for underflow: result is zero but dividend is non-zero and divisor is not infinite
    if (unlikely(result == 0.0f) && val1 != 0.0f && !isinf(val2))
        float_underflow_error();

    return result;
}
```