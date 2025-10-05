# float_underflow_error

## Location
[src/backend/utils/adt/float.c:87-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L87-L94)

## Overview
A utility function that reports floating-point underflow errors using PostgreSQL's error reporting mechanism.

## Definition

```c
pg_noinline void
float_underflow_error(void)
```
## Detailed Description
This function serves as a centralized error reporting mechanism for floating-point underflow conditions. Similar to its overflow counterpart, it uses the  attribute to prevent compiler inlining, which helps reduce code bloat across the codebase. The function reports an ERROR level message with the error code  and the descriptive message "value out of range: underflow".

Underflow occurs when a floating-point operation results in a value that is too small to be represented in the available precision, typically approaching zero. This centralized approach ensures consistent error reporting across all PostgreSQL floating-point operations while maintaining code efficiency.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL's error reporting function)
  -  (error code specification macro)
  -  (error message specification macro)
  -  (error level constant)
  -  (specific error code)
- Called from (representative examples):
  -  - double to float conversion
  -  - double square root function
  -  - double cube root function
  -  - double power function
  -  - double exponential function
  -  - double hyperbolic cosine function
  - ,  - floating-point multiplication operations
  - ,  - floating-point division operations
  -  - hypotenuse calculation function

## Notes and Other Information
- The function is marked with  to prevent compiler inlining, reducing code bloat at the expense of specific error location information
- Part of a consistent trio of floating-point error reporting functions (overflow, underflow, zero-divide)
- Underflow detection is particularly important in scientific and financial calculations where very small values need to be handled appropriately
- The error reporting follows PostgreSQL's standard error handling conventions with appropriate error codes and messages
- Used less frequently than overflow errors but critical for mathematical operations that can produce very small results

## Simplified Source

```c
pg_noinline void float_underflow_error(void)
{
    ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
             errmsg("value out of range: underflow")));
}
```