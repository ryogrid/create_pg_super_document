# float_overflow_error

## Location
[src/backend/utils/adt/float.c:79-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L79-L86)

## Overview
A utility function that reports floating-point overflow errors using PostgreSQL's error reporting mechanism.

## Definition

```c
pg_noinline void
float_overflow_error(void)
```
## Detailed Description
This function is designed as a centralized error reporting mechanism for floating-point overflow conditions. It uses the  attribute to prevent inlining, which helps reduce code bloat that would occur if the error reporting code were repeated at each call site. The function reports an ERROR level message with the specific error code  and a descriptive message "value out of range: overflow".

The design philosophy follows PostgreSQL's practice of centralizing common error reporting to maintain code size efficiency while providing consistent error messages across the codebase.

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
  - ,  - floating-point addition operations
  - ,  - floating-point multiplication operations
  - Various other mathematical and arithmetic functions

## Notes and Other Information
- The function is marked with  to prevent compiler inlining, which is a deliberate design choice to reduce code bloat
- This centralized approach means that specific error location indicators are not available, which is a trade-off for code size efficiency
- The function is part of a trio of similar error reporting functions for different floating-point exceptional conditions (overflow, underflow, zero-divide)
- Used extensively throughout PostgreSQL's floating-point arithmetic operations to provide consistent error handling