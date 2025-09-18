# int8gcd_internal

## Location
src/backend/utils/adt/int8.c: 606 - 666

## Overview
Internal implementation of the Greatest Common Divisor (GCD) algorithm for 64-bit signed integers using the Euclidean algorithm with special handling for edge cases.

## Definition


## Detailed Description
The int8gcd_internal function implements the mathematical greatest common divisor operation using the Euclidean algorithm. It finds the largest positive integer that exactly divides both input values. The function includes comprehensive handling for special cases including zero inputs and the problematic INT64_MIN value. It performs computations in negative space initially to handle INT64_MIN safely, then converts the final result to positive. The algorithm optimizes by ensuring the larger absolute value is processed first to reduce the number of modulo operations needed.

## Parameters / Member Variables
- : First 64-bit signed integer input
- : Second 64-bit signed integer input
- : Temporary variable for swapping values during algorithm execution
- : Negative representation of arg1's absolute value for safe INT64_MIN handling
- : Negative representation of arg2's absolute value for safe INT64_MIN handling

## Dependencies
- Functions called/Symbols referenced:
  - PG_INT64_MIN (minimum int64 constant for overflow detection)
  - ereport (error reporting for overflow cases)
  - errcode/errmsg (error code and message macros)
- Called from:
  - int8gcd (public GCD function wrapper)
  - int8lcm (least common multiple function)

## Notes and Other Information
- Implements mathematical properties: gcd(x, 0) = gcd(0, x) = abs(x), gcd(0, 0) = 0
- Special case handling for INT64_MIN prevents overflow when computing absolute values
- Uses negative space computation to safely handle INT64_MIN without overflow
- Optimizes by putting larger absolute value in arg1 first to reduce iterations
- Guards against floating-point exceptions on some hardware for INT64_MIN % -1
- Returns positive result always (except for gcd(0,0) = 0)
- Core algorithm for both GCD and LCM operations in PostgreSQL bigint arithmetic