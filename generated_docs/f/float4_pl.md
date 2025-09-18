# float4_pl

## Location
src/include/utils/float.h: 146 - 157

## Overview
Performs single-precision floating-point addition with overflow detection and error reporting.

## Definition


## Detailed Description
This inline function implements safe single-precision floating-point addition by performing the arithmetic operation and checking for overflow conditions. The function adds two float4 values and validates that the result hasn't overflowed to infinity. If both input values are finite but the result is infinite, it indicates an overflow condition and triggers an error. The function is designed to catch arithmetic overflow while allowing legitimate infinite results (when at least one input is already infinite) to pass through unchanged.

## Parameters / Member Variables
- `val1`: First single-precision floating-point operand
- `val2`: Second single-precision floating-point operand

## Dependencies
- Functions called/Symbols referenced:
  - isinf (checks if value is infinite)
  - float_overflow_error (reports overflow error)
  - float4 (PostgreSQL's single-precision float type)
- Called from (representative examples):
  - float4pl (src/backend/utils/adt/float.c:726)

## Notes and Other Information
- Defined as a static inline function in src/include/utils/float.h:146-157
- Part of PostgreSQL's floating-point arithmetic with overflow/underflow error reporting
- Cannot detect underflow in addition/subtraction due to rounding near underflow values
- Uses unlikely() macro for branch prediction optimization on overflow check
- Implements IEEE 754 compliant arithmetic with PostgreSQL-specific error handling
- The function allows infinite results when at least one operand is already infinite