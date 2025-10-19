# float4_mi

## Location
[src/include/utils/float.h:170-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/float.h#L170-L181)

## Overview
Performs single-precision floating-point subtraction with overflow detection and error reporting.

## Definition
```c
static inline float4 float4_mi(const float4 val1, const float4 val2)
```

## Detailed Description
This inline function implements safe single-precision floating-point subtraction by performing the arithmetic operation and checking for overflow conditions. The function subtracts val2 from val1 and validates that the result hasn't overflowed to infinity. If both input values are finite but the result is infinite, it indicates an overflow condition and triggers an error. The function is designed to catch arithmetic overflow while allowing legitimate infinite results (when at least one input is already infinite) to pass through unchanged.

## Parameters / Member Variables
- `val1`: First single-precision floating-point operand (minuend)
- `val2`: Second single-precision floating-point operand (subtrahend)

## Dependencies
- Functions called/Symbols referenced:
  - isinf (checks if value is infinite)
  - [float_overflow_error](float_overflow_error.md) (reports overflow error)
  - float4 (PostgreSQL's single-precision float type)
- Called from (representative examples):
  - [float4mi](float4mi.md) (src/backend/utils/adt/float.c:735)

## Notes and Other Information
- Defined as a static inline function in src/include/utils/float.h:170-181
- Part of PostgreSQL's floating-point arithmetic with overflow/underflow error reporting
- Cannot detect underflow in addition/subtraction due to rounding near underflow values
- Uses unlikely() macro for branch prediction optimization on overflow check
- Implements IEEE 754 compliant arithmetic with PostgreSQL-specific error handling
- The function allows infinite results when at least one operand is already infinite
- Subtraction operation: result = val1 - val2

## Simplified Source

```c
static inline float4 float4_mi(const float4 val1, const float4 val2) {
    float4 result;

    // Perform subtraction
    result = val1 - val2;

    // Check for overflow: result is infinite but neither input was infinite
    if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
        float_overflow_error();

    return result;
}
```