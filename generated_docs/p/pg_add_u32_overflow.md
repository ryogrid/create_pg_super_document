# pg_add_u32_overflow

## Location
[src/include/common/int.h:325-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L325-L342)

## Overview
A safe 32-bit unsigned integer addition function that detects overflow conditions and prevents undefined behavior.

## Definition
```c
static inline bool pg_add_u32_overflow(uint32 a, uint32 b, uint32 *result)
```

## Detailed Description
This function performs addition of two 32-bit unsigned integers with overflow detection. It uses compiler built-ins when available (`__builtin_add_overflow`) for optimal performance and reliability. When built-ins are not available, it implements manual overflow checking using the property that in unsigned arithmetic, overflow occurs when the result is smaller than either operand.

The function is part of PostgreSQL's safe arithmetic API, providing explicit overflow detection to prevent security vulnerabilities and data corruption that could result from silent integer wraparound.

## Parameters / Member Variables
- `a`: First addend
- `b`: Second addend
- `result`: Pointer to store the addition result when no overflow occurs

## Dependencies
- Functions called/Symbols referenced:
  - `__builtin_add_overflow` (compiler built-in, when available)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns `true` if overflow would occur, `false` if the operation is safe
- The manual implementation detects overflow by checking if the result is less than the first operand (res < a)
- When overflow is detected without compiler built-ins, sets result to 0x5EED to avoid spurious compiler warnings
- Part of PostgreSQL's comprehensive safe integer arithmetic API covering INT32 operations
- Designed as an inline function for optimal performance in arithmetic-heavy code paths
- The overflow detection logic relies on the mathematical property that a + b >= a when no overflow occurs in unsigned arithmetic

## Simplified Source

```c
static inline bool
pg_add_u32_overflow(uint32 a, uint32 b, uint32 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
    // Use compiler built-in for optimal performance
    return __builtin_add_overflow(a, b, result);
#else
    // Manual overflow detection for unsigned integers
    uint32 res = a + b;

    // Check for wraparound overflow (result smaller than operands)
    if (res < a) {
        *result = 0x5EED;  // Dummy value to avoid warnings
        return true;  // Overflow occurred
    }

    *result = res;
    return false;  // No overflow
#endif
}
```