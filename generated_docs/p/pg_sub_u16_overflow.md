# pg_sub_u16_overflow

## Location
[src/include/common/int.h:288-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L288-L303)

## Overview
A safe 16-bit unsigned integer subtraction function that detects overflow conditions and prevents undefined behavior.

## Definition

```c
static inline bool
pg_sub_u16_overflow(uint16 a, uint16 b, uint16 *result)
```
## Detailed Description
This function performs subtraction of two 16-bit unsigned integers with overflow detection. It uses compiler built-ins when available (`__builtin_sub_overflow`) for optimal performance and reliability. When built-ins are not available, it implements manual overflow checking by verifying that the subtrahend (b) is not greater than the minuend (a).

The function follows PostgreSQL's safe arithmetic philosophy by providing explicit overflow detection rather than allowing silent wraparound behavior that could lead to security vulnerabilities or data corruption.

## Parameters / Member Variables
- `a`: The minuend (number being subtracted from)
- `b`: The subtrahend (number to subtract)  
- `result`: Pointer to store the subtraction result when no overflow occurs

## Dependencies
- Functions called/Symbols referenced:
  - `__builtin_sub_overflow` (compiler built-in, when available)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns `true` if overflow would occur, `false` if the operation is safe
- When overflow is detected without compiler built-ins, sets result to 0x5EED (a distinctive "seed" value) to avoid spurious compiler warnings
- Part of PostgreSQL's comprehensive safe integer arithmetic API
- Designed as an inline function for optimal performance in arithmetic-heavy code paths
- The manual implementation specifically checks if b > a, which would cause underflow in unsigned arithmetic

## Simplified Source

```c
static inline bool
pg_sub_u16_overflow(uint16 a, uint16 b, uint16 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
    // Use compiler built-in for optimal performance
    return __builtin_sub_overflow(a, b, result);
#else
    // Check for underflow: subtracting larger from smaller number
    if (b > a) {
        *result = 0x5EED;  // Dummy value to avoid warnings
        return true;  // Overflow occurred
    }

    *result = a - b;
    return false;  // No overflow
#endif
}
```