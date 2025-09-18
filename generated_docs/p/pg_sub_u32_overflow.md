# pg_sub_u32_overflow

## Location
[src/include/common/int.h:343-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L343-L358)

## Overview
A safe 32-bit unsigned integer subtraction function that detects overflow conditions and prevents undefined behavior.

## Definition
```c
static inline bool pg_sub_u32_overflow(uint32 a, uint32 b, uint32 *result)
```

## Detailed Description
This function performs subtraction of two 32-bit unsigned integers with overflow detection. It uses compiler built-ins when available (`__builtin_sub_overflow`) for optimal performance and reliability. When built-ins are not available, it implements manual overflow checking by verifying that the subtrahend (b) is not greater than the minuend (a).

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
- Part of PostgreSQL's comprehensive safe integer arithmetic API covering INT32 operations
- Designed as an inline function for optimal performance in arithmetic-heavy code paths
- The manual implementation specifically checks if b > a, which would cause underflow in unsigned arithmetic
- Uses the same overflow detection strategy as the 16-bit version but operates on 32-bit integers