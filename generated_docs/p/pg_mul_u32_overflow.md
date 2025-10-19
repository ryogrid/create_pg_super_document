# pg_mul_u32_overflow

## Location
[src/include/common/int.h:359-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L359-L379)

## Overview
A safe 32-bit unsigned integer multiplication function that detects overflow conditions and prevents undefined behavior.

## Definition
```c
static inline bool pg_mul_u32_overflow(uint32 a, uint32 b, uint32 *result)
```

## Detailed Description
This function performs multiplication of two 32-bit unsigned integers with overflow detection. It leverages compiler built-ins when available (`__builtin_mul_overflow`) for optimal performance. When built-ins are unavailable, it implements manual overflow checking by performing the multiplication in a larger 64-bit integer space and comparing the result against PG_UINT32_MAX.

The function is part of PostgreSQL's safe arithmetic API, designed to prevent silent overflow that could lead to security vulnerabilities or incorrect calculations in critical database operations.

## Parameters / Member Variables
- `a`: First multiplicand
- `b`: Second multiplicand  
- `result`: Pointer to store the multiplication result when no overflow occurs

## Dependencies
- Functions called/Symbols referenced:
  - `__builtin_mul_overflow` (compiler built-in, when available)
  - PG_UINT32_MAX
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns `true` if overflow would occur, `false` if the operation is safe
- Manual implementation uses 64-bit arithmetic to safely detect if the result exceeds 32-bit range
- When overflow is detected without compiler built-ins, sets result to 0x5EED to avoid spurious compiler warnings
- Part of PostgreSQL's comprehensive safe integer arithmetic API covering INT32 operations
- Designed as an inline function for performance in multiplication-heavy code paths
- The fallback implementation widens the operands to uint64 before multiplication to prevent intermediate overflow
- Uses the same overflow detection strategy as the 16-bit version but operates on 32-bit integers with 64-bit intermediate calculations

## Simplified Source

```c
static inline bool pg_mul_u32_overflow(uint32 a, uint32 b, uint32 *result) {
    // Use compiler built-in if available for optimal performance
    #if defined(HAVE__BUILTIN_OP_OVERFLOW)
        return __builtin_mul_overflow(a, b, result);
    #else
        // Manual overflow check: multiply in 64-bit space to detect overflow
        uint64 wide_result = (uint64) a * (uint64) b;

        if (wide_result > PG_UINT32_MAX) {
            return true;  // Overflow detected
        }
        *result = (uint32) wide_result;
        return false;     // Safe multiplication completed
    #endif
}
```