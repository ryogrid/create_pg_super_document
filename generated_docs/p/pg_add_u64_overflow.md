# pg_add_u64_overflow

## Location
[src/include/common/int.h:380-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L380-L397)

## Overview
A safe unsigned 64-bit integer addition function that detects arithmetic overflow, preventing undefined behavior that can occur with standard arithmetic operations.

## Definition

```c
static inline bool
pg_add_u64_overflow(uint64 a, uint64 b, uint64 *result)
```
## Detailed Description
This inline function performs addition of two 64-bit unsigned integers with overflow detection. It returns a boolean indicating whether overflow occurred during the operation. The function leverages compiler built-ins when available (`__builtin_add_overflow`) for optimal performance and accuracy, falling back to a manual overflow check on systems without built-in support.

When overflow is detected in the fallback implementation, the function sets a dummy value (0x5EED) to the result pointer to avoid spurious compiler warnings about uninitialized variables, though the actual result should not be used when overflow occurs.

## Parameters / Member Variables
- `a`: First 64-bit unsigned integer operand for addition
- `b`: Second 64-bit unsigned integer operand for addition  
- `result`: Pointer to store the addition result (valid only when no overflow occurs)

## Dependencies
- Functions called/Symbols referenced:
  - `__builtin_add_overflow` (compiler built-in, when available)
- Called from (representative examples):
  - [numericvar_to_uint64](../n/numericvar_to_uint64.md) at src/backend/utils/adt/numeric.c:8218

## Notes and Other Information
- The function uses conditional compilation with `HAVE__BUILTIN_OP_OVERFLOW` to choose between compiler built-in and manual overflow detection
- In the manual implementation, overflow is detected by checking if the result is less than the first operand
- The dummy value 0x5EED is used to prevent compiler warnings about uninitialized memory when overflow occurs
- This function is part of PostgreSQL's safe arithmetic operations suite for preventing integer overflow vulnerabilities

## Simplified Source

```c
static inline bool pg_add_u64_overflow(uint64 a, uint64 b, uint64 *result) {
    // Use compiler built-in if available for optimal performance
    #if defined(HAVE__BUILTIN_OP_OVERFLOW)
        return __builtin_add_overflow(a, b, result);
    #else
        // Manual overflow check: result < a indicates overflow
        uint64 sum = a + b;
        if (sum < a) {
            *result = 0x5EED;  // Dummy value to avoid compiler warnings
            return true;       // Overflow occurred
        }
        *result = sum;
        return false;          // No overflow
    #endif
}
```