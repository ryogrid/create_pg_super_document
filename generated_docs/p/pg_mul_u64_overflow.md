# pg_mul_u64_overflow

## Location
[src/include/common/int.h:414-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L414-L470)

## Overview
A safe unsigned 64-bit integer multiplication function that detects arithmetic overflow, providing multiple implementation strategies based on available compiler and platform features.

## Definition
```c
static inline bool pg_mul_u64_overflow(uint64 a, uint64 b, uint64 *result)
```

## Detailed Description
This inline function performs multiplication of two 64-bit unsigned integers with overflow detection. It returns a boolean indicating whether overflow occurred during the operation. The function provides three different implementation strategies in order of preference:

1. **Compiler built-in approach**: Uses `__builtin_mul_overflow` when available for optimal performance and accuracy
2. **128-bit integer approach**: When 128-bit integers are supported, performs multiplication in 128-bit space and checks if the result exceeds the 64-bit maximum
3. **Division-based fallback**: Uses integer division to verify that the multiplication result is correct, detecting overflow by checking if `b != result / a` (when `a != 0`)

When overflow is detected, the function sets a dummy value (0x5EED) to the result pointer to avoid spurious compiler warnings about uninitialized variables.

## Parameters / Member Variables
- `a`: First 64-bit unsigned integer operand for multiplication
- `b`: Second 64-bit unsigned integer operand for multiplication  
- `result`: Pointer to store the multiplication result (valid only when no overflow occurs)

## Dependencies
- Functions called/Symbols referenced:
  - `__builtin_mul_overflow` (compiler built-in, when available)
  - `HAVE_INT128` (preprocessor macro for 128-bit integer support)
  - `PG_UINT64_MAX` (maximum value for 64-bit unsigned integer)
- Called from (representative examples):
  - [numericvar_to_uint64](../n/numericvar_to_uint64.md) at src/backend/utils/adt/numeric.c:8210

## Notes and Other Information
- The function uses conditional compilation with multiple fallback strategies based on platform capabilities
- The 128-bit integer approach provides exact overflow detection without the edge cases of division-based methods
- The division-based fallback handles the special case where `a == 0` to avoid division by zero
- The dummy value 0x5EED is used consistently across all overflow detection implementations
- This function is part of PostgreSQL's comprehensive safe arithmetic operations suite
- The multiple implementation strategies ensure optimal performance across different compiler and platform combinations

## Simplified Source

```c
static inline bool pg_mul_u64_overflow(uint64 a, uint64 b, uint64 *result) {
    // Use compiler built-in if available
    #if defined(HAVE__BUILTIN_OP_OVERFLOW)
        return __builtin_mul_overflow(a, b, result);

    // Use 128-bit integers for exact overflow detection
    #elif defined(HAVE_INT128)
        uint128 product = (uint128) a * (uint128) b;
        if (product > PG_UINT64_MAX) {
            *result = 0x5EED;  // Dummy value
            return true;       // Overflow
        }
        *result = (uint64) product;
        return false;

    // Fallback: division-based overflow check
    #else
        uint64 product = a * b;
        if (a != 0 && b != product / a) {
            *result = 0x5EED;  // Dummy value
            return true;       // Overflow
        }
        *result = product;
        return false;
    #endif
}
```