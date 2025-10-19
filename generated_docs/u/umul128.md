# umul128

## Location
[src/common/d2s_intrinsics.h:42-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s_intrinsics.h#L42-L47)

## Overview
Computes the full 128-bit product of two 64-bit unsigned integers, returning the low 64 bits and storing the high 64 bits in an output parameter.

## Definition

```c
static inline uint64
umul128(const uint64 a, const uint64 b, uint64 *const productHi)
```
## Detailed Description
The  function performs 64-bit × 64-bit → 128-bit unsigned multiplication. This is part of the Ryu floating-point number formatting algorithm used in PostgreSQL for fast and accurate double-precision floating-point to string conversion. The function has two implementations depending on compiler intrinsic availability:

1. **With 64-bit intrinsics (HAS_64_BIT_INTRINSICS defined)**: Uses the compiler's  intrinsic for optimal performance on platforms that support it (typically x64 with MSVC).

2. **Without intrinsics**: Uses a manual implementation that breaks down the 64-bit operands into 32-bit parts and performs four 32-bit multiplications, then combines the results using the standard multiplication algorithm. This ensures portability across all platforms while avoiding library function calls.

The function is critical for the Ryu algorithm's precision requirements, as it needs to perform exact arithmetic on large integers during the floating-point conversion process.

## Parameters / Member Variables
- `a`: First 64-bit unsigned integer operand
- `b`: Second 64-bit unsigned integer operand
- `productHi`: Pointer to store the high 64 bits of the 128-bit product
## Dependencies
- Functions called/Symbols referenced:
  -  (when HAS_64_BIT_INTRINSICS is defined)
- Called from (representative examples):
  -  (in src/common/d2s.c:188, 195)
  -  (in src/common/d2s_intrinsics.h:128)

## Notes and Other Information
- This is part of the Ryu floating-point formatting library, originally by Ulf Adams
- The manual implementation uses careful casting to help MSVC avoid calls to the  library function
- The function is declared as  for performance optimization
- Used primarily in 32-bit platform builds where efficient 64-bit arithmetic operations are crucial for performance

## Simplified Source

```c
static inline uint64 umul128(const uint64 a, const uint64 b, uint64 *const productHi) {
    // Use compiler intrinsic for 64-bit × 64-bit → 128-bit multiplication
    return _umul128(a, b, productHi);
}
```