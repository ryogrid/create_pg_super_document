# shiftright128

## Location
src/common/d2s_intrinsics.h: 48 - 64

## Overview
Performs a right shift operation on a 128-bit integer represented as two 64-bit values, returning the shifted result as a 64-bit value.

## Definition


## Detailed Description
The  function performs a right shift operation on a 128-bit value represented by two 64-bit integers (low and high parts). This function is part of the Ryu floating-point formatting algorithm and has two implementations depending on compiler intrinsic availability:

1. **With 64-bit intrinsics (HAS_64_BIT_INTRINSICS defined)**: Uses the compiler's  intrinsic for optimal performance. The intrinsic automatically handles modulo 64 behavior for the shift distance.

2. **Without intrinsics**: Provides a manual implementation that:
   - On 64-bit platforms: Uses bit manipulation with 
   - On 32-bit platforms: Uses optimized logic assuming dist >= 32 to avoid expensive 64-bit shifts

The function includes assertions to ensure the shift distance is less than 64 bits, which is guaranteed by the current Ryu algorithm implementation. The shift ranges are [49, 58] when RYU_OPTIMIZE_SIZE == 0, otherwise [2, 59].

## Parameters / Member Variables
- : Low 64 bits of the 128-bit integer to be shifted
- : High 64 bits of the 128-bit integer to be shifted  
- : Number of bits to shift right (must be < 64)

## Dependencies
- Functions called/Symbols referenced:
  -  (when HAS_64_BIT_INTRINSICS is defined)
  -  (for runtime validation)
- Called from (representative examples):
  -  (in src/common/d2s.c:204)

## Notes and Other Information
- Part of the Ryu floating-point formatting library for fast double-precision to string conversion
- The function is declared as  for performance optimization
- Contains platform-specific optimizations for both 32-bit and 64-bit architectures
- The assertion validates that shift distances stay within expected bounds, protecting against future algorithm changes that might require larger shifts
- On 32-bit platforms, the implementation takes advantage of the known shift range to avoid expensive 64-bit shift operations