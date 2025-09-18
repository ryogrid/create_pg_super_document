# zmm_regs_available

## Location
src/port/pg_popcount_avx512_choose.c: 61 - 74

## Overview
A static inline function that checks if the ZMM registers (512-bit SIMD registers used by AVX-512) are enabled by the operating system using the XGETBV instruction.

## Definition
```c
static inline bool zmm_regs_available(void)
```

## Detailed Description
This function uses the XGETBV instruction to query the Extended Control Register (XCR0) to determine if the operating system has enabled the ZMM register state for AVX-512 instructions. The function checks specific bits in XCR0 that must all be set for ZMM registers to be available:

- Bit 1: SSE state
- Bit 2: YMM state (AVX)
- Bits 5-7: ZMM state, ZMM_Hi256 state, and Hi16_ZMM state

The bitmask 0xe6 represents these required bits. All these bits must be set (hence the == 0xe6 comparison) for the OS to properly save/restore the full AVX-512 register state during context switches.

The function requires XSAVE intrinsics to be available and returns false if they are not supported at compile time.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - `_xgetbv()` (XSAVE intrinsic for reading extended control registers)
- Called from (representative examples):
  - [pg_popcount_avx512_available](../p/pg_popcount_avx512_available.md) at src/port/pg_popcount_avx512_choose.c:98

## Notes and Other Information
- This is a static inline function, only visible within the compilation unit
- Caller must verify `xsave_available()` returns true before calling this function
- Returns false at compile time if HAVE_XSAVE_INTRINSICS is not defined
- The 0xe6 bitmask specifically checks for SSE (bit 1), AVX (bit 2), and all AVX-512 state bits (bits 5, 6, 7)
- This function is part of the runtime feature detection chain for enabling AVX-512 optimizations
- Proper ZMM register support requires both CPU capability and OS enablement