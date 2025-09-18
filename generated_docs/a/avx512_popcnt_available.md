# avx512_popcnt_available

## Location
[src/port/pg_popcount_avx512_choose.c:75-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_popcount_avx512_choose.c#L75-L94)

## Overview
A static inline function that uses CPUID to detect CPU support for specific AVX-512 instruction subsets required for optimized population count operations: AVX512-VPOPCNTDQ and AVX512-BW.

## Definition
```c
static inline bool avx512_popcnt_available(void)
```

## Detailed Description
This function performs a CPUID query using leaf 7, subleaf 0 to check for the availability of two specific AVX-512 instruction set extensions that are required for efficient population count (bit counting) operations:

1. **AVX512-VPOPCNTDQ** (ECX bit 14): Vector population count instructions for double and quad word elements
2. **AVX512-BW** (EBX bit 30): Byte and word operations support for AVX-512

Both instruction sets must be available for the function to return true. The function uses platform-specific CPUID intrinsics to perform the capability detection:
-  on systems that support it
-  on systems with Microsoft-style intrinsics
- Compile-time error if neither is available

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - `__get_cpuid_count()` or `__cpuidex()` (platform-specific CPUID intrinsics)
- Called from (representative examples):
  - [pg_popcount_avx512_available](../p/pg_popcount_avx512_available.md) at src/port/pg_popcount_avx512_choose.c:99

## Notes and Other Information
- This is a static inline function, only visible within the compilation unit
- Both AVX512-VPOPCNTDQ and AVX512-BW must be available for the function to return true
- AVX512-VPOPCNTDQ provides vectorized population count instructions for 32-bit and 64-bit elements
- AVX512-BW provides byte and word-level operations in 512-bit registers
- The function will cause a compile-time error on platforms without CPUID intrinsic support
- This is part of the CPU feature detection chain for enabling AVX-512 optimized population count implementations
- CPUID leaf 7, subleaf 0 contains structured extended feature information