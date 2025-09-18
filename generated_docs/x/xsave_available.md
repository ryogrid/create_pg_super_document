# xsave_available

## Location
src/port/pg_popcount_avx512_choose.c: 40 - 60

## Overview
A static inline function that checks if the CPU supports XSAVE instructions by querying the CPUID instruction to determine if the operating system has enabled the OSXSAVE feature bit.

## Definition


## Detailed Description
This function performs a CPUID query to determine whether the processor and operating system support the XSAVE instruction family. XSAVE is a CPU instruction that allows the operating system to save and restore extended processor state information, which is essential for using advanced SIMD instructions like AVX-512. The function specifically checks the OSXSAVE bit (bit 27) in the ECX register returned by CPUID leaf 1, which indicates that the OS has enabled the XSAVE feature and can properly save/restore extended state during context switches.

The implementation uses platform-specific CPUID intrinsics:
-  on systems that have it
-  on systems with Microsoft-style intrinsics
- Compile-time error if neither is available

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  or  (platform-specific CPUID intrinsics)
- Called from (representative examples):
  -  at src/port/pg_popcount_avx512_choose.c:97

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the compilation unit
- The function is part of the runtime CPU feature detection mechanism for AVX-512 support
- OSXSAVE support is a prerequisite for safely using extended SIMD instruction sets like AVX-512
- The function will cause a compile-time error on platforms that don't provide CPUID intrinsics
- The checked bit (ECX bit 27) specifically indicates OS-level XSAVE support, not just CPU capability