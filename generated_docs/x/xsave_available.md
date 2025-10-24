# xsave_available

## Location
[src/port/pg_popcount_avx512_choose.c:40-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_popcount_avx512_choose.c#L40-L60)

## Overview
A static inline function that checks if the CPU supports XSAVE instructions by querying the CPUID instruction to determine if the operating system has enabled the OSXSAVE feature bit.

## Definition

```c
struction not available
#endif
	return (exx[2] & (1 << 27)) != 0;
```
## Detailed Description
This function performs a CPUID query to determine whether the processor and operating system support the XSAVE instruction family. XSAVE is a CPU instruction that allows the operating system to save and restore extended processor state information, which is essential for using advanced SIMD instructions like AVX-512. The function specifically checks the OSXSAVE bit (bit 27) in the ECX register returned by CPUID leaf 1, which indicates that the OS has enabled the XSAVE feature and can properly save/restore extended state during context switches.

The implementation uses platform-specific CPUID intrinsics:
-  on systems that have it
-  on systems with Microsoft-style intrinsics
- Compile-time error if neither is available

## Parameters / Member Variables


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

## Simplified Source

```c
static inline bool xsave_available(void) {
    unsigned int cpuid_regs[4] = {0, 0, 0, 0};

    // Query CPUID leaf 1 using platform-specific intrinsics
    #if defined(HAVE__GET_CPUID)
        __get_cpuid(1, &cpuid_regs[0], &cpuid_regs[1], &cpuid_regs[2], &cpuid_regs[3]);
    #elif defined(HAVE__CPUID)
        __cpuid(cpuid_regs, 1);
    #endif

    // Check OSXSAVE bit (bit 27) in ECX register
    // This indicates OS has enabled XSAVE feature for extended state management
    return (cpuid_regs[2] & (1 << 27)) != 0;
}
```