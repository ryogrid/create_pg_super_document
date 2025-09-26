# pg_popcount32

## Location
src/port/pg_bitutils.c: 499 - 504

## Overview
An external interface function for 32-bit population count operations that provides a consistent API regardless of whether hardware-optimized instructions are available.

## Definition


## Detailed Description
This function serves as the primary external interface for 32-bit population count operations in PostgreSQL. Its implementation varies based on compile-time configuration:

1. **When TRY_POPCNT_FAST is not defined**: This function is implemented as a simple wrapper that directly calls . The compiler is expected to inline the slow version for optimal performance.

2. **When TRY_POPCNT_FAST is defined**: This becomes a function pointer that is dynamically set at runtime to either:
   - A fast hardware-optimized implementation using POPCNT instructions
   - The slow fallback implementation ()

The choice between these approaches is made at runtime through  based on CPU feature detection. This design allows PostgreSQL to automatically use the fastest available popcount implementation while maintaining a consistent API.

## Parameters / Member Variables
- : The 32-bit unsigned integer for which to count the number of set bits

## Dependencies
- Functions called/Symbols referenced:
  -  (fallback implementation)
- Called from (representative examples):
  -  (in user management)
  -  (bitmap operations)
  -  (runtime selection)
  -  (function pointer selection)

## Notes and Other Information
- This function demonstrates PostgreSQL's adaptive approach to performance optimization
- The implementation automatically adapts based on available hardware capabilities
- When used as a function pointer (TRY_POPCNT_FAST), it's initialized during system startup
- Part of the core bit manipulation API used throughout PostgreSQL for various operations including bitmap processing and user permission management
- The simple wrapper approach (when TRY_POPCNT_FAST is not defined) relies on compiler optimization to eliminate function call overhead