# pg_atomic_fetch_and_u64_impl

## Location
src/include/port/atomics/generic-gcc.h: 306 - 312

## Overview
Implements atomic fetch-and-bitwise-AND operation for 64-bit unsigned integers, atomically performing bitwise AND and returning the previous value.

## Definition
```c
static inline uint64 pg_atomic_fetch_and_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 and_)
```

## Detailed Description
This function provides platform-specific implementations of atomic fetch-and-bitwise-AND operations for 64-bit unsigned integers. There are multiple implementations depending on compiler and platform capabilities:

1. **GCC implementation** (src/include/port/atomics/generic-gcc.h:306-312): Uses GCC's `__sync_fetch_and_and` builtin when available for direct hardware-optimized bitwise AND operation
2. **Generic fallback implementation** (src/include/port/atomics/generic.h:380-388): Uses compare-and-swap loop when native fetch-and is not available

The function atomically performs a bitwise AND operation between the current value at the memory location and the provided mask, returning the value that was previously stored at that location.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to operate on
- `and_`: The 64-bit unsigned integer mask to AND with the current value

## Dependencies
- Functions called/Symbols referenced:
  - __sync_fetch_and_and (GCC implementation)
  - pg_atomic_compare_exchange_u64_impl (generic fallback)
  - pg_atomic_uint64 (type)
- Called from (representative examples):
  - pg_atomic_fetch_and_u64 (inline wrapper)

## Notes and Other Information
- Multiple implementations exist for different compilers and platforms
- GCC version uses hardware-optimized builtin functions when supported for better performance
- Generic implementation uses compare-and-swap loop with bitwise AND logic for broader platform compatibility
- Commonly used for atomic bit manipulation operations like clearing specific bits
- Part of PostgreSQL's portable atomic operations infrastructure providing consistent API across platforms
- Located primarily in src/include/port/atomics/generic-gcc.h:306-312 for the GCC implementation
- Generic fallback ensures the operation works even on platforms without native bitwise atomic operations