# pg_atomic_fetch_sub_u64_impl

## Location
src/include/port/atomics/generic-gcc.h: 297 - 303

## Overview
Implements atomic fetch-and-subtract operation for 64-bit unsigned integers, atomically subtracting a value and returning the previous value.

## Definition
```c
static inline uint64 pg_atomic_fetch_sub_u64_impl(volatile pg_atomic_uint64 *ptr, int64 sub_)
```

## Detailed Description
This function provides platform-specific implementations of atomic fetch-and-subtract operations for 64-bit unsigned integers. There are multiple implementations depending on compiler and platform capabilities:

1. **GCC implementation** (src/include/port/atomics/generic-gcc.h:297-303): Uses GCC's `__sync_fetch_and_sub` builtin when available for direct hardware-optimized subtraction
2. **Generic fallback implementation** (src/include/port/atomics/generic.h:371-375): Implemented as negated addition using `pg_atomic_fetch_add_u64_impl(ptr, -sub_)` when native fetch-sub is not available

The function atomically subtracts the specified value from the memory location and returns the value that was previously stored at that location.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to operate on
- `sub_`: The signed 64-bit value to subtract (can be negative for addition)

## Dependencies
- Functions called/Symbols referenced:
  - __sync_fetch_and_sub (GCC implementation)
  - [pg_atomic_fetch_add_u64_impl](pg_atomic_fetch_add_u64_impl.md) (generic fallback)
  - [pg_atomic_uint64](pg_atomic_uint64.md) (type)
- Called from (representative examples):
  - [pg_atomic_fetch_sub_u64](pg_atomic_fetch_sub_u64.md) (inline wrapper)
  - [pg_atomic_sub_fetch_u64_impl](pg_atomic_sub_fetch_u64_impl.md)

## Notes and Other Information
- Multiple implementations exist for different compilers and platforms
- GCC version uses hardware-optimized builtin functions when supported
- Generic implementation cleverly reuses fetch-add by negating the subtraction value
- More efficient than generic compare-and-swap loops when hardware support is available
- Used as building block for other atomic operations like sub-fetch
- Part of PostgreSQL's portable atomic operations infrastructure providing consistent API across platforms
- Located primarily in src/include/port/atomics/generic-gcc.h:297-303 for the GCC implementation