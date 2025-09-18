# pg_atomic_fetch_add_u64_impl

## Location
src/include/port/atomics/generic-gcc.h: 288 - 294

## Overview
Implements atomic fetch-and-add operation for 64-bit unsigned integers, atomically adding a value and returning the previous value.

## Definition
```c
uint64 pg_atomic_fetch_add_u64_impl(volatile pg_atomic_uint64 *ptr, int64 add_)
```

## Detailed Description
This function provides platform-specific implementations of atomic fetch-and-add operations for 64-bit unsigned integers. There are multiple implementations depending on compiler and platform capabilities:

1. **Spinlock-based fallback implementation** (src/backend/port/atomics.c:228-239): Uses spinlocks to ensure atomicity when hardware atomic operations are not available
2. **GCC implementation** (src/include/port/atomics/generic-gcc.h:288-291): Uses GCC's `__sync_fetch_and_add` builtin when available
3. **Generic fallback implementation** (src/include/port/atomics/generic.h:358-365): Uses compare-and-swap loop when native fetch-add is not available

The function atomically adds the specified value to the memory location and returns the value that was previously stored at that location.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to operate on
- `add_`: The signed 64-bit value to add (can be negative for subtraction)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease (spinlock implementation)
  - __sync_fetch_and_add (GCC implementation)
  - pg_atomic_compare_exchange_u64_impl (generic fallback)
  - pg_atomic_uint64 (type)
  - slock_t (type)
- Called from (representative examples):
  - pg_atomic_fetch_add_u64 (inline wrapper)
  - pg_atomic_fetch_sub_u64_impl
  - pg_atomic_add_fetch_u64_impl
  - pg_atomic_read_membarrier_u64_impl

## Notes and Other Information
- Multiple implementations exist for different compilers and platforms
- Spinlock-based implementation is used as the ultimate fallback when no hardware atomic support is available
- GCC version uses hardware-optimized builtin functions when supported
- Generic implementation uses compare-and-swap loop for broader platform compatibility
- Used as building block for other atomic operations like subtract, add-fetch, and memory barrier reads
- Part of PostgreSQL's portable atomic operations infrastructure
- Located primarily in src/backend/port/atomics.c:228-239 for the fallback implementation