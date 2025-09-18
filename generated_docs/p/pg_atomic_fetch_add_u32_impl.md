# pg_atomic_fetch_add_u32_impl

## Location
[src/include/port/atomics/generic.h:183-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic.h#L183-L193)

## Overview
Implements atomic fetch-and-add operation for 32-bit unsigned integers using spinlock-based fallback implementation, returning the original value before addition.

## Definition
```c
uint32 pg_atomic_fetch_add_u32_impl(volatile pg_atomic_uint32 *ptr, int32 add_)
```

## Detailed Description
This function provides a fallback implementation of atomic fetch-and-add operation for platforms that do not have native hardware support for atomic operations. It uses PostgreSQL's spinlock mechanism to ensure atomicity by acquiring an exclusive lock, performing the addition operation, and then releasing the lock. The function returns the original value that was stored in the atomic variable before the addition took place.

This implementation is part of PostgreSQL's portable atomic operations layer, which provides a consistent interface across different platforms and hardware architectures. When native atomic operations are not available, this spinlock-based approach ensures correctness at the cost of some performance compared to hardware-native implementations.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `add_`: The signed 32-bit value to add to the current value (can be negative for subtraction)

## Dependencies
- Functions called/Symbols referenced:
  - `SpinLockAcquire` (PostgreSQL spinlock function)
  - `SpinLockRelease` (PostgreSQL spinlock function)
  - [slock_t](../s/slock_t.md) (PostgreSQL spinlock type)
  - [pg_atomic_uint32](pg_atomic_uint32.md) (PostgreSQL atomic type)
- Called from (representative examples):
  - [pg_atomic_fetch_add_u32](pg_atomic_fetch_add_u32.md)
  - [pg_atomic_fetch_sub_u32_impl](pg_atomic_fetch_sub_u32_impl.md)
  - [pg_atomic_add_fetch_u32_impl](pg_atomic_add_fetch_u32_impl.md)
  - [pg_atomic_read_membarrier_u32_impl](pg_atomic_read_membarrier_u32_impl.md)

## Notes and Other Information
- This is a fallback implementation used when hardware atomic operations are not available
- The spinlock-based approach provides correctness but may have higher overhead than native atomic operations
- The function is thread-safe but may cause blocking if multiple threads contend for the same atomic variable
- Part of PostgreSQL's conditional compilation system - only compiled when native atomic operations are unavailable
- The semaphore field in the atomic structure is used as a spinlock for synchronization