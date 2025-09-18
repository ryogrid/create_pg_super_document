# pg_atomic_exchange_u64

## Location
src/include/port/atomics.h: 498 - 506

## Overview
Atomically exchanges the value of a 64-bit unsigned integer variable with a new value, returning the previous value that was stored.

## Definition
```c
static inline uint64
pg_atomic_exchange_u64(volatile pg_atomic_uint64 *ptr, uint64 newval)
```

## Detailed Description
This function provides a thread-safe way to atomically exchange the value stored in a 64-bit atomic variable with a new value. The operation is atomic, meaning that no other thread can observe an intermediate state during the exchange. The function returns the value that was previously stored in the atomic variable before the exchange occurred.

This operation is commonly used in lock-free programming and synchronization primitives where you need to atomically update a value while also retrieving the previous value for further processing or verification.

The function is implemented as an inline wrapper around the platform-specific implementation `pg_atomic_exchange_u64_impl()`. When atomic 64-bit operations are not simulated (PG_HAVE_ATOMIC_U64_SIMULATION is not defined), it includes pointer alignment assertions to ensure the atomic variable is properly aligned on an 8-byte boundary, which is required for efficient atomic operations on most architectures.

## Parameters / Member Variables
- `ptr`: A pointer to the volatile atomic 64-bit unsigned integer variable whose value will be exchanged. Must be 8-byte aligned when not using simulation.
- `newval`: The new 64-bit unsigned integer value that will be stored in the atomic variable.

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_atomic_exchange_u64_impl](pg_atomic_exchange_u64_impl.md)`
  - `AssertPointerAlignment` (when PG_HAVE_ATOMIC_U64_SIMULATION is not defined)
  - `[pg_atomic_uint64](pg_atomic_uint64.md)` (type)
  - `PG_HAVE_ATOMIC_U64_SIMULATION` (macro)
- Called from (representative examples):
  - `LWLockUpdateVar` (src/backend/storage/lmgr/lwlock.c:1733)
  - `LWLockReleaseClearVar` (src/backend/storage/lmgr/lwlock.c:1862)
  - `[test_atomic_uint64](../t/test_atomic_uint64.md)` (src/test/regress/regress.c:815)

## Notes and Other Information
- This is a read-modify-write operation that is guaranteed to be atomic across all threads
- Commonly used in lock-free algorithms and synchronization primitives where you need both the old and new values
- The return value allows callers to take action based on the previous state of the variable
- Used extensively in PostgreSQL's lightweight lock (LWLock) implementation for managing shared variables
- The pointer alignment requirement (8-byte boundary) is enforced through assertions in debug builds
- This function is part of PostgreSQL's portable atomic operations interface, providing consistent behavior across different platforms and architectures
- The volatile qualifier on the pointer parameter prevents compiler optimizations that might eliminate or cache the memory access