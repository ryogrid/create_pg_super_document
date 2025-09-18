# pg_atomic_compare_exchange_u64

## Location
src/include/port/atomics.h: 507 - 516

## Overview
Atomically compares the value of a 64-bit unsigned integer variable with an expected value and, if they match, replaces it with a new value, returning whether the exchange was successful.

## Definition
```c
static inline bool
pg_atomic_compare_exchange_u64(volatile pg_atomic_uint64 *ptr,
                               uint64 *expected, uint64 newval)
```

## Detailed Description
This function implements the fundamental compare-and-swap (CAS) operation for 64-bit atomic variables. It atomically compares the current value stored in the atomic variable with an expected value. If they are equal, the function replaces the current value with the new value and returns true. If they are not equal, the function updates the expected value with the actual current value and returns false.

This operation is the cornerstone of lock-free programming and is used to implement many higher-level synchronization primitives. The compare-and-swap operation allows multiple threads to safely coordinate updates to shared data without using locks.

The function is implemented as an inline wrapper around the platform-specific implementation `pg_atomic_compare_exchange_u64_impl()`. When atomic 64-bit operations are not simulated (PG_HAVE_ATOMIC_U64_SIMULATION is not defined), it includes pointer alignment assertions to ensure the atomic variable is properly aligned on an 8-byte boundary, which is required for efficient atomic operations on most architectures.

## Parameters / Member Variables
- `ptr`: A pointer to the volatile atomic 64-bit unsigned integer variable to be compared and potentially modified. Must be 8-byte aligned when not using simulation.
- `expected`: A pointer to the expected value. On input, contains the value expected to be in the atomic variable. On output (if the comparison fails), contains the actual current value that was found.
- `newval`: The new 64-bit unsigned integer value to store in the atomic variable if the comparison succeeds.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_compare_exchange_u64_impl](pg_atomic_compare_exchange_u64_impl.md)
  - `AssertPointerAlignment` (when PG_HAVE_ATOMIC_U64_SIMULATION is not defined)
  - [pg_atomic_uint64](pg_atomic_uint64.md) (type)
  - `PG_HAVE_ATOMIC_U64_SIMULATION` (macro)
- Called from (representative examples):
  - `pg_atomic_monotonic_advance_u64` (src/include/port/atomics.h:597)
  - `dsa_pointer_atomic_compare_exchange` (src/include/utils/dsa.h:68)
  - [test_atomic_uint64](../t/test_atomic_uint64.md) (src/test/regress/regress.c:820)

## Notes and Other Information
- This is the fundamental building block for lock-free algorithms and atomic operations
- The function provides strong consistency guarantees - the comparison and potential swap happen as a single atomic operation
- When the comparison fails, the expected value is updated with the actual current value, which is useful for retry loops
- Commonly used in retry loops where multiple attempts may be needed to successfully update a contested atomic variable
- The pointer alignment requirement (8-byte boundary) is enforced through assertions in debug builds
- This function is part of PostgreSQL's portable atomic operations interface, providing consistent behavior across different platforms and architectures
- The volatile qualifier on the pointer parameter prevents compiler optimizations that might eliminate or cache the memory access
- Essential for implementing wait-free and lock-free data structures