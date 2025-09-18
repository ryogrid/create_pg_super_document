# pg_atomic_compare_exchange_u32

## Location
src/include/port/atomics.h: 344 - 360

## Overview
Performs an atomic compare-and-swap (CAS) operation on a 32-bit unsigned integer, comparing the current value with an expected value and conditionally storing a new value.

## Definition
```c
static inline bool
pg_atomic_compare_exchange_u32(volatile pg_atomic_uint32 *ptr,
                               uint32 *expected, uint32 newval)
```

## Detailed Description
This function implements the fundamental compare-and-swap (CAS) atomic operation. It atomically compares the current value stored at `ptr` with the value pointed to by `expected`. If they are equal, it stores `newval` at the location pointed to by `ptr` and returns true. If they are not equal, it stores the actual current value of `ptr` into `*expected` and returns false without modifying `ptr`.

The operation provides full memory barrier semantics, ensuring proper ordering of memory operations. This is the cornerstone primitive for implementing lock-free data structures and algorithms, as it allows for atomic updates with failure detection.

The function is implemented as a wrapper around `pg_atomic_compare_exchange_u32_impl`, which contains the platform-specific implementation.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be compared and potentially modified
- `expected`: Pointer to the expected value; will be updated with the actual current value if the comparison fails
- `newval`: The new 32-bit unsigned integer value to store if the comparison succeeds

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint32 (type definition)
  - AssertPointerAlignment (alignment checks for both ptr and expected)
  - pg_atomic_compare_exchange_u32_impl (platform-specific implementation)
- Called from (representative examples):
  - TransactionGroupUpdateXidStatus (src/backend/access/transam/clog.c:518)
  - MarkBufferDirty (src/backend/storage/buffer/bufmgr.c:2552)
  - PinBuffer (src/backend/storage/buffer/bufmgr.c:2686)
  - UnpinBufferNoOwner (src/backend/storage/buffer/bufmgr.c:2849)
  - LWLockAttemptLock (src/backend/storage/lmgr/lwlock.c:829)

## Notes and Other Information
- Returns true if the exchange occurred, false if the comparison failed
- The `expected` parameter serves dual purpose: input (comparison value) and output (actual current value)
- Both `ptr` and `expected` must be 4-byte aligned as enforced by AssertPointerAlignment
- Provides full barrier semantics, making it suitable for implementing synchronization primitives
- This is the fundamental building block for lock-free programming in PostgreSQL
- Commonly used in retry loops where the caller checks the returned value and retries if the operation failed
- Essential for implementing atomic updates to reference counts, flags, and other shared state