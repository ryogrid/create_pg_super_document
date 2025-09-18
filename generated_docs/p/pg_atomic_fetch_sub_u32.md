# pg_atomic_fetch_sub_u32

## Location
src/include/port/atomics.h: 376 - 390

## Overview
Atomically subtracts a signed 32-bit integer value from a 32-bit unsigned atomic variable and returns the original value before the subtraction.

## Definition
```c
static inline uint32
pg_atomic_fetch_sub_u32(volatile pg_atomic_uint32 *ptr, int32 sub_)
```

## Detailed Description
This function performs an atomic fetch-and-subtract operation on a 32-bit unsigned integer. It atomically subtracts the signed value `sub_` from the value pointed to by `ptr` and returns the original value that was stored at that location before the subtraction. The subtraction is performed using modular arithmetic, so underflow wraps around according to standard C semantics.

The operation provides full memory barrier semantics, ensuring that all memory operations before this call are completed before the subtraction, and all memory operations after this call happen after the subtraction. This function is particularly useful for implementing atomic counters, reference counting, and other scenarios where you need to atomically decrement a value and also need to know the previous value.

The function includes a platform limitation check that prevents `sub_` from being `INT_MIN`, as this value may cause issues on certain platforms. The function is implemented as a wrapper around `pg_atomic_fetch_sub_u32_impl`, which contains the platform-specific implementation.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `sub_`: The signed 32-bit integer value to subtract from the atomic variable (must not be INT_MIN)

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint32 (type definition)
  - AssertPointerAlignment (alignment check)
  - Assert (runtime assertion for INT_MIN check)
  - pg_atomic_fetch_sub_u32_impl (platform-specific implementation)
- Called from (representative examples):
  - LWLockDequeueSelf (src/backend/storage/lmgr/lwlock.c:1154)
  - LWLockAcquire (src/backend/storage/lmgr/lwlock.c:1301)
  - LWLockAcquireOrWait (src/backend/storage/lmgr/lwlock.c:1463)
  - LWLockWaitForVar (src/backend/storage/lmgr/lwlock.c:1681)
  - pgstat_release_entry_ref (src/backend/utils/activity/pgstat_shmem.c:571)

## Notes and Other Information
- Returns the value that was stored before the subtraction operation
- The `sub_` parameter must not be `INT_MIN` due to platform limitations
- Provides full barrier semantics, making it suitable for synchronization and coordination
- The pointer must be 4-byte aligned as enforced by AssertPointerAlignment
- Commonly used for implementing atomic counters, reference counts, and resource tracking
- Underflow behavior follows standard C arithmetic (wraps around for unsigned integers)
- Frequently used in lock management and resource deallocation scenarios where you need to atomically decrement counts
- The INT_MIN restriction is enforced by an assertion to prevent potential platform-specific issues