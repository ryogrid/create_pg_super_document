# LWLockAnyHeldByMe

## Location
[src/backend/storage/lmgr/lwlock.c:1913-1938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1913-L1938)

## Overview
A debugging function that checks whether the current process holds any LWLock from a specified array of locks, useful for verifying that no locks from a particular set are held.

## Definition
```c
bool LWLockAnyHeldByMe(LWLock *lock, int nlocks, size_t stride)
```

## Detailed Description
LWLockAnyHeldByMe is a debugging utility that determines whether the current process holds any lock from a specified array of LWLocks. Unlike LWLockHeldByMe which checks for a single specific lock, this function checks for ownership of any lock within a contiguous array of locks.

The function works by defining a memory range from the starting lock address to the end of the array (calculated using nlocks * stride), then iterating through all currently held locks to see if any fall within this range. The stride parameter allows for flexible array layouts where locks may not be contiguously packed, but are instead separated by a fixed offset.

This is particularly useful for debugging scenarios where you need to ensure that no locks from a particular subsystem or lock group are held, such as partition locks in hash tables or buffer locks in a buffer pool.

## Parameters / Member Variables
- `lock`: Pointer to the first LWLock in the array to check
- `nlocks`: Number of locks in the array
- `stride`: Byte offset between consecutive locks in the array

## Dependencies
- Global variables used:
  - num_held_lwlocks: Current count of held locks by the process
  - held_lwlocks: Array containing information about currently held locks
- Returns: boolean value indicating whether any lock from the specified array is held
- Called from (representative examples):
  - ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME: Macro used in dshash.c for asserting no partition locks are held

## Notes and Other Information
- This function is explicitly documented as "debug support only" in the source code
- The stride parameter enables checking of non-contiguous lock arrays where locks are separated by fixed offsets
- Uses address arithmetic to determine if a held lock falls within the specified array range
- The modulo operation ((held_lock_addr - begin) % stride == 0) ensures that held locks align with the expected stride pattern
- Primarily used in assertions to verify proper lock management in complex data structures
- More efficient than calling LWLockHeldByMe multiple times for each lock in an array
- Essential for debugging lock contention and ensuring proper lock release in subsystems that use arrays of locks
- Located in src/backend/storage/lmgr/lwlock.c:1913-1938