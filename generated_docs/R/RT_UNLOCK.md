# RT_UNLOCK

## Location
src/include/lib/radixtree.h: 1955 - 1964

## Overview
RT_UNLOCK is a macro that generates a function name for releasing a lock on a shared memory radix tree structure in PostgreSQL.

## Definition


## Detailed Description
RT_UNLOCK is part of PostgreSQL's generic radix tree implementation for shared memory usage. This macro uses the RT_MAKE_NAME helper to generate a prefixed function name that releases any type of lock (exclusive or shared) previously acquired on a shared memory radix tree. The actual function signature generated would be:



This macro is only available when RT_SHMEM is defined, indicating the radix tree is configured for shared memory operations. RT_UNLOCK releases both exclusive and shared locks, allowing other processes to acquire locks on the radix tree. Proper lock release is critical to prevent deadlocks and ensure system performance in multi-process environments.

## Parameters / Member Variables
- Uses RT_MAKE_NAME macro to construct the actual function name
- The generated function takes a pointer to RT_RADIX_TREE and returns void
- **tree**: Pointer to the radix tree structure to unlock

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX
  - RT_PREFIX (defined by the including code)
- Called from (representative examples):
  - Functions that previously called RT_LOCK_EXCLUSIVE
  - Functions that previously called RT_LOCK_SHARE
  - Cleanup and error handling routines
- Related symbols:
  - RT_LOCK_EXCLUSIVE (acquires exclusive lock)
  - RT_LOCK_SHARE (acquires shared lock)

## Notes and Other Information
- Only available when RT_SHMEM preprocessing directive is defined
- Part of PostgreSQL's template-based radix tree implementation
- Must be called after every RT_LOCK_EXCLUSIVE or RT_LOCK_SHARE operation
- Uses PostgreSQL's LWLock mechanism internally for synchronization
- Failure to call RT_UNLOCK can lead to deadlocks and hung processes
- Should be called in exception handlers to ensure locks are released even on errors
- The function automatically determines the type of lock to release (exclusive or shared)