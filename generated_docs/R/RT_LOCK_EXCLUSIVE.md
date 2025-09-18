# RT_LOCK_EXCLUSIVE

## Location
src/include/lib/radixtree.h: 1941 - 1947

## Overview
RT_LOCK_EXCLUSIVE is a macro that generates a function name for acquiring an exclusive lock on a shared memory radix tree structure in PostgreSQL.

## Definition


## Detailed Description
RT_LOCK_EXCLUSIVE is part of PostgreSQL's generic radix tree implementation for shared memory usage. This macro uses the RT_MAKE_NAME helper to generate a prefixed function name that acquires an exclusive lock on a shared memory radix tree. The actual function signature generated would be:



This macro is only available when RT_SHMEM is defined, indicating the radix tree is configured for shared memory operations. An exclusive lock prevents other processes from reading or writing to the radix tree simultaneously, ensuring data consistency during modification operations. This is essential for concurrent access control in multi-process environments.

## Parameters / Member Variables
- Uses RT_MAKE_NAME macro to construct the actual function name
- The generated function takes a pointer to RT_RADIX_TREE and returns void
- **tree**: Pointer to the radix tree structure to lock exclusively

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX  
  - RT_PREFIX (defined by the including code)
- Called from (representative examples):
  - Functions performing write operations on shared radix trees
  - Modification routines that require exclusive access
- Related symbols:
  - RT_LOCK_SHARE (shared/read lock counterpart)
  - RT_UNLOCK (releases the acquired lock)

## Notes and Other Information
- Only available when RT_SHMEM preprocessing directive is defined
- Part of PostgreSQL's template-based radix tree implementation
- Must be paired with RT_UNLOCK to release the lock
- Uses PostgreSQL's LWLock mechanism internally for synchronization
- Exclusive locks are blocking - the calling process will wait if another process holds a lock
- Essential for maintaining data integrity in concurrent shared memory access scenarios