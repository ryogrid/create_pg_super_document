# RT_LOCK_SHARE

## Location
[src/include/lib/radixtree.h:1948-1954](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/radixtree.h#L1948-L1954)

## Overview
RT_LOCK_SHARE is a macro that generates a function name for acquiring a shared lock on a shared memory radix tree structure in PostgreSQL.

## Definition

```c
RT_SCOPE void
RT_LOCK_SHARE(RT_RADIX_TREE * tree)
```
## Detailed Description
RT_LOCK_SHARE is part of PostgreSQL's generic radix tree implementation for shared memory usage. This macro uses the RT_MAKE_NAME helper to generate a prefixed function name that acquires a shared lock on a shared memory radix tree. The actual function signature generated would be:



This macro is only available when RT_SHMEM is defined, indicating the radix tree is configured for shared memory operations. A shared lock allows multiple processes to read from the radix tree concurrently, but prevents write operations. This enables efficient concurrent read access while maintaining data consistency. Multiple shared locks can be held simultaneously, but they are mutually exclusive with exclusive locks.

## Parameters / Member Variables
- Uses RT_MAKE_NAME macro to construct the actual function name  
- The generated function takes a pointer to RT_RADIX_TREE and returns void
- **tree**: Pointer to the radix tree structure to lock for shared access

## Dependencies
- Functions called/Symbols referenced:
  - RT_MAKE_NAME
  - RT_MAKE_PREFIX
  - RT_PREFIX (defined by the including code)
- Called from (representative examples):
  - Functions performing read operations on shared radix trees
  - [Query](../Q/Query.md) routines that need consistent read access
- Related symbols:
  - [RT_LOCK_EXCLUSIVE](RT_LOCK_EXCLUSIVE.md) (exclusive/write lock counterpart)
  - [RT_UNLOCK](RT_UNLOCK.md) (releases the acquired lock)

## Notes and Other Information
- Only available when RT_SHMEM preprocessing directive is defined
- Part of PostgreSQL's template-based radix tree implementation
- Must be paired with RT_UNLOCK to release the lock
- Uses PostgreSQL's LWLock mechanism internally for synchronization
- Multiple shared locks can be held concurrently by different processes
- Shared locks block exclusive lock requests until all shared locks are released
- Ideal for read-heavy workloads where concurrent access improves performance