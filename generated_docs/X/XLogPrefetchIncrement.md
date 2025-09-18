# XLogPrefetchIncrement

## Location
[src/backend/access/transam/xlogprefetcher.c:351-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L351-L361)

## Overview
A thread-safe utility function that atomically increments a counter in shared memory for WAL prefetch statistics tracking.

## Definition
```c
static inline void XLogPrefetchIncrement(pg_atomic_uint64 *counter)
```

## Detailed Description
XLogPrefetchIncrement provides a platform-safe way to increment 64-bit atomic counters used for tracking WAL prefetch statistics. The function ensures atomic increment operations even on platforms where reading uint64 values might result in torn reads. It uses PostgreSQL's atomic operation primitives to perform a safe read-modify-write sequence without explicit locking.

The function includes an assertion to ensure it's only called by the startup process or outside the postmaster context, maintaining the expected execution environment for WAL prefetch operations.

## Parameters / Member Variables
- `counter`: Pointer to a 64-bit atomic counter that will be incremented by 1

## Dependencies
- Functions called/Symbols referenced:
  - AmStartupProcess (assertion check)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md) (atomic read operation)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md) (atomic write operation)
- Called from (representative examples):
  - [XLogPrefetcherNextBlock](XLogPrefetcherNextBlock.md) (multiple locations for different statistics)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the xlogprefetcher.c file and likely inlined at call sites for performance
- The function provides memory-safe increment operations on platforms where direct uint64 increment might not be atomic
- Used extensively by XLogPrefetcherNextBlock for tracking various prefetch statistics including blocks examined, skipped, and prefetched
- The assertion ensures proper execution context, as WAL prefetching is typically done by the startup process during recovery