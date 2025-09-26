# lwlock_stats

## Location
[src/backend/storage/lmgr/lwlock.c:247-255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L247-L255)

## Overview
lwlock_stats is a structure that stores comprehensive performance statistics for individual lightweight locks, including acquisition counts, blocking events, and timing metrics.

## Definition
```c
typedef struct lwlock_stats
{
    lwlock_stats_key key;
    int             sh_acquire_count;
    int             ex_acquire_count;
    int             block_count;
    int             dequeue_self_count;
    int             spin_delay_count;
} lwlock_stats;
```

## Detailed Description
lwlock_stats is the main structure for tracking detailed performance statistics of lightweight locks in PostgreSQL. This structure captures various metrics that are crucial for performance analysis and debugging of lock contention issues. Each instance of this structure corresponds to a specific lock (identified by the key) and maintains counters for different types of lock operations and events.

The structure provides insight into lock usage patterns, contention levels, and performance characteristics. It distinguishes between shared and exclusive lock acquisitions, tracks blocking events when processes must wait for locks, and monitors self-dequeue operations and spin delays. This information is valuable for database administrators and developers to identify performance bottlenecks and optimize lock usage.

## Parameters / Member Variables
- `key`: lwlock_stats_key structure that uniquely identifies the lock being tracked (contains tranche and instance)
- `sh_acquire_count`: Counter for the number of times this lock was acquired in shared mode
- `ex_acquire_count`: Counter for the number of times this lock was acquired in exclusive mode
- `block_count`: Counter for the number of times processes were blocked waiting for this lock
- `dequeue_self_count`: Counter for the number of times a process removed itself from this lock's wait queue
- `spin_delay_count`: Counter for the number of spin delay operations performed while waiting for this lock

## Dependencies
- Functions called/Symbols referenced:
  - lwlock_stats_key (embedded structure for lock identification)
- Called from (representative examples):
  - LOG_LWDEBUG
  - init_lwlock_stats
  - print_lwlock_stats
  - get_lwlock_stats_entry
  - LWLockWaitListLock
  - LWLockDequeueSelf
  - LWLockAcquire
  - LWLockAcquireOrWait
  - LWLockWaitForVar

## Notes and Other Information
- This structure is central to PostgreSQL's lock performance monitoring system
- Statistics are collected only when lock debugging or statistics tracking is enabled
- The counters help identify heavily contended locks and performance bottlenecks
- Used extensively throughout the LWLock subsystem for performance analysis
- Defined in src/backend/storage/lmgr/lwlock.c at lines 247-255
- The structure supports detailed performance profiling of the lightweight lock subsystem
- Different counter types allow analysis of various aspects of lock behavior and contention patterns