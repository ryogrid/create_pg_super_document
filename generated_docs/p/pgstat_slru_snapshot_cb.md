# pgstat_slru_snapshot_cb

## Location
[src/backend/utils/activity/pgstat_slru.c:203-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L203-L219)

## Overview
A callback function that creates a snapshot of current SLRU statistics by copying them from shared memory to local statistics storage under appropriate locking.

## Definition

```c
void
pgstat_slru_snapshot_cb(void)
```
## Detailed Description
This function serves as a callback for taking a consistent snapshot of SLRU statistics. It acquires a shared lock on the SLRU statistics in shared memory, copies the entire statistics structure to the local snapshot area, and then releases the lock. This ensures that the snapshot represents a consistent point-in-time view of all SLRU statistics across all types. The shared lock allows multiple processes to take snapshots simultaneously without blocking each other, while preventing inconsistent reads during updates.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  -  (shared memory structure for SLRU stats)
  -  (lightweight lock mode constant)
  -  (acquire lightweight lock)
  -  (release lightweight lock)
  -  (memory copy function)
  -  (shared memory reference)
  -  (local snapshot storage)
- Called from (representative examples):
  -  at src/backend/utils/activity/pgstat.c:382

## Notes and Other Information
- This is a callback function registered with the PostgreSQL statistics system
- Uses shared locking to allow concurrent snapshot operations while maintaining consistency
- The snapshot provides a point-in-time view that remains stable for the duration of queries or operations that need consistent statistics
- Critical for ensuring accurate reporting of SLRU statistics to monitoring tools and system views

## Simplified Source

```c
void pgstat_slru_snapshot_cb(void)
{
    PgStatShared_SLRU *shared_stats = &pgStatLocal.shmem->slru;

    // Take shared lock for consistent read
    LWLockAcquire(&shared_stats->lock, LW_SHARED);

    // Copy all SLRU statistics to local snapshot
    memcpy(pgStatLocal.snapshot.slru, &shared_stats->stats,
           sizeof(shared_stats->stats));

    LWLockRelease(&shared_stats->lock);
}
```