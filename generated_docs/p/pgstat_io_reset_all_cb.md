# pgstat_io_reset_all_cb

## Location
[src/backend/utils/activity/pgstat_io.c:255-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_io.c#L255-L276)

## Overview
Resets all I/O statistics across all backend types and updates the reset timestamp.

## Definition

```c
struct assignment due to better type safety */
		*bktype_snap = *bktype_shstats;
```
## Detailed Description
This function serves as a callback to reset all I/O statistics stored in shared memory across all backend types. It iterates through each backend type, acquires the appropriate exclusive lock, and clears the statistics data using memset. The function also updates the statistics reset timestamp using the first backend type's lock for synchronization. This ensures that all I/O statistics are atomically reset and the reset time is properly recorded for reference.

## Parameters / Member Variables
- `ts`: Timestamp value to set as the new statistics reset timestamp

## Dependencies
- Functions called/Symbols referenced:
  - BACKEND_NUM_TYPES
  - [LWLock](../L/LWLock.md)
  - [PgStat_BktypeIO](../P/PgStat_BktypeIO.md)
- Called from (representative examples):
  - SH_DECLARE (as part of statistics system callbacks)

## Notes and Other Information
- Uses exclusive lightweight locks to ensure thread-safe reset operations
- Resets statistics for all backend types in a loop from 0 to BACKEND_NUM_TYPES-1
- Sets the reset timestamp only when processing the first backend type (i == 0)
- Clears all statistics data structures using memset to zero them out completely
- This is a callback function used by PostgreSQL's statistics reset infrastructure
- Located in src/backend/utils/activity/pgstat_io.c:255-276

## Simplified Source

```c
void pgstat_io_reset_all_cb(TimestampTz ts)
{
    // Reset I/O statistics for all backend types
    for (int i = 0; i < BACKEND_NUM_TYPES; i++)
    {
        LWLock *lock = &pgStatLocal.shmem->io.locks[i];
        PgStat_BktypeIO *stats = &pgStatLocal.shmem->io.stats.stats[i];

        LWLockAcquire(lock, LW_EXCLUSIVE);

        // Set reset timestamp using first backend type's lock
        if (i == 0)
            pgStatLocal.shmem->io.stats.stat_reset_timestamp = ts;

        // Clear all statistics data
        memset(stats, 0, sizeof(*stats));
        LWLockRelease(lock);
    }
}
```