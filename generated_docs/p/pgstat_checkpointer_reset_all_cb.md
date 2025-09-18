# pgstat_checkpointer_reset_all_cb

## Location
[src/backend/utils/activity/pgstat_checkpointer.c:88-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_checkpointer.c#L88-L102)

## Overview
Resets all checkpointer statistics by copying current statistics to the reset offset and updating the reset timestamp.

## Definition
```c
void pgstat_checkpointer_reset_all_cb(TimestampTz ts)
```

## Detailed Description
This function implements the statistics reset callback for checkpointer statistics. It follows the PostgreSQL statistics reset protocol by acquiring an exclusive lock on the shared checkpointer statistics, copying the current statistics to the reset_offset using the change-counted statistics mechanism, updating the reset timestamp, and releasing the lock. This allows the statistics system to provide delta values by subtracting the reset_offset from current values, enabling proper statistics reset functionality without losing data consistency.

## Parameters / Member Variables
- `ts`: The timestamp to set as the new stat_reset_timestamp, indicating when the statistics were reset

## Dependencies
- Functions called/Symbols referenced:
  - PgStatShared_Checkpointer (structure type)
  - [pgstat_copy_changecounted_stats](pgstat_copy_changecounted_stats.md)
  - LWLockAcquire (implicitly via lock operations)
  - LWLockRelease (implicitly via lock operations)
- Called from (representative examples):
  - SH_DECLARE (hash table declaration context)

## Notes and Other Information
- Located at src/backend/utils/activity/pgstat_checkpointer.c:88-102
- Uses the change-counted statistics protocol for atomic updates as explained in the PgStatShared_Checkpointer documentation
- Acquires exclusive lock (`LW_EXCLUSIVE`) on `stats_shmem->lock` to ensure atomic reset operation
- The reset protocol copies current stats to reset_offset, allowing the statistics system to calculate deltas
- After reset, reported statistics will show values relative to the reset point rather than absolute values since system start
- The function is part of the callback mechanism used by the statistics reset infrastructure