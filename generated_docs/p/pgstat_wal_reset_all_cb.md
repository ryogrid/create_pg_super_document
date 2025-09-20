# pgstat_wal_reset_all_cb

## Location
[src/backend/utils/activity/pgstat_wal.c:167-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_wal.c#L167-L177)

## Overview
This function resets all WAL (Write-Ahead Log) statistics to zero values and sets the statistics reset timestamp in the shared memory statistics area.

## Definition

```c
void
pgstat_wal_reset_all_cb(TimestampTz ts)
```
## Detailed Description
 is a callback function that handles the reset of all WAL-related statistics in PostgreSQL's shared memory statistics system. The function operates on the shared WAL statistics structure by acquiring an exclusive lock, zeroing out all statistical counters, and setting the reset timestamp to the provided value. This ensures thread-safe access to the shared statistics while performing a complete reset operation.

The function is part of PostgreSQL's statistics collection framework and is designed to be used as a callback in the statistics reset mechanism. It provides a clean slate for WAL statistics collection by clearing all accumulated counter values.

## Parameters / Member Variables
- : A TimestampTz value representing the timestamp when the statistics reset occurred. This timestamp is stored in the statistics structure to track when the last reset happened.

## Dependencies
- Functions called/Symbols referenced:
  - [PgStatShared_Wal](../P/PgStatShared_Wal.md) (structure type)
  - LWLockAcquire (for exclusive lock acquisition)
  - memset (for zeroing statistics structure)
  - LWLockRelease (for lock release)
  - pgStatLocal.shmem->wal (shared memory statistics access)

- Called from (representative examples):
  - Statistics reset system via SH_DECLARE mechanism (src/backend/utils/activity/pgstat.c:390)

## Notes and Other Information
- The function uses LW_EXCLUSIVE lock mode to ensure exclusive access while modifying the shared statistics
- All WAL statistics counters (records, FPI, bytes, buffers_full, write, sync, write_time, sync_time) are reset to zero
- The reset timestamp allows tracking of when statistics collection was last restarted
- This callback is part of the PostgreSQL statistics collection framework's modular design
- Located in src/backend/utils/activity/pgstat_wal.c:167-177