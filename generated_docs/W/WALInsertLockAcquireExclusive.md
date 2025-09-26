# WALInsertLockAcquireExclusive

## Location
[src/backend/access/transam/xlog.c:1418-1446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L1418-L1446)

## Overview
Acquires all WAL insertion locks in exclusive mode to prevent other backends from inserting records to the Write-Ahead Log during critical operations.

## Definition
```c
static void WALInsertLockAcquireExclusive(void)
```

## Detailed Description
This function provides a mechanism to temporarily halt all WAL insertion activity across the entire PostgreSQL instance by acquiring all WAL insertion locks exclusively. It's used during critical operations that require exclusive access to the WAL, such as checkpoint creation, backup operations, and recovery-related tasks.

The function implements a special optimization: when holding all locks, it sets the insertingAt indicator of all but the last lock to PG_UINT64_MAX (0xFFFFFFFFFFFFFFFF). This high value ensures that no other processes will block waiting on these locks, as it's higher than any real XLogRecPtr value that could be encountered.

The function sets a global flag `holdingAllLocks` to true, which other parts of the system can check to determine if exclusive WAL access is currently in effect.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (acquires individual WAL insertion locks)
  - [LWLockUpdateVar](../L/LWLockUpdateVar.md) (updates the insertingAt variable for each lock)
  - NUM_XLOGINSERT_LOCKS (constant defining number of WAL insertion locks)
  - PG_UINT64_MAX (maximum value for insertingAt indicator)
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [XLogInsertRecord](../X/XLogInsertRecord.md)
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [CreateEndOfRecoveryRecord](../C/CreateEndOfRecoveryRecord.md)
  - [do_pg_backup_start](../d/do_pg_backup_start.md)
  - [do_pg_backup_stop](../d/do_pg_backup_stop.md)

## Notes and Other Information
- This is a static function, only accessible within the xlog.c module
- Must be paired with WALInsertLockRelease() to release the acquired locks
- The insertingAt variable for the last lock is reset to 0 upon release
- Used primarily during backup operations, checkpoints, and other system-wide WAL coordination tasks
- The function blocks until all WAL insertion locks can be acquired exclusively