# LogAccessExclusiveLocks

## Location
[src/backend/storage/ipc/standby.c:1405-1422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1405-L1422)

## Overview
Logs multiple AccessExclusive locks in bulk to the Write-Ahead Log (WAL) for standby server replication.

## Definition

```c
static void
LogAccessExclusiveLocks(int nlocks, xl_standby_lock *locks)
```
## Detailed Description
LogAccessExclusiveLocks performs wholesale logging of AccessExclusive locks into the WAL. This function is designed to efficiently log multiple AccessExclusive locks in a single WAL record rather than logging them individually. Only AccessExclusive locks need to be logged for standby servers, as other lock types do not conflict with read-only queries that standby servers typically execute.

The function constructs an xl_standby_locks WAL record containing the lock count and an array of lock descriptors, then inserts it into the WAL with the XLOG_STANDBY_LOCK record type. The record is marked as unimportant for durability since it's primarily used for Hot Standby conflict resolution rather than crash recovery.

## Parameters / Member Variables
- `nlocks`: The number of AccessExclusive locks to be logged
- `*locks`: An array of xl_standby_lock structures containing the lock information to be recorded
## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [XLogInsert](../X/XLogInsert.md)
  - XLOG_MARK_UNIMPORTANT
  - XLOG_STANDBY_LOCK
  - [xl_standby_locks](../x/xl_standby_locks.md)
  - [xl_standby_lock](../x/xl_standby_lock.md)
- Called from (representative examples):
  - [LogStandbySnapshot](LogStandbySnapshot.md)
  - [LogAccessExclusiveLock](LogAccessExclusiveLock.md)

## Notes and Other Information
- Only AccessExclusive locks are logged because other lock types do not cause conflicts with read-only queries on standby servers
- The function uses bulk logging for efficiency when multiple locks need to be recorded
- Records are marked with XLOG_MARK_UNIMPORTANT to avoid unnecessary checkpoint/archiving overhead
- The logging mechanism is part of the Hot Standby conflict resolution system
- More details about lock logging rationale can be found in backend/storage/lmgr/README
- Located in src/backend/storage/ipc/standby.c:1405-1422

## Simplified Source

```c
// Simplified version of LogAccessExclusiveLocks
static void LogAccessExclusiveLocks(int nlocks, xl_standby_lock *locks) {
    // Step 1: Create WAL record structure
    xl_standby_locks xlrec;
    xlrec.nlocks = nlocks;

    // Step 2: Begin WAL record construction
    XLogBeginInsert();

    // Step 3: Register the record header (lock count)
    XLogRegisterData((char *) &xlrec, offsetof(xl_standby_locks, locks));

    // Step 4: Register the actual lock data array
    XLogRegisterData((char *) locks, nlocks * sizeof(xl_standby_lock));

    // Step 5: Mark record as unimportant for performance
    XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);

    // Step 6: Insert the complete record into WAL
    XLogInsert(RM_STANDBY_ID, XLOG_STANDBY_LOCK);
}
```

Key simplifications made:
- Added step-by-step comments explaining the WAL record construction process
- Clarified the purpose of each XLogRegisterData call (header vs. data)
- Explained why XLOG_MARK_UNIMPORTANT flag is used
- Maintained the exact original logic flow without any functional changes
- Focused on the core workflow: create record structure → register data → insert to WAL