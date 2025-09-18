# LogAccessExclusiveLocks

## Location
src/backend/storage/ipc/standby.c: 1405 - 1422

## Overview
Logs multiple AccessExclusive locks in bulk to the Write-Ahead Log (WAL) for standby server replication.

## Definition


## Detailed Description
LogAccessExclusiveLocks performs wholesale logging of AccessExclusive locks into the WAL. This function is designed to efficiently log multiple AccessExclusive locks in a single WAL record rather than logging them individually. Only AccessExclusive locks need to be logged for standby servers, as other lock types do not conflict with read-only queries that standby servers typically execute.

The function constructs an xl_standby_locks WAL record containing the lock count and an array of lock descriptors, then inserts it into the WAL with the XLOG_STANDBY_LOCK record type. The record is marked as unimportant for durability since it's primarily used for Hot Standby conflict resolution rather than crash recovery.

## Parameters / Member Variables
- : The number of AccessExclusive locks to be logged
- : An array of xl_standby_lock structures containing the lock information to be recorded

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [XLogInsert](../X/XLogInsert.md)
  - XLOG_MARK_UNIMPORTANT
  - XLOG_STANDBY_LOCK
  - xl_standby_locks
  - [xl_standby_lock](../x/xl_standby_lock.md)
- Called from (representative examples):
  - LogStandbySnapshot
  - [LogAccessExclusiveLock](LogAccessExclusiveLock.md)

## Notes and Other Information
- Only AccessExclusive locks are logged because other lock types do not cause conflicts with read-only queries on standby servers
- The function uses bulk logging for efficiency when multiple locks need to be recorded
- Records are marked with XLOG_MARK_UNIMPORTANT to avoid unnecessary checkpoint/archiving overhead
- The logging mechanism is part of the Hot Standby conflict resolution system
- More details about lock logging rationale can be found in backend/storage/lmgr/README
- Located in src/backend/storage/ipc/standby.c:1405-1422