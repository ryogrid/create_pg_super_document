# replorigin_advance

## Location
src/backend/replication/logical/origin.c: 888 - 1013

## Overview
Advances the replication progress for a specific replication origin, recording that a commit from a remote node has been successfully replayed locally.

## Definition


## Detailed Description
replorigin_advance is a core function that updates the replication progress tracking for a specific replication origin. It searches for or creates a replication state slot for the given origin, then updates the remote and local LSN positions to reflect successful replay of a transaction. The function handles concurrent access through lwlocks, supports both forward and backward LSN movement, and optionally logs the change to WAL for durability. It's essential for crash recovery and ensuring that replicated transactions aren't replayed multiple times.

## Parameters / Member Variables
- : RepOriginId identifying the replication origin
- : XLogRecPtr of the commit LSN on the remote node
- : XLogRecPtr of the local commit LSN
- : boolean allowing LSN to move backward (for special cases)
- : boolean indicating whether to write a WAL record for this change

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - ereport (ERROR level)
  - RepOriginId
  - ReplicationState (struct)
  - [xl_replorigin_set](../x/xl_replorigin_set.md) (struct)
  - InvalidRepOriginId
  - DoNotReplicateId
  - XLOG_REPLORIGIN_SET
  - RM_REPLORIGIN_ID
- Called from (representative examples):
  - [xact_redo_commit](../x/xact_redo_commit.md) (src/backend/access/transam/xact.c:6152)
  - [replorigin_redo](replorigin_redo.md) (src/backend/replication/logical/origin.c:838)
  - [pg_replication_origin_advance](../p/pg_replication_origin_advance.md) (src/backend/replication/logical/origin.c:1474)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md) (src/backend/replication/logical/tablesync.c:1487)

## Notes and Other Information
- Requires RowExclusiveLock on pg_replication_origin unless running in recovery
- Skips processing for DoNotReplicateId to avoid unnecessary tracking
- Uses ReplicationOriginLock for concurrent access protection
- Handles race conditions during checkpoints by allowing older values in certain scenarios
- Creates new replication state slots when needed, respecting max_replication_slots limit
- WAL logging is conditional - typically disabled during recovery replay but enabled for direct API calls
- Critical for maintaining replication consistency and preventing duplicate transaction replay