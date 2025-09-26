# smgr_redo

## Location
[src/backend/catalog/storage.c:965-1079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L965-L1079)

## Overview
smgr_redo processes Write-Ahead Log (WAL) records for storage manager operations during recovery, handling both relation creation and truncation operations to ensure data consistency.

## Definition
```c
void smgr_redo(XLogReaderState *record)
```

## Detailed Description
This function is a WAL replay handler that processes storage manager-related log records during PostgreSQL's crash recovery or streaming replication. It handles two main types of operations: relation creation (XLOG_SMGR_CREATE) and relation truncation (XLOG_SMGR_TRUNCATE).

For relation creation operations, the function extracts the relation locator information from the WAL record and recreates the relation using smgrcreate(). This ensures that relations created during the original transaction are properly recreated during recovery.

For truncation operations, the function performs a more complex sequence:
1. Forcibly creates the relation if it doesn't exist (handles cases where the relation was dropped later in the WAL sequence)
2. Updates the minimum recovery point by flushing WAL to ensure WAL-first rule compliance
3. Prepares truncation for multiple forks (MAIN, FSM, VM) based on the operation flags
4. Performs the actual truncation using smgrtruncate2() within a critical section
5. Updates Free Space Map pages to account for the truncation

The function ensures crash consistency by carefully ordering operations and maintaining the WAL-first rule, where WAL records must be flushed before corresponding data changes.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record being replayed, including operation type and data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo (extracts operation info from WAL record)
  - XLogRecGetData (extracts data payload from WAL record)
  - [smgropen](smgropen.md) (opens storage manager relation)
  - [smgrcreate](smgrcreate.md) (creates relation files)
  - [XLogFlush](../X/XLogFlush.md) (flushes WAL to ensure durability)
  - [smgrnblocks](smgrnblocks.md) (gets number of blocks in relation fork)
  - [XLogTruncateRelation](../X/XLogTruncateRelation.md) (notifies xlogutils about truncation)
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)/FreeFakeRelcacheEntry (temporary relation cache entries)
  - [FreeSpaceMapPrepareTruncateRel](../F/FreeSpaceMapPrepareTruncateRel.md) (prepares FSM for truncation)
  - [visibilitymap_prepare_truncate](../v/visibilitymap_prepare_truncate.md) (prepares visibility map for truncation)
  - [smgrtruncate2](smgrtruncate2.md) (performs actual truncation)
  - [FreeSpaceMapVacuumRange](../F/FreeSpaceMapVacuumRange.md) (updates FSM after truncation)

- Called from (representative examples):
  - This function is registered as a WAL redo handler and called by the WAL replay mechanism during recovery

## Notes and Other Information
- This function is critical for PostgreSQL's crash recovery and replication mechanisms
- Handles two operation types: XLOG_SMGR_CREATE and XLOG_SMGR_TRUNCATE
- Implements proper WAL-first rule compliance by flushing WAL before truncation
- Uses critical sections during truncation to ensure atomic operations
- Supports truncation of multiple relation forks (main data, free space map, visibility map)
- Forcibly recreates relations during replay to handle out-of-order WAL processing
- The function panics on unknown operation codes to ensure data integrity
- Plays a crucial role in maintaining storage consistency across system failures and recovery scenarios