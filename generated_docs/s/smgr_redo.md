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
- `*record`: XLogReaderState pointer containing the WAL record being replayed, including operation type and data
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

## Simplified Source

```c
void smgr_redo(XLogReaderState *record) {
    XLogRecPtr lsn = record->EndRecPtr;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    // Assert that backup blocks are not used in smgr records
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_SMGR_CREATE) {
        // Handle relation creation during recovery
        xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(record);
        SMgrRelation reln = smgropen(xlrec->rlocator, INVALID_PROC_NUMBER);
        smgrcreate(reln, xlrec->forkNum, true);
    }
    else if (info == XLOG_SMGR_TRUNCATE) {
        // Handle relation truncation during recovery
        xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
        SMgrRelation reln = smgropen(xlrec->rlocator, INVALID_PROC_NUMBER);

        // Forcibly create relation if it doesn't exist
        smgrcreate(reln, MAIN_FORKNUM, true);

        // Ensure WAL-first rule: flush WAL before truncation
        XLogFlush(lsn);

        // Prepare truncation for different forks
        ForkNumber forks[MAX_FORKNUM];
        BlockNumber blocks[MAX_FORKNUM], old_blocks[MAX_FORKNUM];
        int nforks = 0;
        bool need_fsm_vacuum = false;

        // Prepare main fork truncation
        if (xlrec->flags & SMGR_TRUNCATE_HEAP) {
            forks[nforks] = MAIN_FORKNUM;
            old_blocks[nforks] = smgrnblocks(reln, MAIN_FORKNUM);
            blocks[nforks] = xlrec->blkno;
            nforks++;
            XLogTruncateRelation(xlrec->rlocator, MAIN_FORKNUM, xlrec->blkno);
        }

        // Prepare FSM and VM fork truncation if needed
        Relation rel = CreateFakeRelcacheEntry(xlrec->rlocator);

        if ((xlrec->flags & SMGR_TRUNCATE_FSM) && smgrexists(reln, FSM_FORKNUM)) {
            blocks[nforks] = FreeSpaceMapPrepareTruncateRel(rel, xlrec->blkno);
            if (BlockNumberIsValid(blocks[nforks])) {
                forks[nforks] = FSM_FORKNUM;
                old_blocks[nforks] = smgrnblocks(reln, FSM_FORKNUM);
                nforks++;
                need_fsm_vacuum = true;
            }
        }

        if ((xlrec->flags & SMGR_TRUNCATE_VM) && smgrexists(reln, VISIBILITYMAP_FORKNUM)) {
            blocks[nforks] = visibilitymap_prepare_truncate(rel, xlrec->blkno);
            if (BlockNumberIsValid(blocks[nforks])) {
                forks[nforks] = VISIBILITYMAP_FORKNUM;
                old_blocks[nforks] = smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
                nforks++;
            }
        }

        // Perform the actual truncation atomically
        if (nforks > 0) {
            START_CRIT_SECTION();
            smgrtruncate2(reln, forks, nforks, old_blocks, blocks);
            END_CRIT_SECTION();
        }

        // Update FSM after truncation if needed
        if (need_fsm_vacuum) {
            FreeSpaceMapVacuumRange(rel, xlrec->blkno, InvalidBlockNumber);
        }

        FreeFakeRelcacheEntry(rel);
    }
    else {
        elog(PANIC, "smgr_redo: unknown op code %u", info);
    }
}
```