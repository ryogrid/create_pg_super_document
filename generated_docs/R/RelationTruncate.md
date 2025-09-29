# RelationTruncate

## Location
[src/backend/catalog/storage.c:288-448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L288-L448)

## Overview
RelationTruncate physically truncates a relation to a specified number of blocks, handling WAL logging, buffer management, and coordination with auxiliary structures like FSM and visibility map.

## Definition
```c
void RelationTruncate(Relation rel, BlockNumber nblocks)
```

## Detailed Description
RelationTruncate is responsible for physically truncating a PostgreSQL relation (table) to a specified number of blocks. This is a complex operation that involves multiple components:

1. **Buffer Management**: Clears any cached block information and invalidates buffers for blocks that will be removed
2. **Multi-Fork Handling**: Truncates not just the main relation fork, but also associated structures:
   - Main fork (MAIN_FORKNUM) - the actual table data
   - Free Space Map (FSM_FORKNUM) - tracks free space in pages
   - Visibility Map (VISIBILITYMAP_FORKNUM) - tracks page visibility for VACUUM optimization
3. **WAL Logging**: Creates XLOG_SMGR_TRUNCATE WAL records when the relation requires WAL logging
4. **Checkpoint Coordination**: Uses DELAY_CHKPT_START and DELAY_CHKPT_COMPLETE flags to ensure proper ordering with concurrent checkpoints
5. **Critical Section Protection**: Executes the actual truncation within a critical section to ensure atomicity

The function ensures data consistency by WAL-logging the truncation before performing it, and uses checkpoint delay mechanisms to prevent race conditions with concurrent checkpoint operations.

## Parameters / Member Variables
- `rel`: The Relation to be truncated
- `nblocks`: The target number of blocks after truncation

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetSmgr](RelationGetSmgr.md)
  - [smgrnblocks](../s/smgrnblocks.md)
  - [smgrexists](../s/smgrexists.md)
  - [FreeSpaceMapPrepareTruncateRel](../F/FreeSpaceMapPrepareTruncateRel.md)
  - [visibilitymap_prepare_truncate](../v/visibilitymap_prepare_truncate.md)
  - [RelationPreTruncate](RelationPreTruncate.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogFlush](../X/XLogFlush.md)
  - [smgrtruncate2](../s/smgrtruncate2.md)
  - [FreeSpaceMapVacuumRange](../F/FreeSpaceMapVacuumRange.md)
- Called from (representative examples):
  - [heapam_relation_nontransactional_truncate](../h/heapam_relation_nontransactional_truncate.md)
  - [lazy_truncate_heap](../l/lazy_truncate_heap.md)
  - [spgvacuumscan](../s/spgvacuumscan.md)
  - [RelationTruncateIndexes](RelationTruncateIndexes.md)

## Notes and Other Information
- The function operates in a critical section to prevent interrupts during the truncation process
- Checkpoint delay flags are used to ensure proper coordination with concurrent checkpoints
- FSM vacuum is performed after truncation to update upper-level FSM pages
- The operation handles multiple relation forks simultaneously for efficiency
- WAL flush is performed immediately after logging to ensure durability before physical truncation

## Simplified Source

```c
void
RelationTruncate(Relation rel, BlockNumber nblocks)
{
    bool fsm, vm;
    bool need_fsm_vacuum = false;
    ForkNumber forks[MAX_FORKNUM];
    BlockNumber old_blocks[MAX_FORKNUM];
    BlockNumber blocks[MAX_FORKNUM];
    int nforks = 0;
    SMgrRelation reln;

    // Clear cached block information
    reln = RelationGetSmgr(rel);
    reln->smgr_targblock = InvalidBlockNumber;
    for (int i = 0; i <= MAX_FORKNUM; ++i)
        reln->smgr_cached_nblocks[i] = InvalidBlockNumber;

    // Prepare truncation for main fork
    forks[nforks] = MAIN_FORKNUM;
    old_blocks[nforks] = smgrnblocks(reln, MAIN_FORKNUM);
    blocks[nforks] = nblocks;
    nforks++;

    // Prepare FSM truncation if it exists
    fsm = smgrexists(RelationGetSmgr(rel), FSM_FORKNUM);
    if (fsm) {
        blocks[nforks] = FreeSpaceMapPrepareTruncateRel(rel, nblocks);
        if (BlockNumberIsValid(blocks[nforks])) {
            forks[nforks] = FSM_FORKNUM;
            old_blocks[nforks] = smgrnblocks(reln, FSM_FORKNUM);
            nforks++;
            need_fsm_vacuum = true;
        }
    }

    // Prepare visibility map truncation if it exists
    vm = smgrexists(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM);
    if (vm) {
        blocks[nforks] = visibilitymap_prepare_truncate(rel, nblocks);
        if (BlockNumberIsValid(blocks[nforks])) {
            forks[nforks] = VISIBILITYMAP_FORKNUM;
            old_blocks[nforks] = smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
            nforks++;
        }
    }

    RelationPreTruncate(rel);

    // Delay checkpoints to ensure proper ordering
    MyProc->delayChkptFlags |= DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE;

    // Critical section: WAL log then truncate atomically
    START_CRIT_SECTION();

    // Write WAL record if relation requires WAL logging
    if (RelationNeedsWAL(rel)) {
        XLogRecPtr lsn;
        xl_smgr_truncate xlrec;

        xlrec.blkno = nblocks;
        xlrec.rlocator = rel->rd_locator;
        xlrec.flags = SMGR_TRUNCATE_ALL;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, sizeof(xlrec));
        lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE);

        // Flush WAL to ensure durability before physical truncation
        XLogFlush(lsn);
    }

    // Perform the actual truncation (removes buffers and truncates files)
    smgrtruncate2(RelationGetSmgr(rel), forks, nforks, old_blocks, blocks);

    END_CRIT_SECTION();

    // Re-enable checkpoints
    MyProc->delayChkptFlags &= ~(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE);

    // Update FSM pages if needed
    if (need_fsm_vacuum)
        FreeSpaceMapVacuumRange(rel, nblocks, InvalidBlockNumber);
}
```