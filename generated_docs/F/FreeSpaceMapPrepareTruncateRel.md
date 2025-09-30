# FreeSpaceMapPrepareTruncateRel

## Location
[src/backend/storage/freespace/freespace.c:275-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L275-L357)

## Overview
FreeSpaceMapPrepareTruncateRel prepares the Free Space Map (FSM) for truncation when a relation is being shortened, returning the new FSM size and ensuring FSM consistency during the truncation process.

## Definition
BlockNumber FreeSpaceMapPrepareTruncateRel(Relation rel, BlockNumber nblocks)

## Detailed Description
This function is called during relation truncation operations to prepare the Free Space Map for the new relation size. When a relation is truncated to nblocks blocks, the FSM must be updated to reflect that heap blocks beyond nblocks no longer exist. The function calculates the new FSM size, zeros out slots that correspond to removed heap blocks, and logs the changes for WAL recovery.

The function handles two main scenarios:
1. When the first removed heap block is not at a page boundary (first_removed_slot > 0), it zeros out the tail portion of the last remaining FSM page
2. When the first removed heap block is at a page boundary (first_removed_slot == 0), it can truncate entire FSM pages

The function uses critical sections and WAL logging to ensure crash safety during the truncation process.

## Parameters / Member Variables
- : The relation whose FSM is being prepared for truncation
- : The new size of the heap (number of blocks the relation will have after truncation)

## Dependencies
- Functions called/Symbols referenced:
  - [smgrexists](../s/smgrexists.md) (checks if FSM fork exists)
  - [RelationGetSmgr](../R/RelationGetSmgr.md) (gets storage manager for relation)
  - [fsm_get_location](../f/fsm_get_location.md) (gets FSM address for a heap block)
  - [fsm_readbuf](../f/fsm_readbuf.md) (reads FSM buffer)
  - [fsm_truncate_avail](../f/fsm_truncate_avail.md) (zeros out FSM slots)
  - [fsm_logical_to_physical](../f/fsm_logical_to_physical.md) (converts logical FSM address to physical block number)
  - [log_newpage_buffer](../l/log_newpage_buffer.md) (logs full page image for WAL)
  - [smgrnblocks](../s/smgrnblocks.md) (gets number of blocks in storage manager)
- Called from (representative examples):
  - [RelationTruncate](../R/RelationTruncate.md) (src/backend/catalog/storage.c:318)
  - [smgr_redo](../s/smgr_redo.md) (src/backend/catalog/storage.c:1037)

## Notes and Other Information
- Returns InvalidBlockNumber if there is nothing to truncate (no FSM exists or FSM is already smaller than required)
- Uses critical sections to ensure atomicity of FSM modifications
- WAL logs the changes using log_newpage_buffer when appropriate to maintain consistency
- The caller is responsible for actually truncating the FSM pages using smgrtruncate() and updating upper-level FSM pages using FreeSpaceMapVacuumRange()
- Located in src/backend/storage/freespace/freespace.c:263-349

## Simplified Source

```c
BlockNumber
FreeSpaceMapPrepareTruncateRel(Relation rel, BlockNumber nblocks)
{
    BlockNumber new_nfsmblocks;
    FSMAddress first_removed_address;
    uint16 first_removed_slot;
    Buffer buf;

    // Exit if no FSM exists
    if (!smgrexists(RelationGetSmgr(rel), FSM_FORKNUM))
        return InvalidBlockNumber;

    // Find location of first removed heap block in FSM
    first_removed_address = fsm_get_location(nblocks, &first_removed_slot);

    // Zero out tail of last remaining FSM page if not at page boundary
    if (first_removed_slot > 0) {
        buf = fsm_readbuf(rel, first_removed_address, false);
        if (!BufferIsValid(buf))
            return InvalidBlockNumber;

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        START_CRIT_SECTION();

        // Clear slots for truncated heap blocks
        fsm_truncate_avail(BufferGetPage(buf), first_removed_slot);
        MarkBufferDirty(buf);

        // WAL log for crash recovery
        if (!InRecovery && RelationNeedsWAL(rel) && XLogHintBitIsNeeded())
            log_newpage_buffer(buf, false);

        END_CRIT_SECTION();
        UnlockReleaseBuffer(buf);

        new_nfsmblocks = fsm_logical_to_physical(first_removed_address) + 1;
    } else {
        // Truncation is at page boundary
        new_nfsmblocks = fsm_logical_to_physical(first_removed_address);
        if (smgrnblocks(RelationGetSmgr(rel), FSM_FORKNUM) <= new_nfsmblocks)
            return InvalidBlockNumber;
    }

    return new_nfsmblocks;
}
```