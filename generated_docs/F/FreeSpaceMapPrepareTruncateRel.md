# FreeSpaceMapPrepareTruncateRel

## Location
src/backend/storage/freespace/freespace.c: 275 - 357

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
  - smgrexists (checks if FSM fork exists)
  - RelationGetSmgr (gets storage manager for relation)
  - fsm_get_location (gets FSM address for a heap block)
  - fsm_readbuf (reads FSM buffer)
  - fsm_truncate_avail (zeros out FSM slots)
  - fsm_logical_to_physical (converts logical FSM address to physical block number)
  - log_newpage_buffer (logs full page image for WAL)
  - smgrnblocks (gets number of blocks in storage manager)
- Called from (representative examples):
  - RelationTruncate (src/backend/catalog/storage.c:318)
  - smgr_redo (src/backend/catalog/storage.c:1037)

## Notes and Other Information
- Returns InvalidBlockNumber if there is nothing to truncate (no FSM exists or FSM is already smaller than required)
- Uses critical sections to ensure atomicity of FSM modifications
- WAL logs the changes using log_newpage_buffer when appropriate to maintain consistency
- The caller is responsible for actually truncating the FSM pages using smgrtruncate() and updating upper-level FSM pages using FreeSpaceMapVacuumRange()
- Located in src/backend/storage/freespace/freespace.c:263-349