# heap_xlog_prune_freeze

## Location
src/backend/access/heap/heapam.c: 9211 - 9362

## Overview
Replays XLOG_HEAP2_PRUNE_* WAL records during recovery, handling both tuple pruning and freezing operations on heap pages.

## Definition


## Detailed Description
The  function is a critical WAL replay function that reconstructs the state of heap pages after prune and freeze operations during PostgreSQL recovery. It processes complex WAL records containing information about line pointer redirections, dead tuples, unused items, and tuple freezing plans.

The function handles multiple aspects of page recovery:
- Resolves recovery conflicts in Hot Standby mode using snapshot conflict horizons
- Manages different lock types (cleanup vs. exclusive) based on WAL record flags
- Deserializes prune and freeze information from WAL data
- Executes line pointer updates (redirections, marking dead/unused items)
- Applies tuple freezing plans to update transaction IDs and infomasks
- Updates the free space map when space is reclaimed

The function is designed to work with or without full-page images and ensures proper recovery semantics for vacuum operations.

## Parameters / Member Variables
- : XLogReaderState containing the WAL record to replay

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - XLogReadBufferForRedoExtended
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [ResolveRecoveryConflictWithSnapshot](../R/ResolveRecoveryConflictWithSnapshot.md)
  - [heap_xlog_deserialize_prune_and_freeze](heap_xlog_deserialize_prune_and_freeze.md)
  - [heap_page_prune_execute](heap_page_prune_execute.md)
  - [heap_execute_freeze_tuple](heap_execute_freeze_tuple.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md)
  - MarkBufferDirty
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - XLogRecordPageWithFreeSpace
  - [xl_heap_prune](../x/xl_heap_prune.md) (WAL record structure)
  - [xlhp_freeze_plan](../x/xlhp_freeze_plan.md)
  - [HeapTupleFreeze](../H/HeapTupleFreeze.md)
  - Various XLHP_* flags
  - BLK_NEEDS_REDO
  - RBM_NORMAL
- Called from:
  - [heap2_redo](heap2_redo.md)

## Notes and Other Information
- This function is static and only used internally within heapam.c for WAL replay
- Handles both pruning (removing dead tuples) and freezing (updating old transaction IDs) in a single operation
- Ensures proper recovery conflict handling in Hot Standby mode by checking snapshot conflict horizons
- Manages different buffer lock types based on whether cleanup locks are required
- Asserts that certain operations requiring tuple movement only occur with cleanup locks
- Processes variable-length WAL data containing offset arrays and freeze plans
- Updates the free space map only when space is actually reclaimed to maintain FSM accuracy
- Does not update page prunability hints during recovery, allowing natural hint bit updates during normal operation
- Supports partial page recovery when full-page images are not available
- Critical for maintaining data consistency during crash recovery and standby replay