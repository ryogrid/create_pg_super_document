# XLogRecordPageWithFreeSpace

## Location
[src/backend/storage/freespace/freespace.c:211-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L211-L243)

## Overview
XLogRecordPageWithFreeSpace is a specialized FSM function designed for WAL (Write-Ahead Log) replay operations that updates free space information using physical file locations rather than relation objects.

## Definition
```c
void XLogRecordPageWithFreeSpace(RelFileLocator rlocator, BlockNumber heapBlk, Size spaceAvail)
```

## Detailed Description
This function serves the same fundamental purpose as RecordPageWithFreeSpace but is specifically designed for use during WAL replay operations. Instead of working with a Relation object, it operates directly with a RelFileLocator to identify the target relation. The function handles the low-level details of accessing and updating FSM pages during recovery, including buffer management, page initialization for new pages, and proper locking.

The function follows a careful protocol for buffer management: it extends the FSM if necessary, locks the buffer exclusively, initializes new pages if needed, updates the free space information, and properly marks the buffer as dirty if changes were made. The use of MarkBufferDirtyHint with a false parameter indicates this is a hint-based update that doesn't require full WAL logging.

## Parameters / Member Variables
- `rlocator`: RelFileLocator identifying the relation (used instead of Relation object during WAL replay)
- `heapBlk`: The block number of the heap page whose free space is being recorded
- `spaceAvail`: The actual amount of free space available on the page in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [fsm_space_avail_to_cat](../f/fsm_space_avail_to_cat.md)
  - FSMAddress
  - [fsm_get_location](../f/fsm_get_location.md)
  - [fsm_logical_to_physical](../f/fsm_logical_to_physical.md)
  - [XLogReadBufferExtended](XLogReadBufferExtended.md)
  - FSM_FORKNUM
  - RBM_ZERO_ON_ERROR
  - BUFFER_LOCK_EXCLUSIVE
  - [PageIsNew](../P/PageIsNew.md)
  - [PageInit](../P/PageInit.md)
  - [fsm_set_avail](../f/fsm_set_avail.md)
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)
- Called from (representative examples):
  - [heap_xlog_prune_freeze](../h/heap_xlog_prune_freeze.md)
  - [heap_xlog_visible](../h/heap_xlog_visible.md)
  - [heap_xlog_insert](../h/heap_xlog_insert.md)
  - [heap_xlog_multi_insert](../h/heap_xlog_multi_insert.md)
  - [heap_xlog_update](../h/heap_xlog_update.md)

## Notes and Other Information
- Specifically designed for WAL replay operations during crash recovery
- Works with RelFileLocator instead of Relation objects
- Handles FSM page extension and initialization automatically
- Uses exclusive buffer locking for consistency during recovery
- Updates are marked as hints and don't require additional WAL logging
- Called from various heap WAL replay functions to maintain FSM consistency
- Located in src/backend/storage/freespace/freespace.c:211-243