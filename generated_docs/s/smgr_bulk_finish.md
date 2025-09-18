# smgr_bulk_finish

## Location
src/backend/storage/smgr/bulk_write.c: 131 - 225

## Overview
Finalizes a bulk write operation by flushing remaining pending writes, performing necessary WAL logging, and ensuring proper synchronization depending on the relation type and WAL usage.

## Definition
```c
void smgr_bulk_finish(BulkWriteState *bulkstate)
```

## Detailed Description
This function completes a bulk write operation by first flushing any remaining pending writes and then handling the different synchronization requirements based on relation type and WAL usage. It implements a complex decision tree to handle temporary relations (no fsync needed), unlogged relations (register for checkpoint sync), and WAL-logged relations (handle potential checkpoint races). For WAL-logged relations, it includes sophisticated logic to detect if a checkpoint occurred during the bulk write operation and take appropriate action.

## Parameters / Member Variables
- `bulkstate`: The BulkWriteState structure containing the bulk write operation state and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [smgr_bulk_flush](smgr_bulk_flush.md)
  - SmgrIsTemp
  - [smgrregistersync](smgrregistersync.md)
  - [smgrimmedsync](smgrimmedsync.md)
  - [GetRedoRecPtr](../G/GetRedoRecPtr.md)
  - DELAY_CHKPT_START
  - MyProc (process state)
  - DEBUG1 (logging level)
- Called from (representative examples):
  - [gist_indexsortbuild](../g/gist_indexsortbuild.md)
  - [end_heap_rewrite](../e/end_heap_rewrite.md)
  - [btbuildempty](../b/btbuildempty.md)
  - _bt_load
  - [spgbuildempty](spgbuildempty.md)
  - [RelationCopyStorage](../R/RelationCopyStorage.md)

## Notes and Other Information
- Temporary relations require no synchronization as they don't survive crashes
- For unlogged relations, the function conservatively registers for sync even when it might be a permanent relation with wal_level=minimal
- WAL-logged relations use checkpoint delay flags to prevent race conditions between checkpoint start and sync registration
- The function detects concurrent checkpoints by comparing the initial and current redo pointers
- If a checkpoint occurred during bulk writing, immediate fsync is performed instead of deferring to the next checkpoint
- This function implements the PostgreSQL crash recovery guarantee that all committed data can be recovered from WAL or is properly fsynced