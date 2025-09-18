# heap_xlog_update

## Location
src/backend/access/heap/heapam.c: 9858 - 10129

## Overview
Handles the replay of UPDATE and HOT_UPDATE operations during WAL (Write-Ahead Log) recovery by reconstructing the heap tuple update transaction from the logged information.

## Definition
```c
static void heap_xlog_update(XLogReaderState *record, bool hot_update)
```

## Detailed Description
The `heap_xlog_update` function is a critical component of PostgreSQL's crash recovery mechanism that processes UPDATE and HOT (Heap-Only Tuple) UPDATE operations from the WAL during database recovery. It reconstructs the state of both the old and new tuple versions by:

1. **Processing the old tuple**: Updates the old tuple's header information, sets the forward chain link (t_ctid) to point to the new tuple location, and marks it appropriately for HOT updates
2. **Reconstructing the new tuple**: Builds the new tuple data by combining prefix/suffix data from the old tuple with new data from the WAL record
3. **Managing visibility maps**: Clears visibility map bits when tuples are no longer all-visible
4. **Handling cross-page updates**: Properly manages updates that span different heap pages
5. **Maintaining consistency**: Ensures proper locking order and atomic operations during replay

The function supports space optimization techniques like prefix/suffix compression where unchanged portions of tuples are not logged but reconstructed from the original tuple.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record to be replayed
- `hot_update`: Boolean flag indicating whether this is a HOT (Heap-Only Tuple) update, which occurs within the same page

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetBlockTag, XLogRecGetBlockTagExtended (block information extraction)
  - XLogReadBufferForRedo, XLogInitBufferForRedo (buffer management during redo)
  - visibilitymap_pin, visibilitymap_clear (visibility map maintenance)
  - PageAddItem, PageSetLSN, PageSetPrunable (page-level operations)
  - HeapTupleHeaderSetXmin, HeapTupleHeaderSetXmax, HeapTupleHeaderSetCmin (tuple header management)
  - fix_infomask_from_infobits (tuple visibility state reconstruction)
- Called from (representative examples):
  - heap_redo (main heap WAL replay dispatcher)

## Notes and Other Information
- **Recovery Safety**: The function carefully manages buffer locking order to prevent deadlocks during recovery, though this is less critical during WAL replay than normal operations
- **Space Optimization**: Supports prefix and suffix compression (XLH_UPDATE_PREFIX_FROM_OLD, XLH_UPDATE_SUFFIX_FROM_OLD flags) to minimize WAL logging overhead
- **Visibility Management**: Handles clearing of visibility map bits for both old and new pages when tuples are no longer all-visible
- **FSM Updates**: Updates the Free Space Map when the new page becomes low on free space (less than 20%), but skips this for HOT updates since space will be reclaimed after pruning
- **Error Handling**: Contains several PANIC-level assertions for data consistency validation during recovery
- **HOT Update Handling**: Special processing for Heap-Only Tuple updates that occur within the same page, avoiding cross-page complexity