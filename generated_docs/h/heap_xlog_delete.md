# heap_xlog_delete

## Location
src/backend/access/heap/heapam.c: 9519 - 9591

## Overview
Replays XLOG_HEAP_DELETE WAL records during PostgreSQL recovery to restore tuple deletion operations and maintain proper visibility map state.

## Definition
```c
static void heap_xlog_delete(XLogReaderState *record)
```

## Detailed Description
This function handles the recovery of tuple deletion operations from WAL records during PostgreSQL crash recovery or standby replay. It performs several key tasks:

1. **Visibility Map Management**: Updates the visibility map when the deletion affects page visibility, clearing appropriate bits when a page transitions from all-visible to having deleted tuples.

2. **Tuple Header Updates**: Modifies the deleted tuples header information including transaction IDs, command IDs, and various status flags to reflect the deletion state.

3. **Special Deletion Types**: Handles different types of deletions including super deletions (used in some optimization scenarios) and partition movement operations.

4. **Page Maintenance**: Marks the page as a candidate for pruning and updates page-level visibility flags as needed.

The function carefully validates the tuple location and panics if inconsistencies are detected, ensuring data integrity during recovery.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the delete operation, including the xl_heap_delete structure with deletion details

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract xl_heap_delete structure from WAL record
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md): Get target relation and block information
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)/ItemPointerSetOffsetNumber: Set target tuple location
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)/FreeFakeRelcacheEntry: Temporary relation cache management
  - [visibilitymap_pin](../v/visibilitymap_pin.md)/visibilitymap_clear: Update visibility map when needed
  - XLogReadBufferForRedo: Read target page for redo operation
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)/PageGetItemId: Access tuple within page
  - [PageGetItem](../P/PageGetItem.md): Get tuple data from page
  - [fix_infomask_from_infobits](../f/fix_infomask_from_infobits.md): Restore tuple header flags from compressed WAL data
  - HeapTupleHeaderSetXmax/HeapTupleHeaderSetXmin: Set transaction ID fields
  - HeapTupleHeaderSetCmax: Set command ID
  - HeapTupleHeaderClearHotUpdated: Clear HOT update flag
  - PageSetPrunable: Mark page for future pruning
  - [PageClearAllVisible](../P/PageClearAllVisible.md): Clear page visibility flag
  - HeapTupleHeaderSetMovedPartitions: Handle partition movement deletions

- Called from:
  - [heap_redo](heap_redo.md): Main heap WAL record replay dispatcher

## Notes and Other Information
- This is a static function exclusively used during WAL recovery operations
- Handles both regular deletions and special cases like super deletions and partition moves  
- Includes comprehensive validation with PANIC on tuple location inconsistencies
- The function distinguishes between different deletion types through XLH_DELETE_* flags
- Super deletions set xmin to InvalidTransactionId instead of setting xmax
- Partition movement deletions use special t_ctid handling via HeapTupleHeaderSetMovedPartitions
- Essential for maintaining MVCC consistency and visibility during recovery operations
- Updates both tuple-level and page-level metadata to ensure proper recovery state