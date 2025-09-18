# heap_xlog_multi_insert

## Location
src/backend/access/heap/heapam.c: 9711 - 9857

## Overview
Replays XLOG_HEAP2_MULTI_INSERT WAL records during PostgreSQL recovery to restore multiple tuple insertion operations in a single atomic operation, optimizing bulk insert performance.

## Definition
```c
static void heap_xlog_multi_insert(XLogReaderState *record)
```

## Detailed Description
This function handles the recovery of multi-tuple insertion operations from WAL records during PostgreSQL crash recovery or standby replay. It efficiently processes multiple tuples that were inserted in a single WAL record, which is commonly used for bulk operations like COPY, INSERT...VALUES with multiple rows, and other batch insertion scenarios.

Key operations include:

1. **Visibility Map Management**: Clears visibility map bits when insertions affect previously all-visible pages, or sets all-visible state for frozen tuple insertions.

2. **Page Initialization**: Handles cases where the multi-insert creates the first tuples on a page, requiring full page initialization.

3. **Batch Tuple Reconstruction**: Iterates through multiple tuples stored in the WAL record, reconstructing each with proper header information, transaction IDs, and placement offsets.

4. **Offset Management**: Handles two different offset strategies - sequential offsets for page initialization, or specific stored offsets for existing pages.

5. **Frozen Tuple Support**: Special handling for all-frozen tuple insertions that can immediately be marked as all-visible.

6. **FSM Updates**: Updates the Free Space Map when page free space falls below 20% after the multi-insert operation.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the multi-insert operation, including the xl_heap_multi_insert structure with insertion details and tuple count

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract xl_heap_multi_insert structure from WAL record
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md): Get target relation and block information
  - XLogRecGetInfo: Check for page initialization flag
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)/FreeFakeRelcacheEntry: Temporary relation cache management
  - [visibilitymap_pin](../v/visibilitymap_pin.md)/visibilitymap_clear: Update visibility map when needed
  - XLogInitBufferForRedo: Initialize buffer for page creation
  - XLogReadBufferForRedo: Read target page for redo operation
  - PageInit: Initialize new page structure
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md): Extract tuple data block from WAL record
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md): Validate insertion offsets
  - SHORTALIGN: Align tuple data pointers properly
  - HeapTupleHeaderSetXmin/HeapTupleHeaderSetCmin: Set transaction and command IDs
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)/ItemPointerSetOffsetNumber: Set tuple location
  - PageAddItem: Insert each tuple into page at specified offset
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md): Calculate remaining free space
  - [PageClearAllVisible](../P/PageClearAllVisible.md): Clear page visibility flag when needed
  - [PageSetAllVisible](../P/PageSetAllVisible.md): Set page as all-visible for frozen insertions
  - XLogRecordPageWithFreeSpace: Update FSM for low free space pages

- Called from:
  - [heap2_redo](heap2_redo.md): Heap2 WAL record replay dispatcher

## Notes and Other Information
- This is a static function exclusively used during WAL recovery operations
- Optimized for bulk insertion scenarios that benefit from batching multiple tuples in a single WAL record
- Includes assertion that XLH_INSERT_ALL_VISIBLE_CLEARED and XLH_INSERT_ALL_FROZEN_SET flags are mutually exclusive
- Handles both page initialization and existing page insertion scenarios
- Uses a union buffer structure to safely reconstruct tuples up to MaxHeapTupleSize
- For page initialization, tuples are placed sequentially starting from FirstOffsetNumber
- For existing pages, specific offsets are stored in the xlrec->offsets array
- Implements comprehensive validation including tuple data length verification
- Supports frozen tuple insertions that can immediately mark pages as all-visible
- Essential for maintaining performance during bulk data loading operations
- Only updates FSM when actual redo is needed and free space is below threshold
- The function panics on various validation failures to ensure data integrity during recovery