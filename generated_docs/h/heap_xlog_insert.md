# heap_xlog_insert

## Location
[src/backend/access/heap/heapam.c:9592-9710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L9592-L9710)

## Overview
Replays XLOG_HEAP_INSERT WAL records during PostgreSQL recovery to restore tuple insertion operations and maintain proper page and visibility map state.

## Definition
```c
static void heap_xlog_insert(XLogReaderState *record)
```

## Detailed Description
This function handles the recovery of tuple insertion operations from WAL records during PostgreSQL crash recovery or standby replay. It reconstructs inserted tuples and places them at their original locations while maintaining data integrity.

Key operations include:

1. **Visibility Map Management**: Clears visibility map bits when insertions affect previously all-visible pages.

2. **Page Initialization**: Handles special case where the insertion creates the first tuple on a page, requiring full page initialization.

3. **Tuple Reconstruction**: Rebuilds the complete tuple from WAL data including header information and user data, setting appropriate transaction IDs and command IDs.

4. **Page Management**: Adds the reconstructed tuple to the page at the correct offset and updates page metadata.

5. **FSM Updates**: Updates the Free Space Map when page free space falls below 20% to maintain accurate free space tracking.

The function includes comprehensive validation and will panic if inconsistencies are detected during recovery.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the insert operation, including the xl_heap_insert structure with insertion details

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract xl_heap_insert structure from WAL record
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md): Get target relation and block information
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)/ItemPointerSetOffsetNumber: Set target tuple location
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)/FreeFakeRelcacheEntry: Temporary relation cache management
  - [visibilitymap_pin](../v/visibilitymap_pin.md)/visibilitymap_clear: Update visibility map when needed
  - XLogRecGetInfo: Check for special page initialization flag
  - XLogInitBufferForRedo: Initialize buffer for page creation
  - XLogReadBufferForRedo: Read target page for redo operation
  - PageInit: Initialize new page structure
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md): Validate insertion offset
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md): Extract tuple data from WAL record
  - HeapTupleHeaderSetXmin/HeapTupleHeaderSetCmin: Set transaction and command IDs
  - PageAddItem: Insert tuple into page at specified offset
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md): Calculate remaining free space
  - [PageClearAllVisible](../P/PageClearAllVisible.md): Clear page visibility flag when needed
  - XLogRecordPageWithFreeSpace: Update FSM for low free space pages

- Called from:
  - [heap_redo](heap_redo.md): Main heap WAL record replay dispatcher

## Notes and Other Information
- This is a static function exclusively used during WAL recovery operations
- Handles both regular insertions and first-tuple-on-page scenarios requiring page initialization
- Includes assertion that frozen tuple insertions are not supported in this code path
- Uses a union buffer structure to safely reconstruct tuples up to MaxHeapTupleSize
- Implements FSM update heuristic for pages with less than 20% free space remaining
- The function validates tuple placement and panics on offset number or tuple addition failures
- Essential for maintaining MVCC consistency during recovery operations
- Reconstructed tuples receive the transaction ID from the WAL record and FirstCommandId
- Target tuple ID (t_ctid) is set to the insertion location for new tuples
- Only updates FSM when actual redo is needed, not when pages are restored from full page images