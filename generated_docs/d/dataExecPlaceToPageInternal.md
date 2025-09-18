# dataExecPlaceToPageInternal

## Location
[src/backend/access/gin/gindatapage.c:1145-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1145-L1200)

## Overview
dataExecPlaceToPageInternal executes the actual data insertion on an internal GIN data page after beginPlaceToPage has determined the insertion will fit.

## Definition
```c
static void dataExecPlaceToPageInternal(GinBtree btree, Buffer buf, GinBtreeStack *stack,
                                       void *insertdata, BlockNumber updateblkno,
                                       void *ptp_workspace)
```

## Detailed Description
This function performs the actual insertion operation on an internal GIN data page within a critical section. It handles two key operations for internal node insertions: updating the existing downlink pointer of the item at the specified offset to point to updateblkno, and inserting the new PostingItem at the correct position. The function also manages WAL logging if the relation requires it and the operation is not part of an index build process.

The function is designed to be called after dataBeginPlaceToPageInternal has confirmed that the insertion will fit, and it operates within an already-established critical section with XLOG record creation initialized.

## Parameters / Member Variables
- `btree`: GIN B-tree structure containing tree metadata and configuration
- `buf`: Buffer containing the target internal data page for insertion (registered in slot 0)
- `stack`: GIN B-tree stack indicating the insertion position and offset
- `insertdata`: Pointer to the PostingItem to be inserted
- `updateblkno`: Block number to update the existing item's downlink pointer to
- `ptp_workspace`: Workspace information passed from the begin phase (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - GinDataPageGetPostingItem
  - PostingItemSetBlockNumber
  - [GinDataPageAddPostingItem](../G/GinDataPageAddPostingItem.md)
  - MarkBufferDirty
  - RelationNeedsWAL
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [PostingItem](../P/PostingItem.md) (struct)
  - ginxlogInsertDataInternal (struct)
  - REGBUF_STANDARD (constant)
- Called from:
  - [dataExecPlaceToPage](dataExecPlaceToPage.md)

## Notes and Other Information
- This function operates within a critical section and modifies the page buffer directly
- The WAL logging uses a static variable for the log data structure to avoid palloc within the critical section
- The function performs both downlink update and new item insertion in a single operation
- The target buffer must be pre-registered in slot 0 for WAL logging purposes
- WAL logging is conditional on RelationNeedsWAL and whether this is a build operation