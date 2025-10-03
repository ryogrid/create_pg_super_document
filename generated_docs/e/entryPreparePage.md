# entryPreparePage

## Location
[src/backend/access/gin/ginentrypage.c:490-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L490-L526)

## Overview
Prepares a GIN index page for tuple insertion by performing necessary cleanup operations including deleting existing tuples and updating child block numbers when needed.

## Definition

```c
static void
entryPreparePage(GinBtree btree, Page page, OffsetNumber off,
				 GinBtreeEntryInsertData *insertData, BlockNumber updateblkno)
```
## Detailed Description
entryPreparePage is a preparatory function that handles two critical operations before tuple insertion in GIN index entry pages:

1. **Tuple Deletion**: If the insertion data indicates a delete operation (insertData->isDelete), it removes the existing tuple at the specified offset on leaf pages using PageIndexTupleDelete.

2. **Downlink Update**: For non-leaf (internal) pages, when a child page split has occurred (indicated by a valid updateblkno), it updates the downlink pointer in the existing tuple to reference the new child block number.

The function ensures the page is properly prepared for subsequent insertion operations by cleaning up old data and maintaining correct tree structure references.

## Parameters / Member Variables
- : GinBtree structure containing context information for the B-tree operation
- : The target page where the preparation operations will be performed
- : OffsetNumber indicating the position on the page where operations should occur
- : GinBtreeEntryInsertData structure containing insertion metadata, including the delete flag
- : Block number of a new child page (when child split occurred), or InvalidBlockNumber if no update needed

## Dependencies
- Functions called/Symbols referenced:
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - GinPageIsData
  - GinPageIsLeaf
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - GinSetDownlink
- Called from (representative examples):
  - [entryExecPlaceToPage](entryExecPlaceToPage.md)
  - [entrySplitPage](entrySplitPage.md)

## Notes and Other Information
- This is a static function, used internally within ginentrypage.c
- The function includes assertions to ensure it's only called on non-data pages and that insertData->entry is valid
- [Delete](../D/Delete.md) operations are only performed on leaf pages, while downlink updates only occur on internal pages
- The function is part of the GIN index entry page management subsystem

## Simplified Source

```c
static void
entryPreparePage(GinBtree btree, Page page, OffsetNumber off,
                 GinBtreeEntryInsertData *insertData, BlockNumber updateblkno)
{
    // Basic validation
    Assert(insertData->entry);
    Assert(!GinPageIsData(page));

    // Delete existing tuple if this is a delete operation on leaf page
    if (insertData->isDelete) {
        Assert(GinPageIsLeaf(page));
        PageIndexTupleDelete(page, off);
    }

    // Update downlink if this is an internal page and child split occurred
    if (!GinPageIsLeaf(page) && updateblkno != InvalidBlockNumber) {
        IndexTuple tuple = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));
        GinSetDownlink(tuple, updateblkno);
    }
}
```