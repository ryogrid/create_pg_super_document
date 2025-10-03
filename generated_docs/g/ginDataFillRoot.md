# ginDataFillRoot

## Location
[src/backend/access/gin/gindatapage.c:1349-1369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1349-L1369)

## Overview
ginDataFillRoot fills a new root page with right bound values from left and right child pages in PostgreSQL's GIN (Generalized Inverted Index) data page management system.

## Definition

```c
void
ginDataFillRoot(GinBtree btree, Page root, BlockNumber lblkno, Page lpage, BlockNumber rblkno, Page rpage)
```
## Detailed Description
This function initializes a new root page by creating PostingItems for both left and right child pages and adding them to the root page. Each PostingItem contains the right bound key from the respective child page and the block number where that child page is located. This operation is typically performed during B-tree splits when a new root level needs to be created.

The function is designed to be callable from both normal B-tree operations and WAL (Write-Ahead Logging) recovery operations (ginxlog), which is why it doesn't rely heavily on the btree parameter for its core functionality.

## Parameters / Member Variables
- `btree`: GinBtree structure representing the GIN B-tree context (may be unused in some contexts)
- `root`: The new root page to be filled
- `lblkno`: Block number of the left child page
- `lpage`: The left child page
- `rblkno`: Block number of the right child page
- `rpage`: The right child page
## Dependencies
- Functions called/Symbols referenced:
  - GinDataPageGetRightBound
  - PostingItemSetBlockNumber
  - [GinDataPageAddPostingItem](../G/GinDataPageAddPostingItem.md)
  - InvalidOffsetNumber (constant)
- Called from (representative examples):
  - [ginPrepareDataScan](ginPrepareDataScan.md)
  - [GinBtreeDataLeafInsertData](../G/GinBtreeDataLeafInsertData.md) (via function pointer)

## Notes and Other Information
- This is a public function (not static) and can be called from other modules
- The function creates two PostingItems, one for each child page
- Uses InvalidOffsetNumber when adding items, allowing the system to determine the appropriate insertion position
- Can be called during WAL recovery operations, making it important for crash recovery
- Located in src/backend/access/gin/gindatapage.c at lines 1349-1369
- The comment indicates it should not rely on the btree parameter since it's called from ginxlog