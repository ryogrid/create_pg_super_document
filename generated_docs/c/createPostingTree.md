# createPostingTree

## Location
[src/backend/access/gin/gindatapage.c:1775-1881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1775-L1881)

## Overview
createPostingTree creates a new GIN posting tree containing the given item pointers (TIDs) and returns the block number of the root page of the newly created posting tree.

## Definition

```c
struct the new root page in memory first. */
	tmppage = (Page) palloc(BLCKSZ);
```
## Detailed Description
createPostingTree constructs a new posting tree for GIN indexes when the posting list becomes too large to fit in a single entry tree leaf page. The function:

1. Creates a new root page in memory with GIN_DATA | GIN_LEAF | GIN_COMPRESSED flags
2. Compresses and writes as many item pointers as possible to the root page in segments (up to GinPostingListSegmentMaxSize bytes each)
3. Allocates a new physical page and copies the in-memory page to it
4. Handles WAL logging if required (except during index builds)
5. Copies predicate locks from the entry buffer to the new posting tree
6. If there are remaining items that couldn't fit in the root, recursively inserts them using ginInsertItemPointers

The function ensures all items are in sorted order with no duplicates before processing.

## Parameters / Member Variables
- : The GIN index relation where the posting tree will be created
- : Array of ItemPointerData (TIDs) to be stored, must be sorted with no duplicates
- : Number of items in the items array
- : Statistics collection structure for index builds (NULL during normal operations)
- : Buffer containing the entry tree leaf page from which predicate locks are copied

## Dependencies
- Functions called/Symbols referenced:
  - [GinInitPage](../G/GinInitPage.md)
  - GinDataLeafPageGetPostingList
  - [ginCompressPostingList](../g/ginCompressPostingList.md)
  - [GinNewBuffer](../G/GinNewBuffer.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PredicateLockPageSplit](../P/PredicateLockPageSplit.md)
  - [PageRestoreTempPage](../P/PageRestoreTempPage.md)
  - [XLogInsert](../X/XLogInsert.md) (for WAL logging)
  - [ginInsertItemPointers](../g/ginInsertItemPointers.md) (for overflow items)
- Called from (representative examples):
  - [addItemPointersToLeafTuple](../a/addItemPointersToLeafTuple.md)
  - [buildFreshLeafTuple](../b/buildFreshLeafTuple.md)

## Notes and Other Information
- The function is critical for GIN index scalability, allowing posting lists to grow beyond single page limits
- WAL logging is skipped during index builds for performance reasons
- Debug logging reports the number of items successfully placed in the root page
- The function handles memory management carefully, using palloc/pfree for temporary structures
- Critical sections protect the page modification and WAL logging operations
- Statistics are updated during index builds to track the number of data pages created