# ginRedoInsertData

## Location
[src/backend/access/gin/ginxlog.c:319-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L319-L346)

## Overview
Replays data insertion operations in GIN index pages during WAL recovery, handling both leaf and internal data page modifications with different strategies.

## Definition
```c
static void ginRedoInsertData(Buffer buffer, bool isLeaf, BlockNumber rightblkno, void *rdata)
```

## Detailed Description
This function serves as a dispatcher for GIN data page modifications during WAL replay, choosing the appropriate strategy based on whether the target page is a leaf or internal page. For leaf pages, it delegates to the complex ginRedoRecompress function to handle posting list recompression. For internal pages, it performs simpler operations: updating downlink pointers after page splits and adding new posting items.

The function demonstrates the architectural difference between GIN leaf and internal data pages - leaf pages store compressed posting lists that require sophisticated recompression logic, while internal pages store posting items with block number references that can be modified more directly.

## Parameters / Member Variables
- `buffer`: Buffer containing the GIN data page to be modified
- `isLeaf`: Boolean flag indicating whether the page is a leaf page or internal page
- `rightblkno`: Block number of the right sibling page after a split operation
- `rdata`: Pointer to WAL record data, cast to either ginxlogRecompressDataLeaf or ginxlogInsertDataInternal depending on page type

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - GinPageIsLeaf
  - [ginRedoRecompress](ginRedoRecompress.md)
  - GinDataPageGetPostingItem
  - PostingItemSetBlockNumber
  - [GinDataPageAddPostingItem](../G/GinDataPageAddPostingItem.md)
- Data structures used:
  - ginxlogRecompressDataLeaf
  - ginxlogInsertDataInternal
  - [PostingItem](../P/PostingItem.md)
- Called from:
  - [ginRedoInsert](ginRedoInsert.md)

## Notes and Other Information
- This is a static function used exclusively within GIN WAL replay operations
- The function acts as a type-specific dispatcher based on the isLeaf parameter
- For leaf pages, all complex logic is delegated to ginRedoRecompress
- For internal pages, operations are much simpler and handled directly
- The function includes assertions to validate page type consistency
- Internal page operations focus on maintaining proper downlink references after page splits
- The design reflects the different data structures and complexity levels between GIN leaf and internal pages

## Simplified Source

```c
static void ginRedoInsertData(Buffer buffer, bool isLeaf, BlockNumber rightblkno, void *rdata)
{
    Page page = BufferGetPage(buffer);

    if (isLeaf) {
        // Leaf pages: handle complex posting list recompression
        ginxlogRecompressDataLeaf *data = (ginxlogRecompressDataLeaf *) rdata;
        ginRedoRecompress(page, data);
    } else {
        // Internal pages: handle posting item updates
        ginxlogInsertDataInternal *data = (ginxlogInsertDataInternal *) rdata;

        // Update downlink to right page after split
        PostingItem *oldpitem = GinDataPageGetPostingItem(page, data->offset);
        PostingItemSetBlockNumber(oldpitem, rightblkno);

        // Add new posting item
        GinDataPageAddPostingItem(page, &data->newitem, data->offset);
    }
}
```