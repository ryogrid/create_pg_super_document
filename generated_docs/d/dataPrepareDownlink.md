# dataPrepareDownlink

## Location
[src/backend/access/gin/gindatapage.c:1333-1348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1333-L1348)

## Overview
dataPrepareDownlink constructs an insertion payload containing a downlink for a given buffer in PostgreSQL's GIN (Generalized Inverted Index) data page management system.

## Definition

```c
static void *
dataPrepareDownlink(GinBtree btree, Buffer lbuf)
```
## Detailed Description
This static function creates a PostingItem that represents a downlink to be inserted into a GIN B-tree structure. The function allocates memory for a new PostingItem, extracts the block number from the provided buffer, and sets the item's key to the right bound value of the data page. This downlink serves as a pointer/reference that allows navigation from parent nodes to child nodes in the GIN index structure.

The function is part of the GIN data page management infrastructure and is specifically designed to prepare downlink entries during B-tree operations such as splits or insertions.

## Parameters / Member Variables
- `btree`: GinBtree structure representing the GIN B-tree context
- `lbuf`: Buffer containing the page for which the downlink is being prepared
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [BufferGetPage](../B/BufferGetPage.md)  
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - PostingItemSetBlockNumber
  - GinDataPageGetRightBound
- Called from (representative examples):
  - [ginPrepareDataScan](../g/ginPrepareDataScan.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the gindatapage.c file
- The function allocates memory using palloc(), so the caller is responsible for proper memory management
- The PostingItem structure contains both a block number (for navigation) and a key (for ordering/searching)
- Located in src/backend/access/gin/gindatapage.c at lines 1333-1348

## Simplified Source

```c
static void *
dataPrepareDownlink(GinBtree btree, Buffer lbuf)
{
    PostingItem *pitem = palloc(sizeof(PostingItem));
    Page lpage = BufferGetPage(lbuf);

    // Set downlink to point to this buffer
    PostingItemSetBlockNumber(pitem, BufferGetBlockNumber(lbuf));

    // Set key to the right boundary of the page
    pitem->key = *GinDataPageGetRightBound(lpage);

    return pitem;
}
```