# GinFormInteriorTuple

## Location
[src/backend/access/gin/ginentrypage.c:201-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L201-L234)

## Overview
Forms a non-leaf entry tuple for GIN index by copying key data from a source tuple and inserting a child block downlink pointer.

## Definition
```c
static IndexTuple GinFormInteriorTuple(IndexTuple itup, Page page, BlockNumber childblk)
```

## Detailed Description
The GinFormInteriorTuple function creates interior (non-leaf) tuples for GIN index pages by copying key data from an existing tuple while excluding any posting list data. It handles two cases: for leaf pages containing posting lists, it copies only the data before the posting list offset; for other tuples, it copies the entire tuple. The function then sets the downlink pointer to the specified child block number, making it suitable for interior nodes in the B-tree structure.

## Parameters / Member Variables
- `itup`: Source IndexTuple to copy key data from (can be leaf or non-leaf)
- `page`: Page containing the source tuple (used to determine page type)
- `childblk`: Block number of the child page to link to via downlink

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsLeaf: Checks if the page is a leaf page
  - GinIsPostingTree: Checks if tuple represents a posting tree entry
  - GinGetPostingOffset: Gets offset to posting list data in tuple
  - IndexTupleSize: Gets size of index tuple
  - GinSetDownlink: Sets downlink pointer to child block
  - [palloc](../p/palloc.md): Allocates memory for new tuple
  - memcpy: Copies tuple data

- Called from (representative examples):
  - [entryPrepareDownlink](../e/entryPrepareDownlink.md): Preparing downlink during page operations
  - [ginEntryFillRoot](../g/ginEntryFillRoot.md): Filling root page entries

## Notes and Other Information
- Function is static (internal to ginentrypage.c)
- Handles both leaf tuples with posting lists and regular tuples differently
- For posting list tuples, only copies data up to posting offset to exclude posting data
- Properly adjusts tuple size header when truncating posting list data
- Essential for maintaining B-tree structure in GIN index interior nodes
- Uses MAXALIGN for proper memory alignment of truncated tuples