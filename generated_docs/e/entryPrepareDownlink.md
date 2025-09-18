# entryPrepareDownlink

## Location
[src/backend/access/gin/ginentrypage.c:702-722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginentrypage.c#L702-L722)

## Overview
Constructs insertion payload data for creating downlink tuples when inserting references to child pages in GIN entry tree internal nodes.

## Definition
```c
static void *entryPrepareDownlink(GinBtree btree, Buffer lbuf)
```

## Detailed Description
entryPrepareDownlink creates the necessary data structure for inserting a downlink tuple into a GIN entry tree internal node. This function is crucial for maintaining tree structure when child pages are split or new child pages are created. The process involves:

1. **Child Page Analysis**: Extracts the page from the provided buffer and obtains its block number for the downlink reference.

2. **Key Extraction**: Retrieves the rightmost tuple from the child page using getRightMostTuple, which serves as the separator key for the internal node entry.

3. **Interior Tuple Formation**: Calls GinFormInteriorTuple to create a properly formatted internal node tuple that combines the separator key with the child page block number.

4. **Insertion Data Preparation**: Allocates and populates a GinBtreeEntryInsertData structure containing the new interior tuple and insertion metadata.

This function is essential for maintaining the hierarchical structure of GIN entry trees by ensuring proper parent-child relationships.

## Parameters / Member Variables
- `btree`: GinBtree structure containing B-tree context and configuration information
- `lbuf`: Buffer containing the child page that needs a downlink created in its parent

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [getRightMostTuple](../g/getRightMostTuple.md)
  - [GinFormInteriorTuple](../G/GinFormInteriorTuple.md)
  - [palloc](../p/palloc.md)
  - [GinBtreeEntryInsertData](../G/GinBtreeEntryInsertData.md) (struct allocation and initialization)
- Called from (representative examples):
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)

## Notes and Other Information
- This is a static function used internally within the GIN entry page management system
- Returns a dynamically allocated GinBtreeEntryInsertData structure that must be freed by the caller
- The function always sets isDelete to false since this is an insertion operation
- Critical for maintaining tree balance and navigation in GIN indexes
- The rightmost tuple from the child page serves as the high key or separator for routing searches
- Part of the GIN index internal node maintenance ensuring proper tree structure during splits and growth