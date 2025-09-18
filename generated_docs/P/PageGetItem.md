# PageGetItem

## Location
[src/include/storage/bufpage.h:352-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L352-L369)

## Overview
Retrieves a specific item (tuple) from a PostgreSQL page using the item identifier, providing the fundamental mechanism for accessing stored data within pages.

## Definition
static inline Item PageGetItem(Page page, ItemId itemId)

## Detailed Description
PageGetItem is a core function that retrieves an item (tuple) from a page by calculating its memory location based on the item identifier. The function works by adding the page's base address to the offset stored in the ItemId structure. It includes essential validation checks to ensure the item has storage allocated before attempting to access it.

This function is fundamental to PostgreSQL's storage layer and is used extensively across all access methods (heap, B-tree, GIN, GiST, Hash, SP-GiST, BRIN) for retrieving tuples and index entries. The inline declaration ensures optimal performance since this function is called very frequently during data access operations.

## Parameters / Member Variables
- : A Page pointer to the page containing the item
- : An ItemId structure that contains metadata about the item, including its offset within the page

## Dependencies
- Functions called/Symbols referenced:
  - Assert (validation macro)
  - ItemIdHasStorage (checks if the item has storage allocated)
  - ItemIdGetOffset (extracts the offset from the ItemId)
  - Item (return type for the retrieved item)
- Called from (representative examples):
  - heap operations (heapgettup, heap_fetch, heap_delete, heap_update)
  - B-tree operations (_bt_search, _bt_readpage, _bt_split)
  - GIN operations (collectMatchBitmap, entryLocateEntry)
  - GiST operations (gistScanPage, gistdoinsert)
  - Hash operations (_hash_load_qualified_items)
  - SP-GiST operations (spgWalk, vacuumLeafPage)
  - BRIN operations (brinGetTupleForHeapBlock)

## Notes and Other Information
- This function does not modify the status of any resources passed to it
- The semantics may change in future PostgreSQL versions (as noted in source comments)
- Essential for all tuple retrieval operations across PostgreSQL's storage system
- Includes Assert statements for debugging builds to catch invalid usage
- The function assumes the caller has already validated that the page and ItemId are valid
- Used in both forward and backward page scans during query execution
- Critical for maintaining data consistency during concurrent access