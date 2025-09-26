# PageGetItemId

## Location
[src/include/storage/bufpage.h:241-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L241-L254)

## Overview
PageGetItemId is an inline function that retrieves a pointer to a specific line pointer (item identifier) from a page's line pointer array based on the given offset number.

## Definition

```c
static inline ItemId
PageGetItemId(Page page, OffsetNumber offsetNumber)
```
## Detailed Description
This function accesses the line pointer array (pd_linp) in a page header to return a pointer to a specific ItemId. Line pointers are 1-based indexed structures that contain metadata about tuples stored on the page, including their offset, length, and flags. The function performs array indexing by subtracting 1 from the offsetNumber to convert from 1-based to 0-based indexing. This is a fundamental function in PostgreSQL's page management system, used extensively across all access methods to locate and manipulate tuples on pages.

## Parameters / Member Variables
- : A pointer to a page (Page type) containing the line pointer array
- : A 1-based index (OffsetNumber type, uint16) specifying which line pointer to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (casting page to PageHeaderData pointer)
  - [PageHeaderData](PageHeaderData.md) structure (accessed via pd_linp array)
  - ItemId (return type - pointer to ItemIdData)
  - OffsetNumber (parameter type - uint16)
- Called from (representative examples):
  - [brininsert](../b/brininsert.md) (in src/backend/access/brin/brin.c:444)
  - [entryLocateEntry](../e/entryLocateEntry.md) (in src/backend/access/gin/ginentrypage.c:310)
  - [gistdoinsert](../g/gistdoinsert.md) (in src/backend/access/gist/gist.c:751)
  - [heapgettup](../h/heapgettup.md) (in src/backend/access/heap/heapam.c:928)
  - [_bt_search](../b/_bt_search.md) (in src/backend/access/nbtree/nbtsearch.c:150)
  - [PageAddItemExtended](PageAddItemExtended.md) (in src/backend/storage/page/bufpage.c:233)

## Notes and Other Information
- This is an inline function defined in bufpage.h for performance
- Uses 1-based indexing consistent with PostgreSQL's OffsetNumber convention
- Returns a pointer to ItemIdData, not the actual tuple data
- The ItemId contains metadata about the tuple (offset, length, flags) rather than the tuple content itself
- Used extensively throughout PostgreSQL's storage layer for tuple access and manipulation
- Essential for all access methods (heap, B-tree, hash, GIN, GiST, SP-GiST, BRIN)
- Line pointers enable indirection, allowing tuples to be moved within a page without changing external references
- The function performs no bounds checking - callers must ensure offsetNumber is valid