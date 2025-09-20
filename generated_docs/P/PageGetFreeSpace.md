# PageGetFreeSpace

## Location
[src/backend/storage/page/bufpage.c:907-933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L907-L933)

## Overview
Returns the size of the free (allocatable) space on a page, reduced by the space needed for a new line pointer.

## Definition

```c
Size
PageGetFreeSpace(Page page)
```
## Detailed Description
PageGetFreeSpace calculates the amount of free space available on a page for new data insertion. It computes this by finding the difference between the upper and lower bounds of the page header (pd_upper and pd_lower), then subtracts the space required for an ItemIdData structure (line pointer). The function is designed primarily for index pages, as noted in the comments. The implementation uses signed arithmetic to handle edge cases where pd_lower might exceed pd_upper, which would indicate a corrupted page.

The function returns 0 if the available space is insufficient to accommodate even a single line pointer, ensuring that callers receive a realistic estimate of usable space.

## Parameters / Member Variables
- : A pointer to the page for which to calculate free space

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (cast to access page header fields)
  - [ItemIdData](../I/ItemIdData.md) (sizeof to calculate line pointer space requirement)
- Called from (representative examples):
  - [terminate_brin_buildstate](../t/terminate_brin_buildstate.md)
  - [br_page_get_freespace](../b/br_page_get_freespace.md)
  - [entryIsEnoughSpace](../e/entryIsEnoughSpace.md)
  - [gist_indexsortbuild_levelstate_add](../g/gist_indexsortbuild_levelstate_add.md)
  - [gistnospace](../g/gistnospace.md)
  - [_hash_doinsert](../h/_hash_doinsert.md)
  - [_bt_search_insert](../b/_bt_search_insert.md)
  - [_bt_findinsertloc](../b/_bt_findinsertloc.md)
  - [_bt_insertonpg](../b/_bt_insertonpg.md)
  - [PageGetHeapFreeSpace](PageGetHeapFreeSpace.md)

## Notes and Other Information
- Primarily intended for index pages; heap pages should use PageGetHeapFreeSpace instead
- Uses signed arithmetic to handle potential page corruption scenarios gracefully
- Accounts for line pointer overhead by subtracting sizeof(ItemIdData)
- Returns 0 when insufficient space is available for a new line pointer
- Located in src/backend/storage/page/bufpage.c:907-933