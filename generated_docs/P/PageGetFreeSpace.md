# PageGetFreeSpace

## Location
src/backend/storage/page/bufpage.c: 907 - 933

## Overview
Returns the size of the free (allocatable) space on a page, reduced by the space needed for a new line pointer.

## Definition


## Detailed Description
PageGetFreeSpace calculates the amount of free space available on a page for new data insertion. It computes this by finding the difference between the upper and lower bounds of the page header (pd_upper and pd_lower), then subtracts the space required for an ItemIdData structure (line pointer). The function is designed primarily for index pages, as noted in the comments. The implementation uses signed arithmetic to handle edge cases where pd_lower might exceed pd_upper, which would indicate a corrupted page.

The function returns 0 if the available space is insufficient to accommodate even a single line pointer, ensuring that callers receive a realistic estimate of usable space.

## Parameters / Member Variables
- : A pointer to the page for which to calculate free space

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (cast to access page header fields)
  - ItemIdData (sizeof to calculate line pointer space requirement)
- Called from (representative examples):
  - terminate_brin_buildstate
  - br_page_get_freespace
  - entryIsEnoughSpace
  - gist_indexsortbuild_levelstate_add
  - gistnospace
  - _hash_doinsert
  - _bt_search_insert
  - _bt_findinsertloc
  - _bt_insertonpg
  - PageGetHeapFreeSpace

## Notes and Other Information
- Primarily intended for index pages; heap pages should use PageGetHeapFreeSpace instead
- Uses signed arithmetic to handle potential page corruption scenarios gracefully
- Accounts for line pointer overhead by subtracting sizeof(ItemIdData)
- Returns 0 when insufficient space is available for a new line pointer
- Located in src/backend/storage/page/bufpage.c:907-933