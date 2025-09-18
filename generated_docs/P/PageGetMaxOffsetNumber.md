# PageGetMaxOffsetNumber

## Location
[src/include/storage/bufpage.h:370-383](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L370-L383)

## Overview
Returns the maximum offset number used on a page, which also represents the total number of items (tuples) stored on the page since offset numbers are 1-based.

## Definition
static inline OffsetNumber PageGetMaxOffsetNumber(Page page)

## Detailed Description
PageGetMaxOffsetNumber calculates the maximum offset number on a page by analyzing the page header's pd_lower field. Since offset numbers start from 1, the maximum offset number also represents the total count of items on the page. The function works by subtracting the page header size from pd_lower and dividing by the size of ItemIdData structures to determine how many item identifiers are present.

The function includes special handling for uninitialized pages (where pd_lower == 0) by returning zero to ensure safe behavior. This is critical for preventing access to invalid memory locations or incorrect calculations on empty or corrupted pages.

This function is essential for page traversal operations, space management, and determining iteration bounds when scanning through items on a page.

## Parameters / Member Variables
- : A Page pointer to the page for which to determine the maximum offset number

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast for accessing page header)
  - SizeOfPageHeaderData (constant defining page header size)
  - [ItemIdData](../I/ItemIdData.md) (structure size used in calculation)
- Called from (representative examples):
  - heap operations (heapgettup_continue_page, heap_fetch, heap_insert)
  - B-tree operations (_bt_binsrch, _bt_readpage, _bt_split)
  - GIN operations (entryLocateEntry, processPendingPage, ginVacuumEntryPage)
  - GiST operations (gistScanPage, gistformdownlink, gistvacuumpage)
  - Hash operations (_hash_load_qualified_items, _hash_pgaddtup)
  - SP-GiST operations (spgWalk, vacuumLeafPage)
  - BRIN operations (brinGetTupleForHeapBlock, brin_evacuate_page)
  - Page management (PageAddItemExtended, PageRepairFragmentation)

## Notes and Other Information
- Returns 0 for uninitialized pages (pd_lower <= SizeOfPageHeaderData) to ensure safe behavior
- The result represents both the maximum offset number and the total item count on the page
- Critical for bounds checking during page iteration and space calculations
- Used extensively in vacuum operations to determine page utilization
- Essential for maintaining page integrity during concurrent access
- The inline declaration provides performance optimization for this frequently called function
- Offset numbers are 1-based, so a page with 3 items will return offset number 3