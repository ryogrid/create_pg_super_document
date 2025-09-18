# PageClearHasFreeLinePointers

## Location
[src/include/storage/bufpage.h:405-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L405-L410)

## Overview
Clears the PD_HAS_FREE_LINES flag in the page header to indicate that the page no longer contains reusable line pointers.

## Definition
static inline void PageClearHasFreeLinePointers(Page page)

## Detailed Description
This function clears the PD_HAS_FREE_LINES bit in the page header's pd_flags field using a bitwise AND operation with the negated flag constant. This operation removes the optimization hint that indicated the page contained reusable line pointers. The function is typically called when all previously freed line pointers on a page have been reclaimed or when the page structure has been reorganized in a way that eliminates the availability of free line pointers.

Clearing this flag helps maintain accurate page state information for PostgreSQL's storage management system, ensuring that space allocation decisions are based on current page conditions.

## Parameters / Member Variables
- page: A pointer to the page whose free line pointer flag should be cleared

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (cast to access page header structure)
  - PD_HAS_FREE_LINES (flag constant)
- Called from (representative examples):
  - [mask_page_hint_bits](../m/mask_page_hint_bits.md) (src/backend/access/common/bufmask.c:55)
  - PageAddItemExtended (src/backend/storage/page/bufpage.c:280)
  - [PageRepairFragmentation](PageRepairFragmentation.md) (src/backend/storage/page/bufpage.c:812)
  - [PageTruncateLinePointerArray](PageTruncateLinePointerArray.md) (src/backend/storage/page/bufpage.c:895)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- The function modifies the page header by clearing a specific bit flag
- Used in conjunction with PageHasFreeLinePointers and PageSetHasFreeLinePointers for complete flag management
- Called during various page maintenance operations to maintain accurate page state
- Often used when free line pointers have been consumed or page structure has been reorganized