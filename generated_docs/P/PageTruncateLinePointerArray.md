# PageTruncateLinePointerArray

## Location
src/backend/storage/page/bufpage.c: 835 - 906

## Overview
Removes unused line pointers from the end of the line pointer array to reclaim space while preserving at least one unused item to avoid creating an empty page.

## Definition
void PageTruncateLinePointerArray(Page page)

## Detailed Description
This function performs selective truncation of the line pointer array by removing trailing unused line pointers. It is specifically designed for heap pages and is typically called by VACUUM during its second pass. The function scans the line pointer array backwards to identify consecutive unused line pointers at the end that can be safely removed. It ensures that at least one item remains to avoid creating a completely empty page, and properly manages the PD_HAS_FREE_LINES hint bit based on whether unused line pointers remain elsewhere in the array.

## Parameters / Member Variables
- page: The heap page whose line pointer array will be truncated (caller can have exclusive lock or full cleanup lock)

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - [PageGetItemId](PageGetItemId.md)
  - ItemIdIsUsed
  - [PageSetHasFreeLinePointers](PageSetHasFreeLinePointers.md)
  - [PageClearHasFreeLinePointers](PageClearHasFreeLinePointers.md)
  - memset (when CLOBBER_FREED_MEMORY is enabled)
  - Assert (for debugging)
- Data types used:
  - PageHeader
  - ItemId
  - [ItemIdData](../I/ItemIdData.md)
  - FirstOffsetNumber
- Called from:
  - [heap_page_prune_execute](../h/heap_page_prune_execute.md) (during heap pruning operations)
  - [lazy_vacuum_heap_page](../l/lazy_vacuum_heap_page.md) (during VACUUM operations)
  - PageIsVerified (for page verification)

## Notes and Other Information
- This function is specifically for heap pages only and should not be used with index pages
- Expects at least one LP_UNUSED line pointer to exist (VACUUM should not call this otherwise)
- Deliberately avoids truncating to zero items to prevent creating PageIsEmpty() pages
- Scans line pointers from back to front to identify safe truncation points
- Updates the page header's pd_lower field to reflect the reduced line pointer array size
- Includes memory clobbering in debug builds (CLOBBER_FREED_MEMORY) to help detect use-after-free bugs
- Properly manages the PD_HAS_FREE_LINES hint bit for PageAddItemExtended optimization
- The function is located in src/backend/storage/page/bufpage.c:835-906