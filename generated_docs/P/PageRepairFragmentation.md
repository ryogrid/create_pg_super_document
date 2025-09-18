# PageRepairFragmentation

## Location
src/backend/storage/page/bufpage.c: 699 - 834

## Overview
Removes fragmentation from a heap page by eliminating unused line pointers and compacting remaining tuples to create contiguous free space.

## Definition
void PageRepairFragmentation(Page page)

## Detailed Description
This function performs comprehensive page defragmentation for heap pages following tuple pruning operations. It removes unused line pointers from the end of the line pointer array (particularly useful after HOT chain removal), compacts remaining tuples using the compactify_tuples function, and updates page metadata accordingly. The function includes extensive validation to prevent data corruption, as it operates on shared disk buffers. It also manages the PD_HAS_FREE_LINES hint bit and can truncate the line pointer array when trailing unused entries exist.

## Parameters / Member Variables
- page: The heap page to be defragmented (caller must hold full cleanup lock)

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - [PageGetItemId](PageGetItemId.md)
  - ItemIdIsUsed
  - ItemIdHasStorage
  - ItemIdGetOffset
  - ItemIdGetLength
  - ItemIdSetUnused
  - [compactify_tuples](../c/compactify_tuples.md)
  - [PageSetHasFreeLinePointers](PageSetHasFreeLinePointers.md)
  - [PageClearHasFreeLinePointers](PageClearHasFreeLinePointers.md)
  - ereport (for error reporting)
- Data types used:
  - Offset
  - PageHeader
  - [itemIdCompactData](../i/itemIdCompactData.md)
  - itemIdCompact
  - ItemId
  - OffsetNumber
  - MaxHeapTuplesPerPage
  - SizeOfPageHeaderData
  - [ItemIdData](../I/ItemIdData.md)
- Called from:
  - [heap_page_prune_execute](../h/heap_page_prune_execute.md) (main heap pruning function)
  - PageIsVerified (for page verification)

## Notes and Other Information
- This function is specifically designed for heap pages only; index pages should use PageIndexMultiDelete
- Caller must have a full cleanup lock on the page buffer to ensure exclusive access
- The function includes extensive paranoid checking to prevent data corruption in shared buffers
- Removes trailing unused line pointers to optimize space usage
- Sets or clears the PD_HAS_FREE_LINES hint bit based on remaining unused line pointers
- Can handle completely empty pages by simply resetting the upper boundary
- The function optimizes for the common case where remaining tuples are already in sorted order
- Located in src/backend/storage/page/bufpage.c:699-834