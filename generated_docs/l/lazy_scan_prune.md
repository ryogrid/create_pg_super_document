# lazy_scan_prune

## Location
src/backend/access/heap/vacuumlazy.c: 1410 - 1653

## Overview
Performs heap page pruning and freezing during VACUUM operations, handling HOT chain pruning, tuple freezing, dead tuple collection, and visibility map updates.

## Definition
```c
static int lazy_scan_prune(LVRelState *vacrel,
                          Buffer buf,
                          BlockNumber blkno,
                          Page page,
                          Buffer vmbuffer,
                          bool all_visible_according_to_vm,
                          bool *has_lpdead_items)
```

## Detailed Description
lazy_scan_prune is a core function in PostgreSQL's lazy VACUUM implementation that performs comprehensive heap page maintenance. It orchestrates the pruning of HOT (Heap-Only Tuple) update chains, freezes tuples when necessary, collects dead tuple information for index cleanup, and maintains visibility map consistency. The function handles the complex logic of determining page visibility status, managing the interaction between page-level and visibility map bits, and ensuring proper synchronization between heap pages and their corresponding visibility map entries. It also accumulates statistics about tuples processed and handles special cases like pages with LP_DEAD items that need index cleanup.

## Parameters / Member Variables
- `vacrel`: LVRelState containing VACUUM operation state and configuration
- `buf`: Buffer containing the heap page to process
- `blkno`: Block number of the page being processed
- `page`: Pointer to the actual page data
- `vmbuffer`: Buffer containing the visibility map block for this heap page
- `all_visible_according_to_vm`: Cached visibility status from earlier VM lookup
- `has_lpdead_items`: Output parameter indicating if LP_DEAD items remain on the page

## Dependencies
- Functions called/Symbols referenced:
  - BufferGetBlockNumber
  - heap_page_prune_and_freeze
  - heap_page_is_all_visible
  - MultiXactIdIsValid
  - qsort
  - cmpOffsetNumbers
  - dead_items_add
  - PageSetAllVisible
  - PageIsAllVisible
  - PageClearAllVisible
  - visibilitymap_set
  - visibilitymap_get_status
  - visibilitymap_clear
  - VM_ALL_FROZEN
- Called from:
  - lazy_scan_heap

## Notes and Other Information
- Returns the number of tuples deleted from the page during HOT pruning
- Handles complex visibility map synchronization with detailed error checking and warnings
- Sorts dead offsets using cmpOffsetNumbers before adding them to the dead items collection
- Updates various VACUUM statistics including frozen pages, dead items, and tuple counts
- Contains extensive assertion checking in debug builds to verify visibility map consistency
- Manages the relationship between page-level PD_ALL_VISIBLE bit and visibility map bits
- For relations without indexes, can immediately mark dead items as LP_UNUSED