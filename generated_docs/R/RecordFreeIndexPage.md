# RecordFreeIndexPage

## Location
src/backend/storage/freespace/indexfsm.c: 52 - 61

## Overview
RecordFreeIndexPage marks a page as completely free in the Free Space Map (FSM), indicating it has maximum available free space.

## Definition
```c
void RecordFreeIndexPage(Relation rel, BlockNumber freeBlock)
```

## Detailed Description
RecordFreeIndexPage is a wrapper function that registers a page as completely free in the Free Space Map. It calls RecordPageWithFreeSpace with the maximum possible free space value (BLCKSZ - 1), effectively marking the entire page as available for reuse.

This function is typically called during index maintenance operations such as vacuum cleanup, when pages have been completely emptied and can be reclaimed for future use. The function ensures that these freed pages are properly tracked in the FSM so they can be efficiently located and reused by subsequent index operations.

The use of BLCKSZ - 1 as the free space amount indicates that virtually the entire page (minus minimal overhead) is available for new data, making it a prime candidate for reallocation.

## Parameters / Member Variables
- `rel`: The Relation structure representing the index containing the page to be marked as free
- `freeBlock`: The BlockNumber of the page being marked as free in the FSM

## Dependencies
- Functions called/Symbols referenced:
  - RecordPageWithFreeSpace (records the page with maximum free space in FSM)
- Called from (representative examples):
  - shiftList (GIN fast update cleanup)
  - ginvacuumcleanup (GIN index vacuum cleanup)
  - gistvacuumpage (GiST page vacuum processing)
  - _bt_pendingfsm_finalize (B-tree FSM finalization)
  - btvacuumpage (B-tree page vacuum processing)
  - spgvacuumpage (SP-GiST page vacuum processing)

## Notes and Other Information
- Marks pages with maximum free space (BLCKSZ - 1)
- Primarily used during vacuum and cleanup operations
- Enables efficient page reuse across all index access methods
- Simple wrapper around RecordPageWithFreeSpace with maximum free space parameter
- Critical for maintaining index storage efficiency and preventing unnecessary growth