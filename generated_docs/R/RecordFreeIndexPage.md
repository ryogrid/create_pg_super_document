# RecordFreeIndexPage

## Location
[src/backend/storage/freespace/indexfsm.c:52-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/indexfsm.c#L52-L61)

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
  - [RecordPageWithFreeSpace](RecordPageWithFreeSpace.md) (records the page with maximum free space in FSM)
- Called from (representative examples):
  - [shiftList](../s/shiftList.md) (GIN fast update cleanup)
  - [ginvacuumcleanup](../g/ginvacuumcleanup.md) (GIN index vacuum cleanup)
  - [gistvacuumpage](../g/gistvacuumpage.md) (GiST page vacuum processing)
  - [_bt_pendingfsm_finalize](../b/_bt_pendingfsm_finalize.md) (B-tree FSM finalization)
  - [btvacuumpage](../b/btvacuumpage.md) (B-tree page vacuum processing)
  - [spgvacuumpage](../s/spgvacuumpage.md) (SP-GiST page vacuum processing)

## Notes and Other Information
- Marks pages with maximum free space (BLCKSZ - 1)
- Primarily used during vacuum and cleanup operations
- Enables efficient page reuse across all index access methods
- Simple wrapper around RecordPageWithFreeSpace with maximum free space parameter
- Critical for maintaining index storage efficiency and preventing unnecessary growth

## Simplified Source
```c
void RecordFreeIndexPage(Relation rel, BlockNumber freeBlock) {
    // Mark page as completely free with maximum available space
    RecordPageWithFreeSpace(rel, freeBlock, BLCKSZ - 1);
}
```