# RecordPageWithFreeSpace

## Location
src/backend/storage/freespace/freespace.c: 194 - 210

## Overview
RecordPageWithFreeSpace is a core FSM function that updates the Free Space Map with the current amount of free space available on a specific page.

## Definition
```c
void RecordPageWithFreeSpace(Relation rel, BlockNumber heapBlk, Size spaceAvail)
```

## Detailed Description
This function serves as the primary mechanism for updating free space information in PostgreSQL's Free Space Map. It takes the actual free space measurement from a page and converts it into the FSM's categorized representation, then updates the appropriate FSM entry. The function is essential for maintaining accurate free space tracking as pages are modified through inserts, updates, and deletes.

An important characteristic of this function is that when the new spaceAvail value is higher than the previously stored value, the increased space might not become immediately visible to searchers. Full visibility requires the upper-level FSM pages to be updated, which typically happens during the next FreeSpaceMapVacuum operation.

## Parameters / Member Variables
- `rel`: The relation containing the page being updated
- `heapBlk`: The block number of the page whose free space is being recorded
- `spaceAvail`: The actual amount of free space available on the page in bytes

## Dependencies
- Functions called/Symbols referenced:
  - fsm_space_avail_to_cat
  - FSMAddress
  - fsm_get_location
  - fsm_set_and_search
- Called from (representative examples):
  - terminate_brin_buildstate
  - brin_doupdate
  - brin_doinsert
  - RelationGetBufferForTuple
  - lazy_scan_heap
  - RecordFreeIndexPage

## Notes and Other Information
- Updates may not be immediately visible to searchers if spaceAvail increased
- Upper-level FSM pages need updating via FreeSpaceMapVacuum for full visibility
- Uses fsm_set_and_search with search_cat=0 (no search, just update)
- Widely used across heap operations, BRIN operations, vacuum, and index FSM management
- Part of PostgreSQL's Free Space Map public API
- Located in src/backend/storage/freespace/freespace.c:194-210