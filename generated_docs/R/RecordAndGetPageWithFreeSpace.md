# RecordAndGetPageWithFreeSpace

## Location
[src/backend/storage/freespace/freespace.c:154-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L154-L193)

## Overview
RecordAndGetPageWithFreeSpace is an optimized FSM function that combines updating free space information for a page with searching for a new page with sufficient free space, providing better performance through reduced locking overhead.

## Definition
```c
BlockNumber RecordAndGetPageWithFreeSpace(Relation rel, BlockNumber oldPage, Size oldSpaceAvail, Size spaceNeeded)
```

## Detailed Description
This function serves as an optimized combination of RecordPageWithFreeSpace and GetPageWithFreeSpace operations. It first updates the FSM with the actual free space available on a previously suggested page, then attempts to find a new suitable page. The function employs a locality-aware search strategy, preferring pages that are close to the old page (on the same FSM page) to improve spatial locality and potentially cache performance.

The function uses fsm_set_and_search for the combined update and search operation, which provides better performance than separate calls. If a suitable page is found nearby, it validates that the page actually exists in the relation before returning it. If no nearby suitable page is found, it falls back to a general FSM search.

## Parameters / Member Variables
- `rel`: The relation to update and search for free space
- `oldPage`: The block number of the page whose free space is being updated
- `oldSpaceAvail`: The actual free space available on the old page
- `spaceNeeded`: The minimum amount of free space required for the new page

## Dependencies
- Functions called/Symbols referenced:
  - [fsm_space_avail_to_cat](../f/fsm_space_avail_to_cat.md)
  - [fsm_space_needed_to_cat](../f/fsm_space_needed_to_cat.md)
  - FSMAddress
  - [fsm_get_location](../f/fsm_get_location.md)
  - [fsm_set_and_search](../f/fsm_set_and_search.md)
  - [fsm_get_heap_blk](../f/fsm_get_heap_blk.md)
  - [fsm_does_block_exist](../f/fsm_does_block_exist.md)
  - [fsm_search](../f/fsm_search.md)
- Called from (representative examples):
  - [brin_getinsertbuffer](../b/brin_getinsertbuffer.md)
  - [RelationGetBufferForTuple](RelationGetBufferForTuple.md)

## Notes and Other Information
- Provides better performance than separate RecordPageWithFreeSpace + GetPageWithFreeSpace calls
- Implements spatial locality optimization by preferring nearby pages
- Validates returned block numbers to ensure they exist in the relation
- Falls back to general search if local search fails
- Part of PostgreSQL's Free Space Map public API
- Located in src/backend/storage/freespace/freespace.c:154-193