# FreeSpaceMapVacuumRange

## Location
[src/backend/storage/freespace/freespace.c:377-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L377-L391)

## Overview
FreeSpaceMapVacuumRange updates upper-level pages in the relation's Free Space Map for a specific range of heap blocks, optimizing FSM maintenance when only certain blocks have been modified.

## Definition
void FreeSpaceMapVacuumRange(Relation rel, BlockNumber start, BlockNumber end)

## Detailed Description
This function is a range-specific version of FreeSpaceMapVacuum that updates only the upper-level FSM slots covering a specific range of heap blocks. It assumes that only heap pages between start and end-1 (inclusive) have new free-space information, making it more efficient than updating the entire FSM tree when only a subset of blocks have been modified.

The function uses the same recursive approach as FreeSpaceMapVacuum but limits the update scope to the specified block range. When end is InvalidBlockNumber, it is equivalent to "all the rest of the relation" from the start block onwards.

This targeted approach is particularly useful during operations like:
- Partial vacuum operations
- BRIN index maintenance
- Relation extension operations
- Truncation operations where specific ranges need FSM updates

## Parameters / Member Variables
- : The relation whose FSM needs to be updated
- : The first heap block number in the range that has new free-space information
- : The block number after the last heap block to be updated (end-1 is the last block included)

## Dependencies
- Functions called/Symbols referenced:
  - [fsm_vacuum_page](../f/fsm_vacuum_page.md) (recursively updates FSM pages within the specified range)
- Called from (representative examples):
  - [terminate_brin_buildstate](../t/terminate_brin_buildstate.md) (src/backend/access/brin/brin.c:1724)
  - [lazy_scan_heap](../l/lazy_scan_heap.md) (src/backend/access/heap/vacuumlazy.c:903, 1018, 1059)
  - [RelationTruncate](../R/RelationTruncate.md) (src/backend/catalog/storage.c:438)
  - [brin_doupdate](../b/brin_doupdate.md) (src/backend/access/brin/brin_pageops.c:140, 161, 214, 312)

## Notes and Other Information
- Only performs updates if end > start, avoiding unnecessary work for empty ranges
- When end == InvalidBlockNumber, it processes from start to the end of the relation
- More efficient than FreeSpaceMapVacuum when only a specific range of blocks needs FSM updates
- Essential for maintaining FSM consistency during partial operations without the overhead of full tree updates
- Uses the same recursive tree traversal approach but with range constraints
- Located in src/backend/storage/freespace/freespace.c:368-384

## Simplified Source

```c
void
FreeSpaceMapVacuumRange(Relation rel, BlockNumber start, BlockNumber end)
{
    bool dummy;

    // Update FSM tree for the specified range if valid
    if (end > start)
        (void) fsm_vacuum_page(rel, FSM_ROOT_ADDRESS, start, end, &dummy);
}
```