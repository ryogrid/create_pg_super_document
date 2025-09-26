# fsm_truncate_avail

## Location
[src/backend/storage/freespace/fsmpage.c:313-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/fsmpage.c#L313-L341)

## Overview
Sets the available space to zero for all slots numbered >= nslots in a Free Space Map (FSM) page, effectively truncating the available space tracking for removed relation blocks.

## Definition

```c
bool
fsm_truncate_avail(Page page, int nslots)
```
## Detailed Description
The  function is used during relation truncation operations to clear the available space information for slots that correspond to blocks that have been removed from the relation. It operates on an FSM page by zeroing out all leaf nodes (slots) starting from the specified  index to the end of the page. After clearing the leaf nodes, it calls  to reconstruct the upper levels of the FSM tree structure to maintain consistency.

The function is part of PostgreSQL's Free Space Map implementation, which tracks available space in heap pages to optimize insertion operations. When a relation is truncated, the corresponding FSM pages must also be updated to reflect that the truncated blocks no longer exist and should not be considered for space allocation.

## Parameters / Member Variables
- : The FSM page to be truncated, represented as a generic Page structure
- : The starting slot number from which all subsequent slots should be cleared (must be >= 0 and < LeafNodesPerPage)

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetContents](../P/PageGetContents.md): Extracts the FSM page content from the generic page structure
  - [fsm_rebuild_page](fsm_rebuild_page.md): Reconstructs the upper levels of the FSM page after modification
- Constants referenced:
  - FSMPage: Type definition for FSM page structure
  - LeafNodesPerPage: Maximum number of leaf nodes per FSM page
  - NonLeafNodesPerPage: Number of non-leaf nodes per FSM page
  - NodesPerPage: Total number of nodes per FSM page
- Called from (representative examples):
  - [FreeSpaceMapPrepareTruncateRel](../F/FreeSpaceMapPrepareTruncateRel.md): Main function that prepares FSM for relation truncation

## Notes and Other Information
- Returns  if the page was modified,  if no changes were needed (all affected slots were already zero)
- The function includes an assertion to validate that  is within valid bounds
- After clearing leaf nodes, the function always calls  if any changes were made to ensure the FSM tree structure remains consistent
- This function is critical for maintaining FSM integrity during VACUUM and relation truncation operations
- The clearing operation works on the fp_nodes array, starting from the leaf nodes section (NonLeafNodesPerPage + nslots)