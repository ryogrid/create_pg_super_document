# FreeSpaceMapVacuum

## Location
[src/backend/storage/freespace/freespace.c:358-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L358-L376)

## Overview
FreeSpaceMapVacuum updates upper-level pages in the relation's Free Space Map after bottom-level pages have been updated with new free-space information.

## Definition
void FreeSpaceMapVacuum(Relation rel)

## Detailed Description
This function is responsible for propagating free space information up the FSM tree after the bottom-level (leaf) pages have been updated with new free space data. The FSM is organized as a tree structure where leaf pages contain actual free space information for heap blocks, and upper-level pages maintain summaries of the maximum free space available in their subtrees.

The function works by recursively scanning the FSM tree starting from the root, using fsm_vacuum_page to update each level. This ensures that upper-level pages correctly reflect the maximum free space available in their subtrees, maintaining the integrity of the FSM tree structure.

## Parameters / Member Variables
- : The relation whose FSM upper-level pages need to be updated

## Dependencies
- Functions called/Symbols referenced:
  - fsm_vacuum_page (recursively updates FSM pages)
- Called from (representative examples):
  - brin_vacuum_scan (src/backend/access/brin/brin.c:2192)
  - IndexFreeSpaceMapVacuum (src/backend/storage/freespace/indexfsm.c:73)

## Notes and Other Information
- This function assumes that bottom-level FSM pages have already been updated with new free-space information
- The function performs a complete tree traversal starting from FSM_ROOT_ADDRESS
- Uses a dummy boolean variable for the recursive fsm_vacuum_page call as the return value is not needed at the root level
- Essential for maintaining FSM consistency after vacuum operations or other processes that update free space information
- Located in src/backend/storage/freespace/freespace.c:351-366