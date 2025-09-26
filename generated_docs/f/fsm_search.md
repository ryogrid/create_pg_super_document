# fsm_search

## Location
src/backend/storage/freespace/freespace.c: 678 - 811

## Overview
Searches the FSM tree structure to find a heap page with at least the specified minimum amount of free space, handling inconsistencies and stale information through automatic correction and retry mechanisms.

## Definition
```c
static BlockNumber fsm_search(Relation rel, uint8 min_cat)
```

## Detailed Description
This function implements the core search algorithm for the Free Space Map, traversing the FSM tree from root to leaf to locate a heap page with sufficient free space. The search process is complex due to the need to handle concurrent updates, stale information, and boundary conditions.

The algorithm works as follows:
1. **Tree Traversal**: Starts at the FSM root and descends the tree level by level
2. **Page Search**: At each level, searches for a slot meeting the minimum space requirement
3. **Descent Logic**: If found and not at bottom level, descends to the child page
4. **Bottom Level**: At the bottom level, converts FSM slot to heap block number and validates existence
5. **Correction Mechanism**: When encountering stale information, updates parent pages and restarts from root
6. **Safety Limits**: Includes restart limiting to prevent infinite loops

The function handles several edge cases:
- Pages beyond the end of the relation (updates FSM and restarts)
- Stale upper-level information (corrects parent and restarts)
- Race conditions during concurrent updates
- Emergency brake for excessive restart loops

## Parameters / Member Variables
- `rel`: Relation to search for free space
- `min_cat`: Minimum free space category required (0-255 scale)

## Dependencies
- Functions called/Symbols referenced:
  - fsm_readbuf
  - fsm_search_avail
  - fsm_get_max_avail
  - fsm_get_heap_blk
  - fsm_does_block_exist
  - fsm_set_avail
  - fsm_get_child
  - fsm_get_parent
  - fsm_set_and_search
  - BufferGetPage
  - LockBuffer
  - UnlockReleaseBuffer
  - ReleaseBuffer
  - MarkBufferDirtyHint
- Called from (representative examples):
  - GetPageWithFreeSpace
  - RecordAndGetPageWithFreeSpace

## Notes and Other Information
- This is a static function, only accessible within the freespace.c file
- Implements a self-correcting search algorithm that handles stale FSM information
- Uses shared locks during search to allow concurrent readers
- Switches to exclusive locks only when updating stale information
- Includes protection against infinite loops with a 10,000 restart limit
- The function tolerates race conditions and eventual consistency through retry logic
- Performance is optimized by pinning buffers during descent when possible
- Handles the case where heap blocks referenced by FSM no longer exist
- The search is restart-based rather than backtrack-based for simplicity and correctness