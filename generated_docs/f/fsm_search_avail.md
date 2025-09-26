# fsm_search_avail

## Location
[src/backend/storage/freespace/fsmpage.c:158-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/fsmpage.c#L158-L312)

## Overview
The fsm_search_avail function searches for a slot within a Free Space Map page that has at least the specified minimum free space available, using an efficient binary tree traversal algorithm.

## Definition
```c
int fsm_search_avail(Buffer buf, uint8 minvalue, bool advancenext, bool exclusive_lock_held)
```

## Detailed Description
This function implements an intelligent search algorithm for finding available space within a Free Space Map page. It uses a sophisticated "search triangle" approach that ensures logarithmic search time complexity while avoiding revisiting previously searched areas.

The algorithm works in three main phases:
1. **Quick Root Check**: First verifies that the root node indicates sufficient space exists on the page
2. **Upward Search**: Starting from a hint position (fp_next_slot), moves right and climbs up the tree, expanding the search triangle until finding a node with sufficient space
3. **Downward Descent**: From the found node, descends to a leaf node, preferring left children when both have sufficient space

The search uses a clever technique of moving right then up, which ensures the search never backtracks and covers progressively larger areas of the tree. The "move right" operation wraps around at level boundaries using the rightneighbor function.

The function includes corruption detection and recovery - if tree invariants are violated (parent promises space but neither child delivers), it rebuilds the page and restarts the search.

## Parameters / Member Variables
- `buf`: Buffer containing the Free Space Map page to search
- `minvalue`: Minimum free space value required (uint8)
- `advancenext`: If true, sets fp_next_slot to the slot after the found slot; if false, sets it to the found slot
- `exclusive_lock_held`: If true, caller already holds exclusive lock, avoiding lock upgrade overhead

## Dependencies
- Functions called/Symbols referenced:
  - `[BufferGetPage](../B/BufferGetPage.md)`: Extracts page from buffer
  - `[PageGetContents](../P/PageGetContents.md)`: Gets FSMPage structure from page
  - `FSMPage`: Type representing Free Space Map page data
  - `LeafNodesPerPage`: Number of leaf nodes per page
  - `NonLeafNodesPerPage`: Number of non-leaf nodes per page  
  - `parentof`: Macro to calculate parent node index
  - `[rightneighbor](../r/rightneighbor.md)`: Function to find right neighbor with level wrapping
  - `leftchild`: Macro to calculate left child node index
  - `NodesPerPage`: Total number of nodes per page
  - `[BufferGetTag](../B/BufferGetTag.md)`: Gets buffer tag information for error reporting
  - `[LockBuffer](../L/LockBuffer.md)`: Buffer locking functions
  - `[fsm_rebuild_page](fsm_rebuild_page.md)`: Rebuilds corrupted FSM page
  - `[MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)`: Marks buffer as dirty after corruption fix
- Called from (representative examples):
  - `[fsm_set_and_search](fsm_set_and_search.md)`: Combined set and search operation
  - `[fsm_search](fsm_search.md)`: Higher-level search across multiple FSM pages

## Notes and Other Information
- Returns slot number on success, or -1 if no suitable slot found
- Requires at least shared lock on the page, but may upgrade to exclusive if corruption is detected
- Uses fp_next_slot as a search hint to avoid repeatedly returning the same slot
- The search algorithm ensures O(log N) complexity for N pages
- Includes detailed corruption detection and automatic recovery via page rebuild
- Updates fp_next_slot even under shared lock for better hint performance
- The "search triangle" approach ensures comprehensive coverage without backtracking
- Handles wraparound at tree level boundaries gracefully
- Part of PostgreSQL's Free Space Map system for efficient space allocation
- The algorithm is designed to distribute load across available slots rather than always returning the first match