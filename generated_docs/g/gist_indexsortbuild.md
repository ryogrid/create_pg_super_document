# gist_indexsortbuild

## Location
src/backend/access/gist/gistbuild.c: 400 - 460

## Overview
Builds GiST index pages from bottom-up using pre-sorted tuples, implementing the core logic for sorted GiST index construction by managing level states and coordinating page creation.

## Definition
```c
static void gist_indexsortbuild(GISTBuildState *state)
```

## Detailed Description
This function implements the bottom-up construction phase of sorted GiST index builds. After tuples have been sorted by tuplesort, this function retrieves them in sorted order and builds index pages level by level, starting from leaf pages and progressing upward until a complete tree structure is established.

The function uses a hierarchical approach with GistSortedBuildLevelState structures to manage each level of the index tree. It begins by allocating space for the root page (block 0) and initializing the bulk write infrastructure for efficient page writing. The algorithm processes sorted tuples sequentially, adding them to leaf pages and automatically handling page splits and parent-level updates.

Key aspects of the algorithm include:
- Managing page batches to optimize I/O operations
- Automatically creating parent levels as leaf pages fill up
- Using bulk writing for improved storage manager performance
- Handling the special case of root page creation and finalization

## Parameters / Member Variables
- `state`: Pointer to GISTBuildState containing build context, sort state, index relations, and memory management information

## Dependencies
- Functions called/Symbols referenced:
  - [smgr_bulk_start_rel](../s/smgr_bulk_start_rel.md): Initialize bulk writing for the index relation
  - [GistSortedBuildLevelState](../G/GistSortedBuildLevelState.md): Level state structure for managing page batches
  - [gistinitpage](gistinitpage.md): Initialize a new GiST page with appropriate flags
  - [tuplesort_getindextuple](../t/tuplesort_getindextuple.md): Retrieve next sorted tuple from tuplesort
  - [gist_indexsortbuild_levelstate_add](gist_indexsortbuild_levelstate_add.md): Add tuple to current level state
  - [gist_indexsortbuild_levelstate_flush](gist_indexsortbuild_levelstate_flush.md): Write completed pages and propagate to parent
  - [smgr_bulk_get_buf](../s/smgr_bulk_get_buf.md): Get buffer for bulk writing
  - [smgr_bulk_write](../s/smgr_bulk_write.md): Write page using bulk writer
  - [smgr_bulk_finish](../s/smgr_bulk_finish.md): Complete bulk writing operation
- Called from (representative examples):
  - [gistbuild](gistbuild.md): Main GiST build function during sorted build mode

## Notes and Other Information
- This function is only called when using GIST_SORTED_BUILD mode with pre-sorted tuples
- The algorithm reserves block 0 for the root page which is written last after the tree structure is complete
- Uses bulk writing infrastructure for optimal I/O performance during large index builds
- Memory context resets after each tuple prevent memory accumulation during processing
- The hierarchical level state management allows for efficient bottom-up tree construction
- Parent levels are created dynamically as child levels become full, ensuring balanced tree growth
- The final root page writing ensures the index becomes immediately usable after construction