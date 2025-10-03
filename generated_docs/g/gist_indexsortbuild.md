# gist_indexsortbuild

## Location
[src/backend/access/gist/gistbuild.c:400-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L400-L460)

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

## Simplified Source

```c
static void
gist_indexsortbuild(GISTBuildState *state)
{
    IndexTuple itup;
    GistSortedBuildLevelState *levelstate;
    BulkWriteBuffer rootbuf;

    // Reserve block 0 for root page and initialize bulk writing
    state->pages_allocated = 1;
    state->bulkstate = smgr_bulk_start_rel(state->indexrel, MAIN_FORKNUM);

    // Initialize first leaf level state
    levelstate = palloc0(sizeof(GistSortedBuildLevelState));
    levelstate->pages[0] = palloc(BLCKSZ);
    levelstate->parent = NULL;
    gistinitpage(levelstate->pages[0], F_LEAF);

    // Process all sorted tuples, building pages bottom-up
    while ((itup = tuplesort_getindextuple(state->sortstate, true)) != NULL)
    {
        gist_indexsortbuild_levelstate_add(state, levelstate, itup);
        MemoryContextReset(state->giststate->tempCxt);
    }

    // Flush all partially filled pages, working up the tree
    while (levelstate->parent != NULL || levelstate->current_page != 0)
    {
        GistSortedBuildLevelState *parent;

        // Flush current level and move to parent
        gist_indexsortbuild_levelstate_flush(state, levelstate);
        parent = levelstate->parent;

        // Free current level resources
        for (int i = 0; i < GIST_SORTED_BUILD_PAGE_NUM; i++)
            if (levelstate->pages[i])
                pfree(levelstate->pages[i]);
        pfree(levelstate);
        levelstate = parent;
    }

    // Write the final root page
    PageSetLSN(levelstate->pages[0], GistBuildLSN);
    rootbuf = smgr_bulk_get_buf(state->bulkstate);
    memcpy(rootbuf, levelstate->pages[0], BLCKSZ);
    smgr_bulk_write(state->bulkstate, GIST_ROOT_BLKNO, rootbuf, true);

    // Final cleanup
    pfree(levelstate);
    smgr_bulk_finish(state->bulkstate);
}
```