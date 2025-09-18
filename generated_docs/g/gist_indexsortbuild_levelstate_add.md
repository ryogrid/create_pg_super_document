# gist_indexsortbuild_levelstate_add

## Location
[src/backend/access/gist/gistbuild.c:461-492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L461-L492)

## Overview
Adds a single index tuple to the current page in a level state during sorted GiST index construction, managing page overflow by advancing to the next page or triggering a flush when pages become full.

## Definition
```c
static void gist_indexsortbuild_levelstate_add(GISTBuildState *state, GistSortedBuildLevelState *levelstate, IndexTuple itup)
```

## Detailed Description
This function manages the addition of individual index tuples to pages within a level state during bottom-up GiST index construction. The function implements intelligent page management by calculating space requirements and handling page overflow conditions.

When a tuple cannot fit on the current page, the function employs a two-tier overflow strategy:
1. If there are available page slots within the level state, it advances to the next page
2. If all page slots are occupied, it triggers a flush operation to write completed pages and create parent-level entries

The function preserves page type information (leaf vs. internal) when initializing new pages by copying flags from the previous page. This ensures consistency in the tree structure during bottom-up construction.

After determining the appropriate target page, the function uses gistfillbuffer to insert the tuple, which handles the low-level page layout and item management.

## Parameters / Member Variables
- `state`: Pointer to GISTBuildState containing build context and memory management
- `levelstate`: Pointer to GistSortedBuildLevelState managing the current tree level's pages
- `itup`: IndexTuple to be added to the current level

## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleSize: Calculate storage size required for the index tuple
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md): Determine available space on the current page
  - GistPageGetOpaque: Access GiST-specific page metadata and flags
  - [gist_indexsortbuild_levelstate_flush](gist_indexsortbuild_levelstate_flush.md): Write full pages and propagate entries to parent level
  - [gistinitpage](gistinitpage.md): Initialize a new GiST page with appropriate type flags
  - [gistfillbuffer](gistfillbuffer.md): Insert tuple into page using GiST page layout rules
- Called from (representative examples):
  - [gist_indexsortbuild](gist_indexsortbuild.md): Main sorted build loop processing sorted tuples
  - [gist_indexsortbuild_levelstate_flush](gist_indexsortbuild_levelstate_flush.md): Recursive calls when adding union tuples to parent levels

## Notes and Other Information
- The function ignores fill factor considerations, packing pages as tightly as possible for optimal space utilization
- Page type preservation ensures that leaf/internal page distinctions are maintained throughout the build process  
- The batch processing approach using GIST_SORTED_BUILD_PAGE_NUM pages optimizes I/O by reducing the frequency of flush operations
- Memory allocation for new pages uses palloc0 to ensure clean initialization
- The function handles the transition from leaf pages to internal pages automatically during tree construction
- Overflow management ensures that the build process can handle arbitrarily large datasets without memory exhaustion