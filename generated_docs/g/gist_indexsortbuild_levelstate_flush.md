# gist_indexsortbuild_levelstate_flush

## Location
src/backend/access/gist/gistbuild.c: 493 - 625

## Overview
Flushes completed pages from a level state during sorted GiST index construction, writing pages to storage and propagating union tuples to parent levels to maintain the tree structure.

## Definition
```c
static void gist_indexsortbuild_levelstate_flush(GISTBuildState *state, GistSortedBuildLevelState *levelstate)
```

## Detailed Description
This function implements the critical flush operation for sorted GiST index construction, responsible for converting accumulated tuples in a level state into persistent index pages and maintaining the hierarchical tree structure. The function operates in several distinct phases:

**Tuple Extraction and Consolidation**: The function extracts all index tuples from the current level's pages using gistextractpage, consolidating them into a single vector for processing. When multiple pages are present, it uses gistjoinvector to merge tuples from all pages.

**Split Decision Logic**: For single-page scenarios, the function creates a simple split layout with a union tuple representing all contained tuples. For multi-page scenarios, it invokes the GiST split algorithm (gistSplit) to determine optimal page partitioning based on the index's split strategy.

**Page Creation and Writing**: For each partition in the split result, the function creates a new index page, populates it with the assigned tuples, and writes it to storage using the bulk writer. Each page receives a unique block number and appropriate LSN for recovery purposes.

**Parent Level Management**: Union tuples representing each written page are propagated to the parent level. If no parent exists (indicating this is the root level), a new parent level is created automatically.

**Tree Structure Maintenance**: The function maintains right-links between pages at the same level for debugging purposes and ensures proper parent-child relationships throughout the tree hierarchy.

## Parameters / Member Variables
- `state`: Pointer to GISTBuildState containing build context, memory management, and bulk writing infrastructure
- `levelstate`: Pointer to GistSortedBuildLevelState containing the pages and metadata for the current tree level

## Dependencies
- Functions called/Symbols referenced:
  - gistextractpage: Extract all tuples from a completed page into an array
  - gistjoinvector: Merge tuple arrays from multiple pages into a single vector
  - gistSplit: Apply GiST split algorithm to determine optimal page partitioning
  - gistunion: Create union tuple representing all tuples in a page
  - gistfillitupvec: Convert tuple array into page layout format
  - smgr_bulk_get_buf: Obtain buffer for bulk writing operations
  - gistinitpage: Initialize new GiST page with appropriate type flags
  - PageAddItem: Add individual tuples to the target page
  - smgr_bulk_write: Write completed page to storage via bulk writer
  - gist_indexsortbuild_levelstate_add: Add union tuples to parent level recursively
- Called from (representative examples):
  - gist_indexsortbuild: Main build loop when processing sorted tuples
  - gist_indexsortbuild_levelstate_add: Recursive calls when page batches become full

## Notes and Other Information
- The function handles both leaf and internal page creation by preserving page type flags from the original pages
- Right-link management creates a chain through all pages at the same level for debugging, though these links are not used during normal index operations
- Memory context switching ensures that temporary allocations during split processing don't accumulate
- The recursive parent creation mechanism automatically builds the tree from bottom to top
- Split algorithm selection depends on the GiST index configuration and can use various partitioning strategies
- Bulk writing optimization significantly improves I/O performance compared to traditional page-by-page writing
- The function ensures atomic page creation - either all pages in a split are created successfully or none are
- LSN assignment ensures proper WAL integration and recovery support for the constructed pages