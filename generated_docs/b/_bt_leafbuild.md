# _bt_leafbuild

## Location
[src/backend/access/nbtree/nbtsort.c:536-576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L536-L576)

## Overview
Completes B-tree index construction by sorting collected tuples and building the actual B-tree structure from the populated spools.

## Definition

```c
static void
_bt_leafbuild(BTSpool *btspool, BTSpool *btspool2)
```
## Detailed Description
 is the culminating function in B-tree index construction that transforms the collected and temporarily stored index tuples into the final B-tree structure. The function operates in several distinct phases:

1. **Sort Execution**: Calls  on both the primary spool (and secondary spool if present) to complete the sorting of all collected index tuples. This ensures tuples are properly ordered according to the index's key columns before tree construction begins.

2. **Write State Initialization**: Sets up a  structure that coordinates the writing of sorted tuples into B-tree pages. This includes:
   - Creating scan keys for tuple insertion operations
   - Determining image equality properties for the index
   - Reserving space for the B-tree metapage

3. **Progress Reporting**: Updates progress statistics at each phase to provide visibility into the index construction process, reporting sort completion and leaf page loading phases.

4. **Tree Construction**: Delegates the actual page construction to , which reads the sorted tuples from both spools and writes them into properly formatted B-tree leaf and internal pages.

The function handles both single-spool scenarios (for non-unique indexes or unique indexes without dead tuples) and dual-spool scenarios (for unique indexes that encountered dead tuples during scanning). The secondary spool, when present, contains dead tuples that were kept separate during scanning to avoid interfering with uniqueness checking.

## Parameters
- : Primary spool containing live index tuples to be inserted into the B-tree
- : Optional secondary spool containing dead tuples (may be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  -  - Completes sorting of tuples in both spools
  -  - Creates scan key structure for index operations
  -  - Determines image equality properties for the index
  -  - Constructs actual B-tree pages from sorted tuples
  -  - Reports progress through different phases
  - ,  - Optional performance statistics (when compiled with BTREE_BUILD_STATS)
  - ,  - Data structures for coordination
- Called from:
  -  - Main index construction function

## Notes and Other Information
- This function represents the transition from tuple collection/sorting to actual B-tree page construction
- Handles both unique and non-unique indexes with appropriate spool management
- Progress reporting provides visibility into potentially long-running sort and load operations
- The function ensures proper ordering of tuples before tree construction to maintain B-tree invariants
- Integrates with PostgreSQL's statistics system for performance monitoring when enabled
- The write state initialization includes important optimizations like image equality detection for better page utilization

## Simplified Source

```c
static void
_bt_leafbuild(BTSpool *btspool, BTSpool *btspool2)
{
    BTWriteState wstate;

    // Execute the sort on primary spool
    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                 PROGRESS_BTREE_PHASE_PERFORMSORT_1);
    tuplesort_performsort(btspool->sortstate);

    // Execute sort on secondary spool if present
    if (btspool2) {
        pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                     PROGRESS_BTREE_PHASE_PERFORMSORT_2);
        tuplesort_performsort(btspool2->sortstate);
    }

    // Initialize write state for B-tree construction
    wstate.heap = btspool->heap;
    wstate.index = btspool->index;
    wstate.inskey = _bt_mkscankey(wstate.index, NULL);

    // Set up image equality optimization
    wstate.inskey->allequalimage = _bt_allequalimage(wstate.index, true);

    // Reserve space for metapage
    wstate.btws_pages_alloced = BTREE_METAPAGE + 1;

    // Begin leaf page loading phase
    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                 PROGRESS_BTREE_PHASE_LEAF_LOAD);

    // Load sorted tuples into B-tree structure
    _bt_load(&wstate, btspool, btspool2);
}
```