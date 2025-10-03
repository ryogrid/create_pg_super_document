# bottomup_sort_and_shrink

## Location
[src/backend/access/heap/heapam.c:8653-8781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8653-L8781)

## Overview
A helper function for heap_index_delete_tuples() that sorts and optimizes the deltids array for bottom-up deletion processing, applying sophisticated heuristics to maximize deletion efficiency.

## Definition
```c
static int bottomup_sort_and_shrink(TM_IndexDeleteOp *delstate)
```

## Detailed Description
This function performs comprehensive optimization of the deletion array for bottom-up index deletion processing. It groups heap TIDs by block, applies power-of-two bucketing to normalize promising TID counts, sorts blocks by deletion potential, and shrinks the array to focus on the most promising blocks.

Key processing steps:

1. **Block Grouping**: Groups TIDs from deltids by heap block number, calculating per-block statistics (ntids, npromisingtids)

2. **Power-of-Two Bucketing**: Normalizes npromisingtids values using power-of-two rounding (minimum 4) to reduce noise and enable locality-based tie-breaking

3. **Multi-Level Sorting**: Applies sophisticated sorting via bottomup_sort_and_shrink_cmp():
   - Primary: npromisingtids (descending - most promising first)
   - Secondary: ntids (descending, with power-of-two bucketing)
   - Tertiary: heap block number (ascending - spatial locality)

4. **Array Shrinking**: Limits processing to BOTTOMUP_MAX_NBLOCKS most promising blocks, often reducing array size significantly

5. **Reordering**: Reconstructs deltids array in optimal processing order

The power-of-two bucketing scheme is crucial for balancing deletion efficiency with spatial locality, treating small differences in promising TID counts as noise while preserving meaningful distinctions.

## Parameters / Member Variables
- `delstate`: Pointer to TM_IndexDeleteOp structure containing the deltids array to optimize and related state

## Dependencies
- Functions called/Symbols referenced:
  - [bottomup_sort_and_shrink_cmp](bottomup_sort_and_shrink_cmp.md)
  - [bottomup_nblocksfavorable](bottomup_nblocksfavorable.md)  
  - qsort
  - [palloc](../p/palloc.md), pfree, memcpy
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - BlockNumberIsValid
  - Min
  - [IndexDeleteCounts](../I/IndexDeleteCounts.md), TM_IndexDelete, TM_IndexStatus (structure types)
  - BOTTOMUP_MAX_NBLOCKS (constant)
- Called from (representative examples):
  - [heap_index_delete_tuples](../h/heap_index_delete_tuples.md)

## Notes and Other Information
- Assumes input deltids array is already sorted in TID order
- Returns number of "favorable" blocks (contiguous/nearly-contiguous blocks at start of processing order)
- Uses power-of-two bucketing to ignore small differences in npromisingtids (treated as noise)
- Handles npromisingtids ≤ 4 specially by rounding up to 4
- Often shrinks deltids array to small fraction of original size by focusing on most promising blocks
- The bucketing scheme enables heap locality factors to influence processing order without sacrificing deletion efficiency
- Allocates temporary arrays (blockgroups, reordereddeltids) that are freed before return
- Located in src/backend/access/heap/heapam.c:8653-8781

## Simplified Source

```c
static int bottomup_sort_and_shrink(TM_IndexDeleteOp *delstate)
{
    IndexDeleteCounts *blockgroups;
    TM_IndexDelete *reordereddeltids;
    BlockNumber curblock = InvalidBlockNumber;
    int nblockgroups = 0;
    int ncopied = 0;
    int nblocksfavorable = 0;

    Assert(delstate->bottomup);
    Assert(delstate->ndeltids > 0);

    // Step 1: Group TIDs by heap block and count statistics
    blockgroups = palloc(sizeof(IndexDeleteCounts) * delstate->ndeltids);

    for (int i = 0; i < delstate->ndeltids; i++)
    {
        TM_IndexDelete *ideltid = &delstate->deltids[i];
        TM_IndexStatus *istatus = delstate->status + ideltid->id;
        ItemPointer htid = &ideltid->tid;
        bool promising = istatus->promising;

        if (curblock != ItemPointerGetBlockNumber(htid))
        {
            // Start new block group
            nblockgroups++;
            curblock = ItemPointerGetBlockNumber(htid);

            blockgroups[nblockgroups - 1].ifirsttid = i;
            blockgroups[nblockgroups - 1].ntids = 1;
            blockgroups[nblockgroups - 1].npromisingtids = 0;
        }
        else
        {
            // Add to current block group
            blockgroups[nblockgroups - 1].ntids++;
        }

        if (promising)
            blockgroups[nblockgroups - 1].npromisingtids++;
    }

    // Step 2: Apply power-of-two bucketing to normalize npromisingtids
    for (int b = 0; b < nblockgroups; b++)
    {
        IndexDeleteCounts *group = blockgroups + b;

        // Round up to power of 2, minimum 4, to reduce noise
        if (group->npromisingtids <= 4)
            group->npromisingtids = 4;
        else
            group->npromisingtids = pg_nextpower2_32((uint32) group->npromisingtids);
    }

    // Step 3: Sort groups by deletion potential and spatial locality
    qsort(blockgroups, nblockgroups, sizeof(IndexDeleteCounts),
          bottomup_sort_and_shrink_cmp);

    // Step 4: Shrink to most promising blocks only
    nblockgroups = Min(BOTTOMUP_MAX_NBLOCKS, nblockgroups);

    // Determine favorable block count for spatial locality
    nblocksfavorable = bottomup_nblocksfavorable(blockgroups, nblockgroups,
                                                 delstate->deltids);

    // Step 5: Reorder deltids array according to optimized group order
    reordereddeltids = palloc(delstate->ndeltids * sizeof(TM_IndexDelete));

    for (int b = 0; b < nblockgroups; b++)
    {
        IndexDeleteCounts *group = blockgroups + b;
        TM_IndexDelete *firstdtid = delstate->deltids + group->ifirsttid;

        memcpy(reordereddeltids + ncopied, firstdtid,
               sizeof(TM_IndexDelete) * group->ntids);
        ncopied += group->ntids;
    }

    // Copy back and update state
    memcpy(delstate->deltids, reordereddeltids,
           sizeof(TM_IndexDelete) * ncopied);
    delstate->ndeltids = ncopied;

    pfree(reordereddeltids);
    pfree(blockgroups);

    return nblocksfavorable;
}
```