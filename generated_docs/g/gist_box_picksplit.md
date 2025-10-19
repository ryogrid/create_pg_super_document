# gist_box_picksplit

## Location
[src/backend/access/gist/gistproc.c:495-711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L495-L711)

## Overview
A PostgreSQL function that implements the double sorting-based node splitting algorithm for GiST indexes on geometric box data types, determining the optimal way to split a node when it becomes full.

## Definition
```c
Datum gist_box_picksplit(PG_FUNCTION_ARGS)
```

## Detailed Description
The `gist_box_picksplit` function implements a sophisticated node splitting algorithm for GiST (Generalized Search Tree) indexes specifically designed for geometric box data types. This function is called when a GiST node becomes full and needs to be split into two nodes.

The algorithm uses a "double sorting" approach that considers splits along both X and Y axes to find the optimal division:

1. **Axis Iteration**: For each axis (X and Y), it projects all entries as intervals and considers various ways to split them into two groups
2. **Interval Analysis**: It creates two sorted arrays of intervals (by lower and upper bounds) and systematically examines potential split points
3. **Split Evaluation**: Uses `g_box_consider_split` to evaluate each potential split based on overlap minimization and other quality criteria
4. **Entry Classification**: After selecting the best split, it classifies entries into three groups:
   - Left group (must go to left node)
   - Right group (must go to right node) 
   - Common entries (can go to either node without affecting axis overlap)
5. **Common Entry Distribution**: Distributes common entries to minimize penalty and maintain balance

The algorithm is based on the research paper "A new double sorting-based node splitting algorithm for R-tree" by A. Korotkov.

## Parameters / Member Variables
- Function uses PostgreSQL's `PG_FUNCTION_ARGS` macro which provides:
  - `entryvec`: Vector of GiST entries to be split (GistEntryVector*)
  - `v`: Split result structure to be filled (GIST_SPLITVEC*)

## Dependencies
- Functions called/Symbols referenced:
  - [GistEntryVector](../G/GistEntryVector.md), GIST_SPLITVEC, ConsiderSplitContext, BOX, SplitInterval, CommonEntry (data structures)
  - [DatumGetBoxP](../D/DatumGetBoxP.md) (extracts box from datum)
  - [adjustBox](../a/adjustBox.md) (adjusts bounding box)
  - [g_box_consider_split](g_box_consider_split.md) (evaluates split candidates)
  - [interval_cmp_lower](../i/interval_cmp_lower.md), interval_cmp_upper (comparison functions for sorting)
  - [float8_eq](../f/float8_eq.md), float8_lt, float8_le, float8_gt, float8_ge (floating-point comparisons)
  - [fallbackSplit](../f/fallbackSplit.md) (fallback splitting strategy)
  - qsort (standard library sorting function)
  - FirstOffsetNumber, OffsetNumberNext (offset number utilities)
- Called from (representative examples):
  - No direct references found (likely called via GiST operator class function pointers)

## Notes and Other Information
- This function is a critical component of PostgreSQL's spatial indexing infrastructure
- The algorithm provides better performance than traditional R-tree splitting by minimizing overlap between resulting nodes
- Returns a Datum (PostgreSQL's generic data type wrapper) containing the split result
- Uses sophisticated memory management with palloc/palloc0 for PostgreSQL's memory contexts
- The double sorting approach significantly improves the quality of splits compared to simpler algorithms like linear or quadratic splitting
- The algorithm handles both points and boxes as it projects them as intervals for analysis
- Fallback to a simpler splitting method if no acceptable split is found
- The splitting quality directly impacts query performance of spatial indexes

## Simplified Source

```c
Datum gist_box_picksplit(PG_FUNCTION_ARGS)
{
    GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
    GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
    OffsetNumber i, maxoff;
    ConsiderSplitContext context;
    BOX *box, *leftBox, *rightBox;
    int dim, commonEntriesCount;
    SplitInterval *intervalsLower, *intervalsUpper;
    CommonEntry *commonEntries;
    int nentries;

    memset(&context, 0, sizeof(ConsiderSplitContext));

    maxoff = entryvec->n - 1;
    nentries = context.entriesCount = maxoff - FirstOffsetNumber + 1;

    // Allocate interval arrays for both axes
    intervalsLower = palloc(nentries * sizeof(SplitInterval));
    intervalsUpper = palloc(nentries * sizeof(SplitInterval));

    // Calculate overall bounding box of all entries
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        box = DatumGetBoxP(entryvec->vector[i].key);
        if (i == FirstOffsetNumber)
            context.boundingBox = *box;
        else
            adjustBox(&context.boundingBox, box);
    }

    // Try splits along both X and Y axes
    context.first = true;
    for (dim = 0; dim < 2; dim++) {
        // Project entries as intervals on current axis
        for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
            box = DatumGetBoxP(entryvec->vector[i].key);
            if (dim == 0) {
                intervalsLower[i - FirstOffsetNumber].lower = box->low.x;
                intervalsLower[i - FirstOffsetNumber].upper = box->high.x;
            } else {
                intervalsLower[i - FirstOffsetNumber].lower = box->low.y;
                intervalsLower[i - FirstOffsetNumber].upper = box->high.y;
            }
        }

        // Create sorted arrays by lower and upper bounds
        memcpy(intervalsUpper, intervalsLower, sizeof(SplitInterval) * nentries);
        qsort(intervalsLower, nentries, sizeof(SplitInterval), interval_cmp_lower);
        qsort(intervalsUpper, nentries, sizeof(SplitInterval), interval_cmp_upper);

        // Find optimal split points by examining interval combinations
        // (Implementation details simplified - involves complex iteration
        // through sorted intervals to find best left/right split boundaries)

        // Two main loops:
        // 1. Iterate over right group lower bounds, find minimal left upper bounds
        // 2. Iterate over left group upper bounds, find maximal right lower bounds
        // Each combination is evaluated using g_box_consider_split()
    }

    // Use fallback split if no acceptable split found
    if (context.first) {
        fallbackSplit(entryvec, v);
        PG_RETURN_POINTER(v);
    }

    // Allocate result arrays
    v->spl_left = palloc(nentries * sizeof(OffsetNumber));
    v->spl_right = palloc(nentries * sizeof(OffsetNumber));
    v->spl_nleft = 0;
    v->spl_nright = 0;

    // Allocate bounding boxes for both groups
    leftBox = palloc0(sizeof(BOX));
    rightBox = palloc0(sizeof(BOX));

    // Classify and distribute entries into left, right, and common groups
    // Common entries are distributed to minimize penalty
    commonEntriesCount = 0;
    commonEntries = palloc(nentries * sizeof(CommonEntry));

    // (Additional logic for entry classification and distribution)
    // Final step: compute union bounding boxes for both result groups

    PG_RETURN_POINTER(v);
}
```