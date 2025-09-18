# gist_box_picksplit

## Location
src/backend/access/gist/gistproc.c: 495 - 711

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