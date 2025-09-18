# gist_point_consistent_internal

## Location
src/backend/access/gist/gistproc.c: 1287 - 1329

## Overview
A static utility function that implements the core consistency logic for point queries in GiST indexes, handling different spatial relationship strategies with appropriate logic for leaf versus internal nodes.

## Definition
```c
static bool gist_point_consistent_internal(StrategyNumber strategy, bool isLeaf, BOX *key, Point *query)
```

## Detailed Description
This function serves as the internal implementation for point consistency checking in GiST indexes, determining whether a given index entry could potentially contain results for a spatial query. It supports various spatial relationship strategies (left-of, right-of, above, below, same-as) and adapts its logic based on whether it's examining a leaf node (actual point data) or an internal node (bounding box).

The function uses floating-point comparison macros (FPlt, FPgt, FPeq, etc.) for robust geometric comparisons. For the 'same' strategy, it distinguishes between leaf and internal nodes: at leaf level, it performs exact point equality; at internal level, it checks if the query point lies within the bounding box. This dual-mode operation is essential for correct index traversal during spatial queries.

The strategy-based approach allows the same function to handle multiple types of spatial queries efficiently, making it a core component of PostgreSQL's geometric indexing infrastructure.

## Parameters / Member Variables
- `strategy`: Strategy number indicating the type of spatial relationship being queried (left, right, above, below, same)
- `isLeaf`: Boolean flag indicating whether this is a leaf node (true) or internal node (false)
- `key`: Bounding box representing the index entry being tested
- `query`: Query point being searched for

## Dependencies
- Functions called/Symbols referenced:
  - [FPlt](../F/FPlt.md): Floating-point less-than comparison macro
  - [FPgt](../F/FPgt.md): Floating-point greater-than comparison macro  
  - [FPeq](../F/FPeq.md): Floating-point equality comparison macro
  - [FPle](../F/FPle.md): Floating-point less-than-or-equal comparison macro
  - [FPge](../F/FPge.md): Floating-point greater-than-or-equal comparison macro
  - `RTLeftStrategyNumber`: Strategy constant for left-of queries
  - `RTRightStrategyNumber`: Strategy constant for right-of queries
  - `RTAboveStrategyNumber`: Strategy constant for above queries
  - `RTBelowStrategyNumber`: Strategy constant for below queries
  - `RTSameStrategyNumber`: Strategy constant for equality queries
  - `elog`: Error logging for unrecognized strategies
- Called from (representative examples):
  - [gist_point_consistent](gist_point_consistent.md): Main point consistency checking function

## Notes and Other Information
- Implements different logic for leaf vs. internal nodes, especially for the 'same' strategy
- Uses robust floating-point comparison macros to handle precision issues
- Supports the full range of 2D spatial relationship queries
- Essential building block for spatial query processing in PostgreSQL's GiST point indexes
- At leaf level, performs exact coordinate matching for equality queries
- At internal level, performs containment checking for equality queries
- Includes comprehensive error handling for invalid strategy numbers
- Part of the R-tree spatial indexing implementation adapted for PostgreSQL's GiST framework