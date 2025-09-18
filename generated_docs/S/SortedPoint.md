# SortedPoint

## Location
src/backend/access/spgist/spgkdtreeproc.c: 78 - 82

## Overview
SortedPoint is a utility structure used in PostgreSQL's SP-GiST k-d tree implementation to associate geometric Point data with their original indices during sorting operations.

## Definition


## Detailed Description
The SortedPoint structure is used within the SP-GiST (Space-Partitioned Generalized Search Tree) k-d tree implementation for geometric point indexing. It serves as a wrapper that pairs a geometric Point with its original index position, enabling the sorting algorithms to maintain the relationship between sorted points and their positions in the original input array. This is essential for the  function, which needs to split point data while preserving the mapping between the sorted order and the original tuple positions.

The structure is specifically designed to support the k-d tree's alternating splitting strategy, where points are sorted by x-coordinate on even levels and y-coordinate on odd levels of the tree. The comparison functions  and  operate on SortedPoint structures to enable this dimensional alternation.

## Parameters / Member Variables
- : Pointer to the geometric Point structure containing x and y coordinates
- : Integer index representing the original position of this point in the input array

## Dependencies
- Functions called/Symbols referenced:
  - Point (geometric point type from utils/geo_decls.h)
- Called from (representative examples):
  - x_cmp (comparison function for x-coordinate sorting)
  - y_cmp (comparison function for y-coordinate sorting)
  - spg_kd_picksplit (main split function that uses arrays of SortedPoint)

## Notes and Other Information
- This structure is used internally within the SP-GiST k-d tree implementation and is not exposed as a public API
- The structure is allocated as an array in  using 
- The sorting is performed using the standard  function with custom comparison functions
- The design allows the k-d tree to maintain balanced splits while preserving the original tuple ordering information needed for index construction
- Located in src/backend/access/spgist/spgkdtreeproc.c, part of PostgreSQL's geometric indexing infrastructure