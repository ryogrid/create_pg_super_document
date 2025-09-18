# spg_quad_picksplit

## Location
src/backend/access/spgist/spgquadtreeproc.c: 169 - 226

## Overview
An SP-GiST operator function that implements the picksplit operation for quadtree-based spatial indexing, determining how to split a node when it becomes full by calculating a centroid point and partitioning tuples into four quadrants.

## Definition


## Detailed Description
The  function is a core component of PostgreSQL's SP-GiST quadtree implementation for spatial data indexing. When an internal node in the quadtree becomes full and needs to be split, this function:

1. **Calculates a centroid point**: The function can use either the median values (when USE_MEDIAN is defined) or average values of x and y coordinates from all input points as the centroid
2. **Creates a prefix datum**: The centroid becomes the prefix datum stored in the internal node
3. **Partitions tuples into quadrants**: Each input tuple is assigned to one of four child nodes based on which quadrant it falls into relative to the centroid
4. **Sets up output structure**: Populates the  structure with the necessary information for the split operation

The quadtree partitioning divides the 2D space into four quadrants (NE, NW, SE, SW) around the centroid point, enabling efficient spatial queries.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (spgPickSplitIn*): Input structure containing tuples to be split
  -  (spgPickSplitOut*): Output structure to be populated with split results

## Dependencies
- Functions called/Symbols referenced:
  - [spgPickSplitIn](spgPickSplitIn.md)
  - [spgPickSplitOut](spgPickSplitOut.md)
  - [Point](../P/Point.md)
  - [DatumGetPointP](../D/DatumGetPointP.md)
  - [x_cmp](../x/x_cmp.md)
  - [y_cmp](../y/y_cmp.md)
  - qsort
  - [getQuadrant](../g/getQuadrant.md)
  - [PointPGetDatum](../P/PointPGetDatum.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - SP-GiST framework (via function pointers in operator class)

## Notes and Other Information
- This function supports two centroid calculation methods: median-based (USE_MEDIAN) and average-based
- The function creates exactly 4 child nodes representing the four quadrants of a 2D space
- No node labels are used (nodeLabels is set to NULL) as quadrant identification is implicit
- The function is typically registered as part of an SP-GiST operator class for Point data types
- Uses 1-based quadrant numbering from getQuadrant() but converts to 0-based indexing for internal use
- Part of PostgreSQL's extensible indexing framework, specifically designed for spatial data structures