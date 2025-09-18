# spg_box_quad_picksplit

## Location
src/backend/utils/adt/geo_spgist.c: 441 - 507

## Overview
The SP-GiST pick-split function for box geometric types that partitions a collection of boxes into quadrants by calculating a central 4D point as the median of all box coordinates.

## Definition
```c
Datum spg_box_quad_picksplit(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the "pick-split" operation for SP-GiST indexes on PostgreSQL's BOX geometric type using a quadtree partitioning strategy. When an internal node becomes full and needs to be split, this function determines how to partition the existing boxes into 16 quadrants based on a calculated centroid.

The function operates by:
1. Extracting coordinate arrays (lowXs, highXs, lowYs, highYs) from all input boxes
2. Sorting each coordinate array to find median values
3. Creating a centroid box using the median coordinates
4. Assigning each input box to one of 16 quadrants relative to the centroid
5. Setting up the output structure with the centroid as prefix and quadrant assignments

The 4D coordinate system considers each box as having four coordinates: low.x, high.x, low.y, high.y, allowing for precise spatial partitioning.

## Parameters / Member Variables
- `in` (spgPickSplitIn*): Input structure containing the array of datums (boxes) to be split and the number of tuples
- `out` (spgPickSplitOut*): Output structure where split results are stored including prefix, node mappings, and leaf data
- `centroid` (BOX*): The calculated median box that serves as the splitting point
- `median` (int): Index position of the median value in sorted arrays
- `lowXs/highXs/lowYs/highYs` (float8*): Arrays holding sorted coordinate values for median calculation

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [DatumGetBoxP](../D/DatumGetBoxP.md) (converts Datum to BOX pointer)
  - qsort (standard library sorting function)
  - [compareDoubles](../c/compareDoubles.md) (comparator function for sorting coordinates)
  - [getQuadrant](../g/getQuadrant.md) (determines quadrant based on centroid and box)
  - [BoxPGetDatum](../B/BoxPGetDatum.md) (converts BOX pointer to Datum)
  - PG_RETURN_VOID (PostgreSQL function return macro)
- Called from (representative examples):
  - SP-GiST index split operations
  - Spatial index rebalancing routines

## Notes and Other Information
- Creates 16 child nodes corresponding to all possible quadrant combinations in 4D space
- Uses median-based splitting strategy to ensure balanced tree structure
- Does not use node labels (nodeLabels set to NULL)
- Memory allocation uses PostgreSQL's palloc for automatic cleanup
- Located in src/backend/utils/adt/geo_spgist.c:441-507
- The split strategy considers boxes as 4D points to enable more precise spatial partitioning than traditional 2D approaches