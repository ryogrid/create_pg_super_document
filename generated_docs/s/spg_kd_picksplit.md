# spg_kd_picksplit

## Location
src/backend/access/spgist/spgkdtreeproc.c: 108 - 159

## Overview
A SP-GiST picksplit function that splits a node in a k-d tree by choosing a coordinate value as the splitting criterion and partitioning tuples into two child nodes based on that coordinate.

## Definition
```c
Datum spg_kd_picksplit(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the picksplit operation for SP-GiST k-d tree indexes on point data. It receives a collection of tuples to be split and determines how to partition them into two child nodes. The function alternates between splitting on X and Y coordinates based on the tree level (even levels split on Y, odd levels split on X).

The splitting process involves:
1. Sorting all input tuples by the appropriate coordinate (X or Y depending on level)
2. Finding the median point to use as the splitting coordinate
3. Partitioning tuples into two groups: those with coordinates less than the median go to the left child, others go to the right child
4. Setting up the output structure with the splitting coordinate as prefix and tuple mappings

The algorithm intentionally allows points with coordinates exactly equal to the splitting coordinate to fall into either child node, which helps maintain tree balance when dealing with duplicate coordinate values.

## Parameters / Member Variables
- `in`: Input structure containing tuples to split and context information
  - `in->nTuples`: Number of tuples to be split
  - `in->datums[]`: Array of point data to be partitioned
  - `in->level`: Current tree level (determines split dimension)
- `out`: Output structure to be populated with split results
  - `out->hasPrefix`: Set to true indicating a splitting coordinate prefix
  - `out->prefixDatum`: The coordinate value used for splitting
  - `out->nNodes`: Number of child nodes (always 2 for k-d trees)
  - `out->mapTuplesToNodes[]`: Mapping of input tuples to output nodes
  - `out->leafTupleDatums[]`: Point data for each tuple in leaf nodes

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointP](../D/DatumGetPointP.md)
  - qsort
  - [x_cmp](../x/x_cmp.md) (for sorting by X coordinate)
  - [y_cmp](../y/y_cmp.md) (for sorting by Y coordinate)
  - [Float8GetDatum](../F/Float8GetDatum.md)
  - [PointPGetDatum](../P/PointPGetDatum.md)
  - [palloc](../p/palloc.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - SP-GiST index operations (no direct references found in codebase)

## Notes and Other Information
- This function is part of the SP-GiST k-d tree operator class for geometric point data
- The splitting strategy alternates dimensions based on tree level to create a balanced k-d tree structure
- Points exactly on the splitting boundary may be assigned to either child, which is acceptable for the consistency function
- The function never triggers allTheSame logic due to its balanced partitioning approach
- Located in src/backend/access/spgist/spgkdtreeproc.c:108-159