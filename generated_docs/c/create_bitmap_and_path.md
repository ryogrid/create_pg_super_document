# create_bitmap_and_path

## Location
[src/backend/optimizer/util/pathnode.c:1075-1126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1075-L1126)

## Overview
Creates a path node representing a BitmapAnd operation, which combines multiple bitmap index scans using logical AND to find tuples that satisfy all of the specified index conditions.

## Definition
```c
BitmapAndPath *create_bitmap_and_path(PlannerInfo *root,
                                      RelOptInfo *rel,
                                      List *bitmapquals)
```

## Detailed Description
This function constructs a BitmapAndPath node that represents the intersection (logical AND) of multiple bitmap index operations. Each bitmap index scan produces a bitmap indicating which heap pages contain tuples matching a specific condition. The BitmapAnd operation combines these bitmaps by performing a bitwise AND operation, resulting in a bitmap that identifies pages containing tuples that satisfy all the conditions.

The function automatically computes the required outer relations by taking the union of what all child paths depend on. This is essential for handling parameterized paths in join scenarios. The resulting path inherits the parallel safety characteristics from the relation but is not itself parallel-aware since bitmap operations are currently not parallelized at this level.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and optimization settings
- `rel`: RelOptInfo for the relation being scanned, providing metadata and statistics
- `bitmapquals`: List of child bitmap paths (IndexPath, BitmapAndPath, or BitmapOrPath nodes) to be combined with AND logic

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new BitmapAndPath node)
  - [bms_add_members](../b/bms_add_members.md) (combines bitmap sets for required outer relations)
  - PATH_REQ_OUTER (macro to get required outer relations from a path)
  - get_baserel_parampathinfo (retrieves parameterization information)
  - [cost_bitmap_and_node](cost_bitmap_and_node.md) (calculates costs and selectivity)
- Called from (representative examples):
  - [choose_bitmap_and](choose_bitmap_and.md) (when selecting optimal AND combinations)
  - [bitmap_and_cost_est](../b/bitmap_and_cost_est.md) (for cost estimation during bitmap path selection)

## Notes and Other Information
- The resulting path is always unordered (pathkeys = NIL) since bitmap operations don't preserve any ordering
- Required outer relations are computed as the union of all child path dependencies
- Currently not parallel-aware but inherits parallel safety from the relation
- The cost_bitmap_and_node function sets both regular cost fields and the bitmapselectivity field
- Used when multiple indexes can be efficiently combined to reduce the number of heap pages that need to be scanned
- The selectivity of the AND operation is typically the product of individual selectivities (assuming independence)