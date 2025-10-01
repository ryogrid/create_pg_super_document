# create_bitmap_or_path

## Location
[src/backend/optimizer/util/pathnode.c:1127-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1127-L1178)

## Overview
Creates a path node representing a BitmapOr operation, which combines multiple bitmap index scans using logical OR to find tuples that satisfy any of the specified index conditions.

## Definition
```c
BitmapOrPath *create_bitmap_or_path(PlannerInfo *root,
                                    RelOptInfo *rel,
                                    List *bitmapquals)
```

## Detailed Description
This function constructs a BitmapOrPath node that represents the union (logical OR) of multiple bitmap index operations. Each bitmap index scan produces a bitmap indicating which heap pages contain tuples matching a specific condition. The BitmapOr operation combines these bitmaps by performing a bitwise OR operation, resulting in a bitmap that identifies pages containing tuples that satisfy at least one of the conditions.

The function automatically computes the required outer relations by taking the union of what all child paths depend on, ensuring proper handling of parameterized paths in join scenarios. Like its AND counterpart, the resulting path inherits parallel safety characteristics from the relation but is not itself parallel-aware since bitmap operations are currently not parallelized at this level.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and optimization settings
- `rel`: RelOptInfo for the relation being scanned, providing metadata and statistics
- `bitmapquals`: List of child bitmap paths (IndexPath, BitmapAndPath, or BitmapOrPath nodes) to be combined with OR logic

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new BitmapOrPath node)
  - [bms_add_members](../b/bms_add_members.md) (combines bitmap sets for required outer relations)
  - PATH_REQ_OUTER (macro to get required outer relations from a path)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md) (retrieves parameterization information)
  - [cost_bitmap_or_node](cost_bitmap_or_node.md) (calculates costs and selectivity)
- Called from (representative examples):
  - [generate_bitmap_or_paths](../g/generate_bitmap_or_paths.md) (when generating OR combinations of bitmap paths)

## Notes and Other Information
- The resulting path is always unordered (pathkeys = NIL) since bitmap operations don't preserve any ordering
- Required outer relations are computed as the union of all child path dependencies
- Currently not parallel-aware but inherits parallel safety from the relation
- The cost_bitmap_or_node function sets both regular cost fields and the bitmapselectivity field
- Used when multiple indexes with different conditions can be combined to capture more matching tuples
- The selectivity of the OR operation follows the inclusion-exclusion principle: sel(A OR B) = sel(A) + sel(B) - sel(A AND B)
- Particularly useful for queries with multiple OR conditions that can each be satisfied by different indexes

## Simplified Source

```c
BitmapOrPath *
create_bitmap_or_path(PlannerInfo *root,
                      RelOptInfo *rel,
                      List *bitmapquals)
{
    BitmapOrPath *pathnode = makeNode(BitmapOrPath);
    Relids required_outer = NULL;
    ListCell *lc;

    pathnode->path.pathtype = T_BitmapOr;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = rel->reltarget;

    // Compute required outer rels as union of child path dependencies
    foreach(lc, bitmapquals)
    {
        Path *bitmapqual = (Path *) lfirst(lc);
        required_outer = bms_add_members(required_outer, PATH_REQ_OUTER(bitmapqual));
    }
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);

    // Set parallel characteristics based on relation
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;

    pathnode->path.pathkeys = NIL; // always unordered
    pathnode->bitmapquals = bitmapquals;

    // Calculate costs and selectivity
    cost_bitmap_or_node(pathnode, root);

    return pathnode;
}
```