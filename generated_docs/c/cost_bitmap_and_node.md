# cost_bitmap_and_node

## Location
[src/backend/optimizer/path/costsize.c:1157-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1157-L1200)

## Overview
Estimates the cost of a BitmapAnd node by calculating the combined costs of its constituent bitmap operations and the bitmap intersection overhead.

## Definition
```c
void cost_bitmap_and_node(BitmapAndPath *path, PlannerInfo *root)
```

## Detailed Description
This function computes the cost and selectivity for a BitmapAnd node, which represents the intersection of multiple bitmap index scans. The costing model includes:

1. **Selectivity Calculation**: Multiplies the selectivities of all child nodes, assuming independence (which may often be incorrect but represents the best available estimate)
2. **Base Costs**: Sums up the costs of all constituent bitmap operations by calling cost_bitmap_tree_node for each child
3. **Intersection Overhead**: Adds 100 * cpu_operator_cost for each tbm_intersect operation needed (one less than the number of inputs)

The function treats the BitmapAndPath as a pseudo-Path object with cost properties but doesn't populate the rows field since this represents an intermediate bitmap operation rather than a complete scan path. The estimated costs cover only index scanning and bitmap creation, not the eventual heap access phase.

## Parameters / Member Variables
- `path`: The BitmapAndPath node whose cost is being calculated
- `root`: The PlannerInfo containing global planning context (currently unused but maintained for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - [cost_bitmap_tree_node](cost_bitmap_tree_node.md) (to get cost/selectivity from child nodes)
  - [list_head](../l/list_head.md) (to check for first element in list)
  - cpu_operator_cost (global cost parameter)

- Called from:
  - [create_bitmap_and_path](create_bitmap_and_path.md) (in pathnode.c:1117)

## Notes and Other Information
- Assumes independence of child selectivities, which is often inaccurate but necessary given available information
- Uses a simplistic cost model for bitmap intersection (100 * cpu_operator_cost per operation)
- Does not set the rows field as this is an intermediate bitmap operation
- The intersection cost may be underestimated and could benefit from more sophisticated modeling
- Located in src/backend/optimizer/path/costsize.c:1157-1200

## Simplified Source

```c
void cost_bitmap_and_node(BitmapAndPath *path, PlannerInfo *root)
{
    Cost totalCost = 0.0;
    Selectivity selec = 1.0;
    ListCell *l;

    // Process each child bitmap path
    foreach(l, path->bitmapquals)
    {
        Path *subpath = (Path *) lfirst(l);
        Cost subCost;
        Selectivity subselec;

        // Get cost and selectivity of child path
        cost_bitmap_tree_node(subpath, &subCost, &subselec);

        // Combine selectivities (assume independence)
        selec *= subselec;

        // Add child cost
        totalCost += subCost;

        // Add intersection cost (100 * cpu_operator_cost per intersection)
        if (l != list_head(path->bitmapquals))
            totalCost += 100.0 * cpu_operator_cost;
    }

    // Set final cost and selectivity
    path->bitmapselectivity = selec;
    path->path.rows = 0;            // Not used for bitmap operations
    path->path.startup_cost = totalCost;
    path->path.total_cost = totalCost;
}
```